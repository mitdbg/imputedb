package simpledb;

import java.util.*;

import simpledb.Utility.AvgAgg;

public class LogicalImputedFilterNode extends ImputedPlan {
	private final DbIterator physicalPlan;
	private final TableStats tableStats;
	private final ImputedPlan subplan;

	public LogicalImputedFilterNode(TransactionId tid, ImputedPlan subplan, Set<LogicalFilterNode> filters) throws ParsingException {
		this.subplan = subplan;
		
		DbIterator physicalPlanTmp = subplan.getPlan();
		TableStats tableStatsTmp = subplan.getTableStats();

		/* Otherwise, construct a Filter operator for each filter, stacking on top of previous */
		for(LogicalFilterNode filter : filters) {
		/* First, get the type of the field in the predicate. */
			Type ftyp;
			TupleDesc td = subplan.getPlan().getTupleDesc();
			try {
				ftyp = td.getFieldType(td.fieldNameToIndex(filter.fieldQuantifiedName));
			} catch (NoSuchElementException e) {
				throw new ParsingException("Unknown field in filter expression " + filter.fieldQuantifiedName);
			}

			// treat comparisons to null as comparisons to missing
			boolean isNull = filter.c.equalsIgnoreCase("NULL");

			// create an appropriate constant field value to compare against
			Field f;
			switch (ftyp) {
				case DOUBLE_TYPE:
					f = isNull ? new DoubleField() : new DoubleField(Double.valueOf(filter.c));
					break;
				case INT_TYPE:
					f = isNull ? new IntField() : new IntField(Integer.valueOf(filter.c));
					break;
				case STRING_TYPE:
					f = isNull ? new StringField(Type.STRING_LEN) : new StringField(filter.c, Type.STRING_LEN);
					break;
				default:
					throw new RuntimeException("Unexpected type.");
			}

			Predicate p = null;
			try {
				p = new Predicate(td.fieldNameToIndex(filter.fieldQuantifiedName), filter.p, f);
			} catch (NoSuchElementException e) {
				throw new ParsingException("Unknown field " + filter.fieldQuantifiedName);
			}

			// estimate selectivity for predicate, and adjust table stats based on that
			double selectivity = tableStatsTmp.estimateSelectivity(p);
			tableStatsTmp = tableStatsTmp.adjustForSelectivity(selectivity);

			physicalPlanTmp = new Filter(p, physicalPlanTmp);
		}
		
		// assign final subplan with all the filters
		physicalPlan = physicalPlanTmp;
		tableStats = tableStatsTmp;
	}

	public TableStats getTableStats() {
		return tableStats;
	}

	public DbIterator getPlan() {
		return physicalPlan;
	}

	public Set<QualifiedName> getDirtySet() {
		return subplan.getDirtySet();
	}
	
	@Override
	protected AvgAgg penalty() {
		return subplan.penalty();
	}
	
	@Override
	protected double time() {
		return subplan.time() + subplan.cardinality() * 0.01;
	}

	public double cardinality() {
		return tableStats.totalTuples();
	}
}