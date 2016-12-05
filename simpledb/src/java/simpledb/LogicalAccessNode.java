package simpledb;

import java.util.*;

import static simpledb.ImputationType.DROP;
import static simpledb.ImputationType.MAXIMAL;
import static simpledb.ImputationType.MINIMAL;

public class LogicalAccessNode extends ImputedPlan {
	private final DbIterator physicalPlan;
	private final HashSet<QuantifiedName> dirtySet;
	private final double loss;
	private final double time;
	private TableStats tableStats;
	
	// TODO: hackish way of getting table alias name for join optimization (better way?)
	public final String alias;
	public final int tableId;

	private static final double LOSS_FACTOR = 1.01;

	public LogicalAccessNode(TransactionId tid, LogicalScanNode scan, ImputationType imp, LogicalFilterNode filter)
	throws ParsingException, BadImputation {
		this(tid, scan, imp, filterToSingletonSet(filter));
	}

	private static Set<LogicalFilterNode> filterToSingletonSet(LogicalFilterNode filter) {
		return filter == null ? null : Collections.singleton(filter);
	}

	public LogicalAccessNode(TransactionId tid, LogicalScanNode scan, ImputationType imp, Set<LogicalFilterNode> filters)
			throws ParsingException, BadImputation {
		DbIterator pp;

		tableId = scan.t;
		alias = scan.alias;
		
		/* Create a physical plan for the scan. */
		try {
			pp = new SeqScan(tid, scan.t, scan.alias);
		} catch (NoSuchElementException e) {
			throw new ParsingException("Unknown table " + scan.alias);
		}

		/* Get the dirty set of the base table. */
		HashSet<QuantifiedName> tableDirtySet = DirtySet.ofBaseTable(scan.t, scan.alias);

		/* Get the required set of the predicate. */
		HashSet<QuantifiedName> required = new HashSet<QuantifiedName>();
		if (filters != null) {
			for(LogicalFilterNode filter : filters) {
				required.add(new QuantifiedName(filter.tableAlias, filter.fieldPureName));
			}
		}

		/* Compute the minimal impute/drop set. */
		required.retainAll(tableDirtySet);
		HashSet<String> requiredAttrs = DirtySet.toAttrs(required);
		
		TupleDesc schema = pp.getTupleDesc();
		ArrayList<Integer> requiredIdx = new ArrayList<Integer>();
		for (String attr : requiredAttrs) {
			requiredIdx.add(schema.fieldNameToIndex(attr));
		}

		/* Add the imputation operator, if any. */
		// use scan's tablestats for initial calculations
		TableStats subplanTableStats = TableStats.getTableStats(Database.getCatalog().getTableName(scan.t));
		double totalData = subplanTableStats.totalTuples() * pp.getTupleDesc().numFields();

		// adjusted tablestats to reflect imputation and filtering
		TableStats adjustedTableStats;

		switch (imp) {
		case DROP:
			if (required.isEmpty()) {
				throw new BadImputation();
			}
			pp = new Drop(requiredAttrs, pp);
			tableDirtySet.removeAll(required);
			dirtySet = tableDirtySet;
			loss = subplanTableStats.estimateTotalNull(requiredIdx);
			time = subplanTableStats.estimateScanCost();
			adjustedTableStats = subplanTableStats.adjustForImpute(DROP, requiredIdx);
			break;
		case MAXIMAL:
			loss = subplanTableStats.estimateTotalNull() * Math.pow(LOSS_FACTOR, -totalData);
			time = subplanTableStats.estimateScanCost() + subplanTableStats.estimateImputeCost();
			pp = new ImputeRegressionTree(DirtySet.toAttrs(tableDirtySet), pp);
			dirtySet = new HashSet<QuantifiedName>();
			adjustedTableStats = subplanTableStats.adjustForImpute(MAXIMAL, requiredIdx);
			break;
		case MINIMAL:
			if (required.isEmpty()) {
				throw new BadImputation();
			}
			loss = subplanTableStats.estimateTotalNull(requiredIdx) * Math.pow(LOSS_FACTOR, -totalData);
			time = subplanTableStats.estimateScanCost() + subplanTableStats.estimateImputeCost();
			pp = new ImputeRegressionTree(requiredAttrs, pp);
			tableDirtySet.removeAll(required);
			dirtySet = tableDirtySet;
			adjustedTableStats = subplanTableStats.adjustForImpute(MINIMAL, requiredIdx);
			break;
		case NONE:
			if (!required.isEmpty()) {
				throw new BadImputation();
			}
			loss = 0.0;
			dirtySet = tableDirtySet;
			time = subplanTableStats.estimateScanCost();
			// if no action, just keep same histograms
			adjustedTableStats = subplanTableStats;
			break;
		default:
			throw new RuntimeException("Unexpected ImputationType.");
		}

		/* If no filters, then we're done constructing the plan. */
		if (filters == null) {
			physicalPlan = pp;
			tableStats = adjustedTableStats;
			return;
		}

		DbIterator subplan = pp;

		/* Otherwise, construct a Filter operator for each filter, stacking on top of previous */
		for(LogicalFilterNode filter : filters) {
		/* First, get the type of the field in the predicate. */
			Type ftyp;
			TupleDesc td = subplan.getTupleDesc();
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
			double selectivity = adjustedTableStats.estimateSelectivity(p);
			adjustedTableStats = adjustedTableStats.adjustForSelectivity(selectivity);

			subplan = new Filter(p, subplan);

		}
		// assign final subplan with all the filters
		physicalPlan = subplan;
		tableStats = adjustedTableStats;
	}

	public TableStats getTableStats() {
		return tableStats;
	}

	public DbIterator getPlan() {
		return physicalPlan;
	}

	public HashSet<QuantifiedName> getDirtySet() {
		return dirtySet;
	}

	public double cost(double lossWeight) {
		return lossWeight * loss + (1 - lossWeight) * time;
	}

	public double cardinality() {
		return tableStats.totalTuples();
	}

}