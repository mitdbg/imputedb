package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.NoSuchElementException;

import simpledb.FilterOptimizer.ImputationType;

public class LogicalAccessNode {
	private final DbIterator physicalPlan;
	private final HashSet<QuantifiedName> dirtySet;
	private final double loss;
	private final double time;

	private static final double LOSS_FACTOR = 1.01;

	public LogicalAccessNode(TransactionId tid, LogicalScanNode scan, ImputationType imp, LogicalFilterNode filter)
			throws ParsingException {
		DbIterator pp;

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
		if (filter != null) {
			required.add(new QuantifiedName(filter.tableAlias, filter.fieldPureName));
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
		TableStats tableStats = TableStats.getTableStats(Database.getCatalog().getTableName(scan.t));
		double totalData = tableStats.totalTuples() * pp.getTupleDesc().numFields();

		switch (imp) {
		case DROP:
			pp = new Drop(pp, requiredAttrs);
			tableDirtySet.removeAll(required);
			dirtySet = tableDirtySet;
			loss = tableStats.estimateTotalNull(requiredIdx);
			time = tableStats.estimateScanCost();
			break;
		case MAXIMAL:
			loss = tableStats.estimateTotalNull() * Math.pow(LOSS_FACTOR, -totalData);
			time = tableStats.estimateScanCost() + tableStats.estimateImputeCost();
			pp = new ImputeRandom(pp);
			dirtySet = new HashSet<QuantifiedName>();
			break;
		case MINIMAL:
			loss = tableStats.estimateTotalNull(requiredIdx) * Math.pow(LOSS_FACTOR, -totalData);
			time = tableStats.estimateScanCost() + tableStats.estimateImputeCost();
			pp = new ImputeRandom(pp, requiredAttrs);
			tableDirtySet.removeAll(required);
			dirtySet = tableDirtySet;
			break;
		case NONE:
			if (!required.isEmpty()) {
				throw new IllegalArgumentException("Must impute all dirty attributes used by the predicate.");
			}
			loss = 0.0;
			dirtySet = tableDirtySet;
			time = tableStats.estimateScanCost();
			break;
		default:
			throw new RuntimeException("Unexpected ImputationType.");
		}

		/* If no filter, then we're done constructing the plan. */
		if (filter == null) {
			physicalPlan = pp;
			return;
		}

		/* Otherwise, construct a Filter operator. */

		/* First, get the type of the field in the predicate. */
		Type ftyp;
		TupleDesc td = pp.getTupleDesc();
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

		physicalPlan = new Filter(p, pp);
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
}