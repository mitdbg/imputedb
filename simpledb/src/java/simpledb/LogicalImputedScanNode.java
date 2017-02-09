package simpledb;

import java.util.NoSuchElementException;
import java.util.Set;

public class LogicalImputedScanNode extends ImputedPlan {
	private final DbIterator physicalPlan;
	private final int tableId;
	private final String tableAlias;
	
	public LogicalImputedScanNode(TransactionId tid, LogicalScanNode scan) throws ParsingException {
		tableId = scan.t;
		tableAlias = scan.alias;
		
		/* Create a physical plan for the scan. */
		try {
			physicalPlan = new SeqScan(tid, scan.t, scan.alias);
		} catch (NoSuchElementException e) {
			throw new ParsingException("Unknown table " + scan.alias);
		}
	}

	@Override
	public Set<QualifiedName> getDirtySet() {
		return DirtySet.ofBaseTable(tableId, tableAlias);
	}

	@Override
	public DbIterator getPlan() {
		return physicalPlan;
	}

	@Override
	public double cost(double lossWeight) {
		return getTableStats().estimateScanCost();
	}

	@Override
	public double cardinality() {
		return getTableStats().totalTuples();
	}

	@Override
	public TableStats getTableStats() {
		return TableStats.getTableStats(Database.getCatalog().getTableName(tableId));
	}
}
