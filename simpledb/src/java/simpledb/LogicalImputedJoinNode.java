package simpledb;

import java.util.HashSet;
import java.util.Map;

import simpledb.Utility.AvgAgg;

/**
 * Performs a join between two plans that have imputations along their respective trees.
 * */
public class LogicalImputedJoinNode extends ImputedPlan {
    /** The first table to join (may be null). It's the alias of the table (if no alias, the true table name) */
    public final String t1Alias;

    /** The second table to join (may be null).  It's the alias of the table, (if no alias, the true table name).*/
    public final String t2Alias;

    /** The name of the field in t1 to join with. It's the pure name of a field, rather that alias.field. */
    public final String f1PureName;

    public final QualifiedName f1QuantifiedName;

    /** The name of the field in t2 to join with. It's the pure name of a field.*/
    public final String f2PureName;

    public final QualifiedName f2QuantifiedName;

    private final TransactionId tid;

    // Plans with imputation
    private final ImputedPlan table1;
    private final ImputedPlan table2;

    // dirty set
    private final HashSet<QualifiedName> dirtySet;

    // physical plan
    private final DbIterator physicalPlan;

    // need to able to lookup tableIds (usually this is in the LogicalPlan)
    private final Map<String, Integer> tableMap;

    /** The join predicate */
    public Predicate.Op p;

    // table stats
    private TableStats tableStats;

    public LogicalImputedJoinNode(
        TransactionId tid,
        String table1Name,
        String table2Name,
        ImputedPlan table1,
        ImputedPlan table2,
        String joinField1,
        String joinField2,
        Predicate.Op pred,
        Map<String, Integer> tableMap) {
        this.tid = tid;
        t1Alias = table1Name;
        t2Alias = table2Name;
        this.table1 = table1;
        this.table2 = table2;
        String[] tmps = joinField1.split("[.]");
        if (tmps.length>1)
            f1PureName = tmps[tmps.length-1];
        else
            f1PureName=joinField1;
        tmps = joinField2.split("[.]");
        if (tmps.length>1)
            f2PureName = tmps[tmps.length-1];
        else
            f2PureName = joinField2;
        p = pred;
        this.f1QuantifiedName = new QualifiedName(t1Alias, f1PureName);
        this.f2QuantifiedName = new QualifiedName(t2Alias, f2PureName);

        // create physical plan
        int ixfield1 = table1.getPlan().getTupleDesc().fieldNameToIndex(f1QuantifiedName.toString());
        int ixfield2 = table2.getPlan().getTupleDesc().fieldNameToIndex(f2QuantifiedName.toString());
        JoinPredicate joinPred = new JoinPredicate(ixfield1, p, ixfield2);
        physicalPlan = new Join(joinPred, table1.getPlan(), table2.getPlan());

        // add dirty set info
        dirtySet = new HashSet<QualifiedName>();
        // add dirty set of table 1
        dirtySet.addAll(table1.getDirtySet());
        // add dirty set of table 2
        dirtySet.addAll(table2.getDirtySet());

        if (dirtySet.contains(f1QuantifiedName) || dirtySet.contains(f2QuantifiedName)) {
            throw new IllegalArgumentException("Must impute all dirty attributes used by the join predicate");
        }
        
        this.tableMap = tableMap;

        // estimate a new combined set of table statistics based on children tablestats and join type
        tableStats = estimateJoinedTableStats();
    }

    public TableStats getTableStats() {
        return tableStats;
    }
    
    /** Return a new LogicalJoinNode with the inner and outer (t1.f1
     * and t2.f2) tables swapped. */
    public LogicalImputedJoinNode swapInnerOuter() {
        Predicate.Op newp;
        if (p == Predicate.Op.GREATER_THAN)
            newp = Predicate.Op.LESS_THAN;
        else if (p == Predicate.Op.GREATER_THAN_OR_EQ)
            newp = Predicate.Op.LESS_THAN_OR_EQ;
        else if (p == Predicate.Op.LESS_THAN)
            newp = Predicate.Op.GREATER_THAN;
        else if (p == Predicate.Op.LESS_THAN_OR_EQ)
            newp = Predicate.Op.GREATER_THAN_OR_EQ;
        else 
            newp = p;
        LogicalImputedJoinNode j2 = new LogicalImputedJoinNode(
            tid, t2Alias,t1Alias, table2, table1, f2PureName,f1PureName, newp, tableMap
        );
        return j2;
    }

    public double cardinality() {
        return tableStats.totalTuples();
    }

    private TableStats estimateJoinedTableStats()  {
        // estimated cardinality of joined table, follows approach in selinger
        double cardEstimate;
        boolean child1HasJoinPK = isPkey(t1Alias, f1PureName);
        boolean child2HasJoinPK = isPkey(t2Alias, f2PureName);
        double child1card = table1.cardinality();
        double child2card = table2.cardinality();
        
        if (p.equals(Predicate.Op.EQUALS)) {
        	if (child1HasJoinPK && child2HasJoinPK) {
            	cardEstimate = Math.min(child1card, child2card);
            } else if (child1HasJoinPK) {
            	cardEstimate = child1card;
            } else if (child2HasJoinPK) {
            	cardEstimate = child2card;
            } else {
            	cardEstimate = Math.max(child1card, child2card);
            }
        } else {
            cardEstimate = table1.cardinality() * table2.cardinality() * 0.3;
        }

        // adjust each set of histograms to the new cardinality (i.e. maintains underlying distribution)
        TableStats stats1 = table1.getTableStats().adjustToTotal(cardEstimate);
        TableStats stats2 = table2.getTableStats().adjustToTotal(cardEstimate);
        // merge the histograms
        return stats1.merge(stats2);
    }
    
    @Override
    protected AvgAgg penalty() {
    	return table1.penalty().add(table2.penalty());
    }
    
    @Override
    protected double time() {
    	switch(p) {
    	// Hash join
		case EQUALS:
		case LIKE:
			return table1.time() + table2.time() + (table1.cardinality() + table2.cardinality()) * 0.01;
			
		// Nested loops join
		case GREATER_THAN:
		case GREATER_THAN_OR_EQ:
		case LESS_THAN:
		case LESS_THAN_OR_EQ:
		case NOT_EQUALS:
			return table1.time() + table1.cardinality() * table2.time() + table1.cardinality() * table2.cardinality() * 0.01;
		default:
			throw new RuntimeException("Unexpected predicate.");
    	}
    }


    private boolean isPkey(String tableAlias, String field) {
        // look up actual table name, which alias points to
        int tid1 = tableMap.get(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);
        return pkey1.equals(field);
    }

    public DbIterator getPlan() {
        return physicalPlan;
    }

    public HashSet<QualifiedName> getDirtySet() {
        return dirtySet;
    }

    @Override public boolean equals(Object o) {
        LogicalImputedJoinNode j2 =(LogicalImputedJoinNode)o;
        return (j2.t1Alias.equals(t1Alias)  || j2.t1Alias.equals(t2Alias)) && (j2.t2Alias.equals(t1Alias)  || j2.t2Alias.equals(t2Alias));
    }

    @Override public String toString() {
        return t1Alias + ":" + t2Alias ;//+ ";" + f1 + " " + p + " " + f2;
    }
    
    @Override public int hashCode() {
        return t1Alias.hashCode() + t2Alias.hashCode() + f1PureName.hashCode() + f2PureName.hashCode();
    }
}

