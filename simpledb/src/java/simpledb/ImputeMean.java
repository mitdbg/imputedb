package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;


public class ImputeMean extends Impute {

    private static final long serialVersionUID = 1L;

	private TableStats tableStats = null;

    /**
     * Impute missing data for a column by using the mean of the
     * non-missing elements of the same column. Thus, this method requires that
     * we store all of the child tuples in a buffer until the child is
     * exhausted. Alternately, we could try to make use of the histograms for the table.
     * @param child
     */
    public ImputeMean(Collection<String> dropFields, DbIterator child){
        super(dropFields, child);

        // get tablestats?
        int thisTableId = -1;
        Catalog catalog = Database.getCatalog();
        Iterator<Integer> it = catalog.tableIdIterator();
        while (it.hasNext()){
		int tableId = it.next();
		if (catalog.getTupleDesc(tableId).equals(td)){
			thisTableId = tableId;
			break;
		}
        }
        if (thisTableId != -1){
		String tablename = catalog.getTableName(thisTableId);
		this.tableStats = TableStats.getTableStats(tablename);
        }
    }

    public ImputeMean(DbIterator child) {
        this(null, child);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (this.tableStats == null){
            throw new DbException("TableStats not loaded.");
        }

        if (child.hasNext()){
            Tuple t = child.next();
            if (t.hasMissingFields()){
                return impute(t);
            } else {
                return t;
            }
        } else {
            return null;
        }
    }

    private Tuple impute(Tuple t) throws DbException {
        Tuple tc = new Tuple(t);

        for (int j : dropFieldsIndices){
            // Don't impute if not missing.
            if (!tc.getField(j).isMissing())
                continue;

            // set to mean
            tc.setField(j, new IntField((int) tableStats.estimateMean(j)));
        }

        return tc;
    }

    /*
     * Time is zero because we can compute in streaming fashion.
     * @see simpledb.Impute#getEstimatedTime(simpledb.ImputedPlan)
     */
	@Override
	public double getEstimatedTime(ImputedPlan subplan) {
		return 0;
	}

    /*
     * Can conceive this as O(n m_i)
     * @see simpledb.Impute#getEstimatedCost(ImputedPlan)
     */
	@Override
	public double getEstimatedPenalty(ImputedPlan subplan) {
		double numTuples = subplan.cardinality();
		Set<QualifiedName> dirtySet = subplan.getDirtySet();

		double sum = 0.0;
		for (QualifiedName qn : dirtySet){
			int qni = subplan.getPlan().getTupleDesc().fieldNameToIndex(qn);
			double var = subplan.getTableStats().estimateVariance(qni);
			sum += var;
		}
		return sum / numTuples;
	}

	public void setTableStats(ImputedPlan subplan){
		this.tableStats = subplan.getTableStats();
	}

}
