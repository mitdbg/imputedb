package simpledb;

import java.util.Collection;

public class Drop extends Impute {

    private static final long serialVersionUID = 1L;

    /**
     * Deal with missing data on a set of column by simply dropping the rows
     * with missing data.
     * @param dropFields set of columns to consider
     * @param child
     */
    public Drop(Collection<String> dropFields, DbIterator child) {
        super(dropFields, child);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        while (child.hasNext()) {
            Tuple t = child.next();
            boolean drop = false;
            for (String field : dropFields) {
                if (t.getField(td.fieldNameToIndex(field)).isMissing()) {
                    drop = true;
                    break;
                }
            }
            if (!drop) {
                return t;
            }
        }
        return null;
    }

	@Override
	public double getEstimatedTime(ImputedPlan subplan) {
		return 0.01 * subplan.cardinality();
	}

	@Override
    public double getEstimatedPenalty(ImputedPlan subplan) {
        return 1.0;
    }
}
