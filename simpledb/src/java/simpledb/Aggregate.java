package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    private static final long serialVersionUID = 1L;
    
    private final int aggField, grpField;
    private DbIterator child;
    private final Aggregator.Op op;
    private final Aggregator agg;
    private DbIterator aggIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		aggField = afield;
		grpField = gfield;
		this.child = child;
		op = aop;
		
		Type aggFieldType = child.getTupleDesc().getFieldType(aggField);
		Type grpFieldType = grpField == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(grpField);
		switch(aggFieldType) {
		case INT_TYPE:
			agg = new IntegerAggregator(grpField, grpFieldType, aggField, op);
			break;
		case DOUBLE_TYPE:
			throw new RuntimeException("Aggregating doubles is not supported.");
		case STRING_TYPE:
			agg = new StringAggregator(grpField, grpFieldType, aggField, op);
			break;
		default:
			throw new RuntimeException("Unexpected field type.");
		}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		return grpField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if (grpField == Aggregator.NO_GROUPING) {
    		return null;
    	}
    	return child.getTupleDesc().getFieldName(grpField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(aggField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	super.open();
    	
    	try {
    		child.open();
        	while (child.hasNext()) {
        		agg.mergeTupleIntoGroup(child.next());
        	}
    	} finally {
    		child.close();
    	}
    	
    	aggIterator = agg.iterator();
    	aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (aggIterator.hasNext()) {
    		return aggIterator.next();
    	}
    	return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
    	if (aggIterator == null) {
    		throw new IllegalStateException();
    	}
    	aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
    	return agg.getTupleDesc();
    }

    @Override
    public void close() {
    	aggIterator.close();
    	aggIterator = null;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (children.length != 1) {
    		throw new IllegalArgumentException("Expected one new child.");
    	}
    	child = children[0];
    }
    
}
