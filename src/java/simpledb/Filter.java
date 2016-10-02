package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private final Predicate pred;
    private DbIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        pred = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return pred;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        child.open();
    }

    @Override
    public void close() {
    	super.close();
    	child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (child.hasNext()) {
        	Tuple t = child.next();
        	if (pred.filter(t)) {
        		return t;
        	}
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (children.length != 1) {
    		throw new IllegalArgumentException("Expected a single new child.");
    	}
    	child = children[0];
    }
}
