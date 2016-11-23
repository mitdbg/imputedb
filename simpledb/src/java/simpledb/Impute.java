package simpledb;

import java.util.NoSuchElementException;

public abstract class Impute extends Operator {

    private static final long serialVersionUID = 1L;

    protected DbIterator child;
    protected TupleDesc td;

    public Impute(DbIterator child){
		this.child = child;
		this.td = child.getTupleDesc();
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

	@Override
	public void open() throws DbException, NoSuchElementException,
			TransactionAbortedException {
		super.open();
		child.open();
	}

	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

}
