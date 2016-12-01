package acstest;

import java.util.NoSuchElementException;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.Operator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

class Limit extends Operator {
	private static final long serialVersionUID = 65760215442849003L;
	
	private final DbIterator child;
	private final int limit;
	private int count = 0;
	
	public Limit(DbIterator child, int limit) {
		this.child = child;
		this.limit = limit;
	}
	
	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
		count = 0;
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
	protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		if (count >= limit || !child.hasNext()) {
			return null;
		}
		count++;
		return child.next();
	}

	@Override
	public DbIterator[] getChildren() {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public void setChildren(DbIterator[] children) {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public TupleDesc getTupleDesc() {
		return child.getTupleDesc();
	}
}