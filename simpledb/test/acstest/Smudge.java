package acstest;

import java.util.*;
import simpledb.*;

public class Smudge extends Operator {
	private static final long serialVersionUID = -1165468253716629695L;
	private final DbIterator child;
	private final double dirtyPerc;
	private final Random rand;
	
	private static final Field MISSING = new IntField();
	
	public Smudge(DbIterator child, double dirtyPerc) {
		super();
		this.child = child;
		this.dirtyPerc = dirtyPerc;
		rand = new Random(0);
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
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
		while (child.hasNext()) {
        	Tuple t = child.next();
        	for (int f = 0; f < child.getTupleDesc().numFields(); f++) {        		
        		if (rand.nextDouble() < dirtyPerc) {
        			t.setField(f, MISSING);
        		}
        	}
        	return t;
        }
        return null;
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
