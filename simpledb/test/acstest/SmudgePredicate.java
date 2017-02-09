package acstest;

import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.Predicate;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.Field;
import simpledb.IntField;
import simpledb.Operator;
import simpledb.QualifiedName;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

class SmudgePredicate extends Operator {
	private static final long serialVersionUID = -5585085012061351883L;
	private final DbIterator child;
	private final int dirtyIndex;
	private final double dirtyPerc;
	private final Predicate<Tuple> shouldDirty;
	
	private final Random rand = new Random(0);
	
	private static final Field missing = new IntField();
	
	public SmudgePredicate(DbIterator child, int dirtyIndex, double dirtyPerc, Predicate<Tuple> shouldDirty) {
		this.child = child;
		this.dirtyIndex = dirtyIndex;
		this.dirtyPerc = dirtyPerc;
		this.shouldDirty = shouldDirty;
	}
	
	public SmudgePredicate(DbIterator child, QualifiedName dirtyName, double dirtyPerc, Predicate<Tuple> shouldDirty) {
		this(child, child.getTupleDesc().fieldNameToIndex(dirtyName), dirtyPerc, shouldDirty);
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
        	if (rand.nextDouble() < dirtyPerc) {
        		if (shouldDirty.test(t)) {
        			Tuple tt = new Tuple(t);
        			tt.setField(dirtyIndex, missing);
        			return tt;
        		}
        	} else {
        		return t;
        	}
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