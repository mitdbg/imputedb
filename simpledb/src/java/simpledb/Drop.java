package simpledb;

import java.util.Collection;
import java.util.NoSuchElementException;

public class Drop extends Operator {
	private final Collection<String> dropFields;
	private final DbIterator child;

	public Drop(Collection<String> dropFields, DbIterator child) {
		this.dropFields = dropFields;
		this.child = child;
	}

	@Override
	public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
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

	@Override
	protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		TupleDesc td = child.getTupleDesc();
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
	public DbIterator[] getChildren() {
		return new DbIterator[] { child };
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
