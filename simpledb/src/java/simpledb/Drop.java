package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

public class Drop extends Operator {
	private static final long serialVersionUID = 7072651197398454951L;
	
	private final Collection<Integer> dropFields;
	private final DbIterator child;
	
	public Drop(DbIterator child, Collection<String> dropFields) {
		this.dropFields = new ArrayList<Integer>(dropFields.size());
		TupleDesc td = child.getTupleDesc();
		for (String field : dropFields) {
			this.dropFields.add(td.fieldNameToIndex(field));
		}
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
        	for (int fieldIdx : dropFields) {
        		if (t.getField(fieldIdx).isMissing()) {
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
	
	public Collection<String> getDropFields() {
		ArrayList<String> ret = new ArrayList<>(dropFields.size());
		for (int f : dropFields) {
			ret.add(getTupleDesc().getFieldName(f));
		}
		return ret;
	}
}
