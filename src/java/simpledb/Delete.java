package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private final TransactionId transId;
    private DbIterator child;
    
    private static final TupleDesc SCHEMA = new TupleDesc(new Type[] { Type.INT_TYPE });

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	if (child == null) {
    		throw new IllegalArgumentException();
    	}
        transId = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return SCHEMA;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	throw new DbException("Rewind is not supported for DELETE.");
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// If this is the second call to fetchNext, return null.
    	if (child == null) {
    		return null;
    	}
    	
    	child.open();
    	BufferPool bp = Database.getBufferPool();
    	int deleted = 0;
    	while (child.hasNext()) {
    		Tuple t = child.next();
    		try {
				bp.deleteTuple(transId, t);
			} catch (IOException e) {
				throw new DbException("IO operation failed during DELETE.");
			}
    		deleted++;
    	}
    	child.close();
    	child = null;
    	
    	return new Tuple(SCHEMA, new Field[] { new IntField(deleted) });
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
