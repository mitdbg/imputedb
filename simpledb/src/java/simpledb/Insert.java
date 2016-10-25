package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private final TransactionId transId;
    private DbIterator child;
    private final int tableId;
    
    private static final TupleDesc SCHEMA = new TupleDesc(new Type[] { Type.INT_TYPE });

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
    	if (t == null || child == null) {
    		throw new IllegalArgumentException();
    	}
    	
        transId = t;
        this.child = child;
        tableId = tableid;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return SCHEMA;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
    	throw new DbException("Rewind is not supported for INSERT.");
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// If this is the second call to fetchNext, return null.
    	if (child == null) {
    		return null;
    	}
    	
    	child.open();
    	BufferPool bp = Database.getBufferPool();
    	int inserted = 0;
    	while (child.hasNext()) {
    		Tuple t = child.next();
    		try {
				bp.insertTuple(transId, tableId, t);
			} catch (IOException e) {
				throw new DbException("IO operation failed during INSERT.");
			}
    		inserted++;
    	}
    	child.close();
    	child = null;
    	
    	return new Tuple(SCHEMA, new Field[] { new IntField(inserted) });
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
