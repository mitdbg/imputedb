package simpledb;

import java.io.*;
import java.util.*;

/**
 * DbIterator is the iterator interface that all SimpleDB operators should
 * implement. If the iterator is not open, none of the methods should work, and
 * should throw an IllegalStateException. In addition to any resource
 * allocation/deallocation, an open method should call any child iterator open
 * methods, and in a close method, an iterator should call its children's close
 * methods.
 */
public interface DbIterator extends Serializable {
	/**
	 * Opens the iterator. This must be called before any of the other methods.
	 * 
	 * @throws DbException
	 *             when there are problems opening/accessing the database.
	 */
	public void open() throws DbException, TransactionAbortedException;

	/**
	 * Returns true if the iterator has more tuples.
	 * 
	 * @return true f the iterator has more tuples.
	 * @throws IllegalStateException
	 *             If the iterator has not been opened
	 */
	public boolean hasNext() throws DbException, TransactionAbortedException;

	/**
	 * Returns the next tuple from the operator (typically implementing by
	 * reading from a child operator or an access method).
	 *
	 * @return the next tuple in the iteration.
	 * @throws NoSuchElementException
	 *             if there are no more tuples.
	 * @throws IllegalStateException
	 *             If the iterator has not been opened
	 */
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

	/**
	 * Resets the iterator to the start.
	 * 
	 * @throws DbException
	 *             when rewind is unsupported.
	 * @throws IllegalStateException
	 *             If the iterator has not been opened
	 */
	public void rewind() throws DbException, TransactionAbortedException;

	/**
	 * Returns the TupleDesc associated with this DbIterator.
	 * 
	 * @return the TupleDesc associated with this DbIterator.
	 */
	public TupleDesc getTupleDesc();

	/**
	 * Closes the iterator. When the iterator is closed, calling next(),
	 * hasNext(), or rewind() should fail by throwing IllegalStateException.
	 */
	public void close();

	public DbIterator[] getChildren();

	public default double error(DbIterator other) throws DbException, TransactionAbortedException, BadErrorException {
		double err = 0.0;
		int count = 0;
		while (hasNext()) {
			try {
				if (other.hasNext()) {
					err += next().error(other.next());
				} else {
					throw new BadErrorException();
				}
			} catch (NoSuchElementException e) {
				throw new RuntimeException("BUG: No more tuples.");
			}
			count++;
		}
		return Math.sqrt(err / count);
	}
	
	public default void print(PrintStream strm) throws DbException, TransactionAbortedException {
		while (hasNext()) {
			strm.println(next());
		}
	}

	public default void print() throws DbException, TransactionAbortedException {
		print(System.out);
	}

	public default boolean contains(Class<?> op) {
		if (op.isInstance(this)) {
			return true;
		}
		for (DbIterator child : getChildren()) {
			if (child.contains(op)) {
				return true;
			}
		}
		return false;
	}
}
