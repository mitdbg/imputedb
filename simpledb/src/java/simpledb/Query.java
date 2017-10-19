package simpledb;

import java.io.*;
import java.util.*;

/**
 * Query is a wrapper class to manage the execution of queries. It takes a query
 * plan in the form of a high level DbIterator (built by initiating the
 * constructors of query plans) and runs it as a part of a specified
 * transaction.
 * 
 * @author Sam Madden
 */

public class Query implements Serializable {

    private static final long serialVersionUID = 1L;

    transient private DbIterator op;
    transient private LogicalPlan logicalPlan;
    TransactionId tid;
    transient private boolean started = false;

    public TransactionId getTransactionId() {
        return this.tid;
    }

    public void setLogicalPlan(LogicalPlan lp) {
        this.logicalPlan = lp;
    }

    public LogicalPlan getLogicalPlan() {
        return this.logicalPlan;
    }

    public void setPhysicalPlan(DbIterator pp) {
        this.op = pp;
    }

    public DbIterator getPhysicalPlan() {
        return this.op;
    }

    public Query(TransactionId t) {
        tid = t;
    }

    public Query(DbIterator root, TransactionId t) {
        op = root;
        tid = t;
    }

    public void start() throws IOException, DbException,
            TransactionAbortedException {
        op.open();

        started = true;
    }

    public TupleDesc getOutputTupleDesc() {
        return this.op.getTupleDesc();
    }

    /** @return true if there are more tuples remaining. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return op.hasNext();
    }

    /**
     * Returns the next tuple, or throws NoSuchElementException if the iterator
     * is closed.
     * 
     * @return The next tuple in the iterator
     * @throws DbException
     *             If there is an error in the database system
     * @throws NoSuchElementException
     *             If the iterator has finished iterating
     * @throws TransactionAbortedException
     *             If the transaction is aborted (e.g., due to a deadlock)
     */
    public Tuple next() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if (!started)
            throw new DbException("Database not started.");

        return op.next();
    }

    /** Close the iterator */
    public void close() throws IOException {
        op.close();
        started = false;
    }

    public void execute(ATupleFormatter formatter) throws IOException, DbException, TransactionAbortedException {
        TupleDesc td = this.getOutputTupleDesc();

        formatter.formatHeader(td);

        int cnt = 0;

        this.start();
        while (this.hasNext()) {
            formatter.formatTuple(this.next());
            cnt++;
        }
        this.close();

        formatter.formatFooter("\n " + cnt + " rows.");
    }
}
