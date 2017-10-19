package simpledb;

import java.io.PrintStream;

/**
 * A tuple formatter prints tuples to some output stream.
 *
 * @author Jack Feser
 */

public abstract class ATupleFormatter {
    protected final PrintStream out;

    public ATupleFormatter(PrintStream out) {
        this.out = out;
    }

    public abstract void formatHeader(TupleDesc td);
    public abstract void formatTuple(Tuple t);
    public abstract void formatFooter(String f);
}
