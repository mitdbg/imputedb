package simpledb;

import java.io.PrintStream;

public class PrintTupleFormatter extends ATupleFormatter {
    public PrintTupleFormatter(PrintStream out) {
        super(out);
    }

    @Override
    public void formatHeader(TupleDesc td) {
        String names = "";
        for (int i = 0; i < td.numFields(); i++) {
            names += td.getFieldName(i) + "\t";
        }
        out.println(names);
        for (int i = 0; i < names.length() + td.numFields() * 4; i++) {
            out.print("-");
        }
        out.println("");
    }

    @Override
    public void formatTuple(Tuple t) {
        out.println(t);
    }

    @Override
    public void formatFooter(String f) {
        out.println(f);
    }
}
