package simpledb;

import java.io.PrintStream;

public class CsvTupleFormatter extends ATupleFormatter {
    public CsvTupleFormatter(PrintStream out) {
        super(out);
    }

    @Override
    public void formatHeader(TupleDesc td) {
        for (int i = 0; i < td.numFields(); i++) {
            out.print(td.getFieldName(i));
            if (i < td.numFields() - 1) {
                out.print(',');
            }
        }
        out.print('\n');
    }

    @Override
    public void formatTuple(Tuple t) {
        TupleDesc td = t.getTupleDesc();
        for (int i = 0; i < td.numFields(); i++) {
            out.print(t.getField(i).toString());
            if (i < td.numFields() - 1) {
                out.print(',');
            }
        }
        out.print('\n');
    }

    @Override
    public void formatFooter(String f) {}
}
