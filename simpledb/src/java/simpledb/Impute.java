package simpledb;

import java.util.ArrayList;
import java.util.Collection;

public abstract class Impute extends Operator {

    private static final long serialVersionUID = 1L;

    protected DbIterator child;
    protected TupleDesc td;
    protected final Collection<String> dropFields;
    protected final Collection<Integer> dropFieldsIndices;

    public Impute(Collection<String> dropFields, DbIterator child){
        this.child = child;
        this.td = child.getTupleDesc();
        
        // Validate drop fields and convert to indices.
        this.dropFields        = dropFields;
        this.dropFieldsIndices = extractDropFieldsIndices(dropFields, td);
    }
    
    public static Collection<Integer> extractDropFieldsIndices(Collection<String> dropFields, TupleDesc td){
        Collection<Integer> dropFieldsIndices = new ArrayList<Integer>();
        for (String field : dropFields){
            if (field == null){
                throw new RuntimeException("All field names must be non-null.");
            }

            try{
                int j = td.fieldNameToIndex(field);
                dropFieldsIndices.add(j);
            } catch (Exception e){
                throw new RuntimeException("Field not present in TupleDesc.");
            }
        }

        return dropFieldsIndices;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Expected a single new child.");
        }
        child = children[0];
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

}
