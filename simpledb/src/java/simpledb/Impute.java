package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
        if (dropFields != null) {
            this.dropFields = dropFields;
        } else {
            // if no fields provided, then we assume all fields are to be used
            List<String> allFields = new ArrayList<>();
            for(int i = 0; i < td.numFields(); i++) {
                allFields.add(td.getFieldName(i));
            }

            this.dropFields = allFields;
        }
        this.dropFieldsIndices = extractDropFieldsIndices(this.dropFields, td);
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
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }
    
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    public Collection<String> getDropFields() {
        return dropFields;
    }

    /**
     * Return the estimated time cost (computational complexity) of the
     * imputation. This is a unitless quantity
     * @return estimated time
     */
	public abstract double getEstimatedTime(ImputedPlan subplan);

    public abstract double getEstimatedPenalty(ImputedPlan subplan);


}