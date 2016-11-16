package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private final Field[] fields;
    
    private TupleDesc schema;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        schema = td;
        fields = new Field[schema.numFields()];
    }
    
    public Tuple(TupleDesc td, Field[] fields) {
    	if (td.numFields() != fields.length) {
    		throw new IllegalArgumentException("Schema does not match fields.");
    	}
    	schema = td;
    	this.fields = fields;
    }
    
    /**
     * Copy constructor
     * @param t
     */
    public Tuple(Tuple t){
    	this(t.schema, Arrays.copyOf(t.fields, t.fields.length));
    }
    
    /**
     * Create a new tuple which is the concatenation of two existing tuples.
     */
    public Tuple(Tuple t1, Tuple t2) {
    	schema = TupleDesc.merge(t1.schema, t2.schema);
    	fields = new Field[schema.numFields()];
    	System.arraycopy(t1.fields, 0, fields, 0, t1.fields.length);
    	System.arraycopy(t2.fields, 0, fields, t1.fields.length, t2.fields.length);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	for (int i = 0; i < fields.length; i++) {
    		sb.append(fields[i].toString());
    		if (i < fields.length - 1) {
    			sb.append(",");
    		}
    	}
    	return sb.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return Arrays.asList(fields).iterator();
    }
    
    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
        schema = td;
    }

    /**
     * True if tuple has any missing fields
     * @return
     */
    public boolean hasMissingFields() {
        for(Field field : fields) {
            if (field.isMissing()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns list of missing field indices
     * @return
     */
    public List<Integer> missingFieldsIndices() {
        List<Integer> missing = new ArrayList<>();
        for(int i = 0; i < getTupleDesc().numFields(); i++) {
            if (getField(i).isMissing()) {
                missing.add(i);
            }
        }
        return missing;
    }

}
