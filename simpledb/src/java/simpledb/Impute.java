package simpledb;

import java.util.Collection;

public abstract class Impute extends Operator {

    private static final long serialVersionUID = 1L;

    protected DbIterator child;
    protected TupleDesc td;
	protected final Collection<String> dropFields;

    public Impute(Collection<String> dropFields, DbIterator child){
    	this.child = child;
    	this.td = child.getTupleDesc();
		if (validateDropFields(dropFields)){
			this.dropFields = dropFields;
		} else {
			throw new RuntimeException("Could not validate dropFields.");
		}
    }

	/**
	 * Validate that each field in dropFields corresponds to a fieldName in
	 * child's TupleDesc. Disallows null (anonymous) fields.
	 * @param dropFields
	 */
	private boolean validateDropFields(Collection<String> dropFields) {
		for (String field : dropFields){
			if (field == null){
				return false;
			}

			try{
				td.fieldNameToIndex(field);
			} catch (Exception e){
				return false;
			}
		}
		
		return true;
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
