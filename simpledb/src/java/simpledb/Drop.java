package simpledb;

import java.util.Collection;

public class Drop extends Impute {

	private static final long serialVersionUID = 1L;

	private final Collection<String> dropFields;

	/**
	 * Deal with missing data on a set of column by simply dropping the rows
	 * with missing data.
	 * @param dropFields set of columns to consider
	 * @param child
	 */
	public Drop(Collection<String> dropFields, DbIterator child) {
		super(child);
		if (validateDropFields(dropFields)){
			this.dropFields = dropFields;
		} else {
			throw new RuntimeException("Could not validate dropFields.");
		}
	}

	/**
	 * Validate that each field in dropFields corresponds to a fieldName in
	 * child's TupleDesc.
	 * @param dropFields
	 */
	private boolean validateDropFields(Collection<String> dropFields) {
		for (String field : dropFields){
			if (field == null){
				System.err.println("Could not validate null field.");
				return false;
			}

			try{
				td.fieldNameToIndex(field);
			} catch (Exception e){
				System.err.println("Could not validate field: "+field);
				return false;
			}
		}
		
		return true;
		
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
	}

	@Override
	protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		while (child.hasNext()) {
        	Tuple t = child.next();
        	boolean drop = false;
        	for (String field : dropFields) {
        		if (t.getField(td.fieldNameToIndex(field)).isMissing()) {
        			drop = true;
        			break;
        		}
        	}
        	if (!drop) {
        		return t;
        	}
        }
        return null;
	}
}