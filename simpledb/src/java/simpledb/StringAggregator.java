package simpledb;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	private class CountAgg {
		private int value = 0;
		public void add() {
			value++;
		}
		public int get() {
			return value;
		}
	}
	
    private static final long serialVersionUID = 1L;
    
    private static final Field NONE = new IntField(0);
    
    private final int gbField;
    private final Type gbFieldType;
    private final Hashtable<Field, CountAgg> groups;
    private final TupleDesc schema;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (what != Op.COUNT) {
    		throw new IllegalArgumentException("Expected a COUNT operator.");
    	}
    	gbField = gbfield;
        gbFieldType = gbfieldtype;
        groups = new Hashtable<Field, CountAgg>();
        
        Type[] types;
    	if (gbField == NO_GROUPING) {
    		types = new Type[] { Type.INT_TYPE };
    	} else {
    		types = new Type[] { gbFieldType, Type.INT_TYPE };
    	}
    	schema = new TupleDesc(types); 
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field key = gbField == NO_GROUPING ? NONE : tup.getField(gbField);
		// can't aggregate with missing key
		if (!key.isMissing()) {
			// only doing count, so always doable
			CountAgg agg = groups.containsKey(key) ? groups.get(key) : new CountAgg();
			agg.add();
			groups.put(key, agg);
		}
    }
    
    @Override
    public TupleDesc getTupleDesc() {
		return schema;
	}

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new DbIterator() {
        	private Enumeration<Field> keys = null;
        	
			private static final long serialVersionUID = 1L;

			@Override
			public void open() throws DbException, TransactionAbortedException {
				keys = groups.keys();
			}

			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if (keys == null) {
					throw new IllegalStateException("Iterator is not open.");
				}
				return keys.hasMoreElements();
			}

			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				Field key = keys.nextElement();
				CountAgg value = groups.get(key);
				Field setSize = new IntField(value.get());
				if (gbField == NO_GROUPING) {
					return new Tuple(schema, new Field[] { setSize });
				} else {
					return new Tuple(schema, new Field[] { key, setSize });
				}
			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				if (keys == null) {
					throw new IllegalStateException("Iterator is not open.");
				}
				keys = groups.keys();
			}

			@Override
			public TupleDesc getTupleDesc() {
				return schema;
			}

			@Override
			public void close() {
				keys = null;
			}
			
			@Override
			public DbIterator[] getChildren() {
				return new DbIterator[]{};
			}
        };
    }

}
