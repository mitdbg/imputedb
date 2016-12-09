package simpledb;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	private interface Agg {
		public void add(int v);
		public int get();
		public double get_double();
	}
	
	private class MaxAgg implements Agg {
		private int value = Integer.MIN_VALUE;
		public void add(int v) {
			value = Math.max(value, v);
		}
		public int get() {
			return value;
		}
		public double get_double() {
			throw new UnsupportedOperationException("no double for max");
		}
	}
	
	private class MinAgg implements Agg {
		private int value = Integer.MAX_VALUE;
		public void add(int v) {
			value = Math.min(value, v);
		}
		public int get() {
			return value;
		}
		public double get_double() {
			throw new UnsupportedOperationException("no double for min");
		}
	}
	
	private class CountAgg implements Agg {
		private int value = 0;
		public void add(int v) {
			value++;
		}
		public int get() {
			return value;
		}
		public double get_double() {
			throw new UnsupportedOperationException("no double for count");
		}
	}
	
	private class SumAgg implements Agg {
		private int value = 0;
		public void add(int v) {
			value += v;
		}
		public int get() {
			return value;
		}
		public double get_double() {
			throw new UnsupportedOperationException("no double for sum");
		}
	}
	
	private class AvgAgg implements Agg {
		private double count = 0, total = 0;
		public void add(int v) {
			total += v;
			count++;
		}
		public int get() {
			return (int) (total / count);
		}
		public double get_double() {
			return total / count;
		}
	}

    private static final long serialVersionUID = 1L;
    
    private static final Field NONE = new IntField(0);
    
    private final int gbField, aggField;
    private final Type gbFieldType;
    private final Op op;
    private final Hashtable<Field, Agg> groups; 
    private final TupleDesc schema;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {    	
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aggField = afield;
        op = what;
        groups = new Hashtable<Field, Agg>();
        
        Type[] types;
		Type aggType = (op == Op.AVG)?  Type.DOUBLE_TYPE : Type.INT_TYPE;
    	if (gbField == NO_GROUPING) {
			types = new Type[] { aggType};
    	} else {
			types = new Type[] { gbFieldType, aggType };
    	}
    	schema = new TupleDesc(types); 
    }
    
    private Agg newAgg() {
    	switch(op) {
		case AVG:
			return new AvgAgg();
		case COUNT:
			return new CountAgg();
		case MAX:
			return new MaxAgg();
		case MIN:
			return new MinAgg();
		case SUM:
			return new SumAgg();
		case SC_AVG:
		case SUM_COUNT:
		default:
			throw new RuntimeException("Unsupported operation.");
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field key = gbField == NO_GROUPING ? NONE : tup.getField(gbField);
		IntField val = (IntField) tup.getField(aggField);
		// can only aggregate on a key if not missing or None and
		// value is not missing or we are working on a count
		if (!key.isMissing() && (!val.isMissing() || op == Op.COUNT)) {
			int value = (val.isMissing()) ? 1 : val.getValue();
			Agg agg = groups.containsKey(key) ? groups.get(key) : newAgg();
			agg.add(value);
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
				Agg value = groups.get(key);
				Field valField = (op == Op.AVG) ? new DoubleField(value.get_double()) : new IntField(value.get());
				if (gbField == NO_GROUPING) {
					return new Tuple(schema, new Field[] { valField });
				} else {
					return new Tuple(schema, new Field[] { key, valField });
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
