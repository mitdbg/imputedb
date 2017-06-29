package simpledb;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    static final double IMPUTE_COST_FUDGE_FACTOR = 1.0;

    public static TableStats getTableStats(String tablename) {
        TableStats stats = statsMap.get(tablename);
        if (stats == null) {
        	throw new RuntimeException("No table stats for " + tablename);
        }
        return stats;
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.err.print("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
            System.err.print('.');
        }
        System.err.println(" done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private final IntHistogram[] intStats;
    private final StringHistogram[] stringStats;
    private final int ioCostPerPage;
    private final TupleDesc schema;
    private int numTuples;
    private final int[] nullStats;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
    	this.ioCostPerPage = ioCostPerPage;
    	
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	schema = file.getTupleDesc();
    	final int numFields = schema.numFields();
    	
    	intStats = new IntHistogram[numFields];
    	stringStats = new StringHistogram[numFields];
    	nullStats = new int[numFields];
    	
		final int[] min = new int[numFields], max = new int[numFields];
		int nt = 0;
		Arrays.fill(min, Integer.MAX_VALUE);
		Arrays.fill(max, Integer.MIN_VALUE);
    	DbFileIterator iter = file.iterator(new TransactionId());
    	try {
    		iter.open();
    		
    		// Get min/max values for integer columns.
			while (iter.hasNext()) {
				Tuple tup = iter.next();
				for (int i = 0; i < numFields; i++) {
					if (tup.getTupleDesc().getFieldType(i) == Type.INT_TYPE && !tup.getField(i).isMissing()) {
						int value = ((IntField)tup.getField(i)).getValue();
						min[i] = Math.min(min[i], value);
						max[i] = Math.max(max[i], value);
					}
				}
				nt++;
			}

	    	iter.rewind();

	    	// Fill in histograms.
			while (iter.hasNext()) {
				Tuple tup = iter.next();
				for (int i = 0; i < tup.getTupleDesc().numFields(); i++) {
					if (tup.getField(i).isMissing()) {
						nullStats[i]++;
					} else {
						switch (tup.getTupleDesc().getFieldType(i)) {
						case INT_TYPE:
							if (intStats[i] == null) {
								intStats[i] = new IntHistogram(NUM_HIST_BINS, min[i], max[i]);
							}
							intStats[i].addValue(tup, i);
							break;
						case STRING_TYPE:
							if (stringStats[i] == null) {
								stringStats[i] = new StringHistogram(NUM_HIST_BINS);
							}
							stringStats[i].addValue(tup, i);
							break;
						case DOUBLE_TYPE:
							break;
						default:
							throw new RuntimeException("Unexpected type.");
						}
					}
				}
			}
		} catch (NoSuchElementException | DbException | TransactionAbortedException e) {
			e.printStackTrace();
		} finally {
			this.numTuples = nt;
			iter.close();
		}
    }

	/**
	 * Meant to create copies of histogram information for imputation. Only works on integer columns.
	 *
	 * Solely to be used for copies, as sets ioCostPerPage and stringStats to null/dummy values
	 * @param intStats
	 * @param nullStats
	 */
	private TableStats(TupleDesc schema, IntHistogram[] intStats, int[] nullStats) {
		this.schema = schema;
		this.intStats = intStats;
		this.nullStats = nullStats;
		// we should be able to use any of the columns to count
		this.numTuples = computeTotalTuples();

		// unused
		stringStats = null;
		ioCostPerPage = -1;
	}

	public TableStats setNullStats(int[] nullStats) {
		TableStats copy = copyTableStats();
		assert(nullStats.length == copy.nullStats.length);
		System.arraycopy(nullStats, 0, copy.nullStats, 0, nullStats.length);
		copy.numTuples = copy.computeTotalTuples();
		return copy;
	}

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
		return ((double)totalTuples() / BufferPool.getPageSize()) * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.round(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	boolean isNullConstant = constant.isMissing();
    	switch (schema.getFieldType(field)) {
    	case INT_TYPE:
    		if (isNullConstant) {
    			return intStats[field].estimateSelectivityNull(op);
    		} else {
    			return intStats[field].estimateSelectivity(op, ((IntField)constant).getValue());
    		}
    	case STRING_TYPE:
    		if (isNullConstant) {
    			return stringStats[field].estimateSelectivityNull(op);
    		} else {
    			return stringStats[field].estimateSelectivity(op, ((StringField)constant).getValue());
    		}
    	case DOUBLE_TYPE:
    		// TODO: Real selectivity estimation.
    		return 1.0;
    	default:
    		throw new RuntimeException("Unexpected type.");
    	}
    }

	/**
	 * Convenience wrapper for selectivity estimation
	 * @param pred
	 * @return
	 */
	public double estimateSelectivity(Predicate pred) {
		return estimateSelectivity(pred.getField(), pred.getOp(), pred.getOperand());
	}
    
    public double estimateTotalNull(Collection<Integer> fields) {
    	double ret = 0.0;
    	for (int field : fields) {
    		ret += nullStats[field];
    	}
    	return ret;
    }
    
    public double estimateTotalNull() {
    	double ret = 0.0;
    	for (int n : nullStats) {
    		ret += n;
    	}
    	return ret;
    }
    
    public double estimateVariance(int field){
    	return this.intStats[field].variance();
    }

    public double estimateMean(int field){
        return this.intStats[field].mean();
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

	/**
	 * Actually calculate the total number of tuples (Rather than return the stored field)
	 * @return
	 */
	private int computeTotalTuples(){
		return (int) (intStats[0].countTuples() + nullStats[0]);
	}


	public Set<String> dirtyAttrs() {
		Set<String> ret = new HashSet<String>();
		for (int i = 0; i < schema.numFields(); i++) {
			if (nullStats[i] > 0) {
				ret.add(schema.getFieldName(i));
			}
		}
		return ret;
	}

	/**
	 * Create a copy of table stats instance
	 * @return
	 */
	public TableStats copyTableStats() {
		IntHistogram[] copyIntStats = new IntHistogram[intStats.length];
		int[] copyNullStats = new int[nullStats.length];

		assert(nullStats.length == intStats.length);
		for(int i = 0 ; i < intStats.length; i++) {
			// create copies of histograms
			copyIntStats[i] = intStats[i].copyHistogram();
			// copy over null counts
			copyNullStats[i] = nullStats[i];
		}

		return new TableStats(schema, copyIntStats, copyNullStats);
	}

	public void printValues(IntHistogram[] stats, int[] nulls) {
		System.out.println("Tuple estimates by attribute");
		for(int i = 0; i < stats.length; i++) {
			int ctNonNulls = (int) stats[i].countTuples();
			int ctNulls = nulls[i];
			System.out.print("Attribute " + i + ": ");
			System.out.print(" Non-null: " + ctNonNulls);
			System.out.print(" Null: " + ctNulls);
			System.out.print("\tTotal: " + (ctNonNulls + ctNulls) + "\n");
		}
	}

	/**
	 * Adjust tablestats to reflect imputation on indices using operation imp. The null count associated
	 * with these indices goes down to zero in all cases.
	 *
	 * For drop, we take the attribute that had the largest number of nulls, this provides a single-attribute-based
	 * conservative estimate of the smallest cardinality (i.e. we may have dropped even more tuples).
	 * After setting all other necessary columns' nulls to zero, scale the entire histogram to the new count.
	 *
	 * For imputations,
	 * the null counts are redistributed to the histogram according to the existing distribution, and then
	 * the null counts are set to zero.
	 *
	 * Creates a new instance
	 * @param imp imputation operation
	 * @param indices indices affected
	 * @return
	 */
	public TableStats adjustForImpute(ImputationType imp, Collection<Integer> indices) {
		TableStats copy = copyTableStats();
		switch(imp) {
			case DROP:
				int minCardinality = Integer.MAX_VALUE;
				for(int ix : indices) {
					copy.nullStats[ix] = 0;
					int estimate = (int) copy.intStats[ix].countTuples();
					minCardinality = (estimate < minCardinality) ? estimate : minCardinality;
				}
				// scale all buckets (including those not involved in imputation) to new cardinality estimate
				copy = copy.adjustToTotal(minCardinality);
				break;
			case MINIMAL:
				// intentional fall through
			case MAXIMAL:
				for(int ix : indices) {
					copy.intStats[ix].addToDistribution(copy.nullStats[ix]);
					copy.nullStats[ix] = 0;
				}
				break;
		}
		copy.numTuples = copy.computeTotalTuples();
		return copy;
	}


	/**
	 * Adjust the counts in all histograms based on the selectivity provided. Also adjusts null counts similarly.
	 *
	 * Creates a new instance.
	 * @param selectivity
	 * @return
	 */
	public TableStats adjustForSelectivity(double selectivity) {
		TableStats copy = copyTableStats();
		assert(intStats.length == nullStats.length);
		for(int i = 0; i < copy.intStats.length; i++) {
			copy.intStats[i].scaleBy(selectivity);
			copy.nullStats[i] = (int) (copy.nullStats[i] * selectivity);
		}
		// update number of tuples
		copy.numTuples = copy.computeTotalTuples();
		return copy;
	}

	/**
	 * Adjust counts such that the total number of tuples is equal to countTuples. It assigns values to buckets
	 * in each histogram according to existing distribution. Accounts for nulls.
	 *
	 * Creates a new instance
	 * @param totalCount new total count of tuples
	 * @return
	 */
	public TableStats adjustToTotal(double totalCount) {
		TableStats copy = copyTableStats();
		for(int i = 0; i < copy.intStats.length; i++) {
			// make sure to account for missing values
			double denom = copy.intStats[i].countTuples() + copy.nullStats[i];
			// with some floating point smudge
			double dirtyNull = (copy.nullStats[i] / denom) * totalCount;
			// integral value
			int cleanNull = (int) dirtyNull;
			// assign new null count
			copy.nullStats[i] = cleanNull;
			// scaleTo remainder amongst non-nulls, based on existing histogram
			copy.intStats[i].scaleTo(totalCount - cleanNull);
		}
		// update number of tuples
		copy.numTuples = copy.computeTotalTuples();
		return copy;
	}

	/**
	 * Combine two table stats instances
	 *
	 * Creates new instance
	 * @param other table stats to append
	 * @return
	 */
	public TableStats merge(TableStats other) {
		// create new instances to avoid modifying existing references
		TableStats copy1 = copyTableStats();
		IntHistogram[] intStats1 = copy1.intStats;
		int[] nullStats1 = copy1.nullStats;

		TableStats copy2 = other.copyTableStats();
		IntHistogram[] intStats2 = copy2.intStats;
		int[] nullStats2 = copy2.nullStats;

		// combine histograms
		IntHistogram[] combinedIntStats = new IntHistogram[intStats1.length + intStats2.length];
		System.arraycopy(intStats1, 0, combinedIntStats, 0, intStats1.length);
		System.arraycopy(intStats2, 0, combinedIntStats, intStats1.length, intStats2.length);

		// combine null stats
		int[] combinedNullStats = new int[nullStats1.length + nullStats2.length];
		System.arraycopy(nullStats1, 0, combinedNullStats, 0, nullStats1.length);
		System.arraycopy(nullStats2, 0, combinedNullStats, nullStats1.length, nullStats2.length);

		// combine schemas
		TupleDesc combinedSchema = TupleDesc.merge(schema, other.schema);

		return new TableStats(combinedSchema, combinedIntStats, combinedNullStats);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(int i = 0; i < intStats.length; i++) {
			sb.append("col ");
			sb.append(i);
			sb.append(": not null[");
			sb.append(intStats[i].countTuples());
			sb.append("], null[");
			sb.append(nullStats[i]);
			sb.append("]\n");
		}
		return sb.toString();
	}
}
