package simpledb;

import java.util.Arrays;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private final double[] buckets;
	// count of missing fields associated with tuples in a given bucket
	private final double[] missingFields;
	// count of tuples with any missing fields in a given bucket
	private final double[] missingTuples;
	private final int min, max;
	private final int valuesPerBucket;
	private int numValues;
	// count of missing values in this field
	private int ctMissing;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) { 
    	if (min > max || buckets <= 0) {
    		throw new IllegalArgumentException();
    	}
    	int numBuckets = Math.min(buckets, max - min + 1);
    	this.buckets = new double[numBuckets];
		this.missingFields = new double[numBuckets];
		this.missingTuples = new double[numBuckets];
    	this.min = min;
    	this.max = max;
    	valuesPerBucket = (int) Math.ceil((max - min + 1) / (double)this.buckets.length);
    }

	/**
	 * Constructor only meant to be used for copying a histogram. We have abandoned the missingFields/missingTuples
	 * stats, so these are initialized to null (and never used)
	 * @param buckets
	 * @param min
	 * @param max
	 * @param valuesPerBucket
	 */
    private IntHistogram(double[] buckets, int min, int max, int valuesPerBucket) {
		this.buckets = buckets;
		this.min = min;
		this.max = max;
		this.valuesPerBucket = valuesPerBucket;
		this.numValues = (int) sum(buckets);

		// unused
		missingFields = null;
		// count of tuples with any missing fields in a given bucket
		missingTuples = null;
	}
    
    private int bucketOfValue(int v) {
    	return (v - min) / valuesPerBucket;
    }
    
    private int bucketMin(int v) {
    	return (bucketOfValue(v) * valuesPerBucket) + min;
    }
    
    private int bucketMax(int v) {
    	return bucketMin(v) + valuesPerBucket - 1;
    }
    
    private double bucketMean(int v){
    	return (bucketMin(v) + bucketMax(v)) / 2.0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	buckets[bucketOfValue(v)]++;
    	numValues++;
    }

    public void addValue(Tuple tup, int index) {
		Field f = tup.getField(index);
		if (f.isMissing()) {
			incrCtMissing();
		} else {
			int vix = bucketOfValue(((IntField) f).getValue());
			buckets[vix]++;
			// count of missing fields in this bucket
			List<Integer> missingIndices = tup.missingFieldsIndices();
			missingFields[vix] += missingIndices.size();
			missingTuples[vix] += (missingIndices.isEmpty()) ? 0 : 1;
		}
		numValues++;
	}

	public void incrCtMissing() {
		ctMissing++;
	}

	public int getCtMissing() {
		return ctMissing;
	}

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
		double selValues;

		if (v < min) {
			switch(op) {
				case EQUALS:
					return 0.0;
				case GREATER_THAN:
					return 1.0;
				case GREATER_THAN_OR_EQ:
					return 1.0;
				case LESS_THAN:
					return 0.0;
				case LESS_THAN_OR_EQ:
					return 0.0;
				case LIKE:
					throw new RuntimeException("LIKE not valid for integer values.");
				case NOT_EQUALS:
					return 1.0;
				default:
					throw new RuntimeException("Unexpected operator.");
			}
		}

		if (v > max) {
			switch(op) {
				case EQUALS:
					return 0.0;
				case GREATER_THAN:
					return 0.0;
				case GREATER_THAN_OR_EQ:
					return 0.0;
				case LESS_THAN:
					return 1.0;
				case LESS_THAN_OR_EQ:
					return 1.0;
				case LIKE:
					throw new RuntimeException("LIKE not valid for integer values.");
				case NOT_EQUALS:
					return 1.0;
				default:
					throw new RuntimeException("Unexpected operator.");
			}
		}

		switch(op) {
			case EQUALS:
				selValues = buckets[bucketOfValue(v)] / valuesPerBucket;
				break;
			case GREATER_THAN:
				selValues = (buckets[bucketOfValue(v)] / valuesPerBucket) * (bucketMax(v) - v);
				for (int b = bucketOfValue(v) + 1; b < buckets.length; b++) {
					selValues += buckets[b];
				}
				break;
			case GREATER_THAN_OR_EQ:
				selValues = (buckets[bucketOfValue(v)] / valuesPerBucket) * (bucketMax(v) - v + 1);
				for (int b = bucketOfValue(v) + 1; b < buckets.length; b++) {
					selValues += buckets[b];
				}
				break;
			case LESS_THAN:
				selValues = (buckets[bucketOfValue(v)] / valuesPerBucket) * (v - bucketMin(v));
				for (int b = bucketOfValue(v) - 1; b >= 0; b--) {
					selValues += buckets[b];
				}
				break;
			case LESS_THAN_OR_EQ:
				selValues = (buckets[bucketOfValue(v)] / valuesPerBucket) * (v - bucketMin(v) + 1);
				for (int b = bucketOfValue(v) - 1; b >= 0; b--) {
					selValues += buckets[b];
				}
				break;
			case LIKE:
				throw new RuntimeException("LIKE not valid for integer values.");
			case NOT_EQUALS:
				selValues = numValues - (buckets[bucketOfValue(v)] / valuesPerBucket);
				break;
			default:
				throw new RuntimeException("Unexpected operator.");
		}

		return selValues / numValues;
	}

	/**
	 * Estimating selectivity relative to null (i.e. missing). Only defined over equals and not-equals
	 * This is a stand in for traditional SQL
	 * SELECT * FROM t WHERE c1 IS [NOT] NULL
	 * @param op
	 * @return
	 */
	public double estimateSelectivityNull(Predicate.Op op) {
		switch(op) {
			case EQUALS:
				return ((double) getCtMissing()) / numValues;
			case NOT_EQUALS:
				return ((double) numValues - getCtMissing()) / numValues;
			default:
				return 0.0;
		}
	}

	private double sum(double[] vs) {
		double res = 0;
		for (double v : vs) res+= v;
		return res;
	}
	
	public double mean() {
		double sum = 0.0;
		for (int v=0; v<buckets.length; v++){
			sum += buckets[v] * bucketMean(v);
		}
		return sum / this.numValues;
	}
	
	public double variance() {
		double mean = this.mean();
		double sum  = 0.0;
		for (int v=0; v<buckets.length; v++){
			sum += buckets[v] * Math.pow(bucketMean(v) - mean, 2.0);
		}
		return sum / this.numValues;
	}

	/**
	 * Estimate number of empty fields, or tuples with at least one empty field, result from the given selection.
	 * @param op
	 * @param v
	 * @param granular if true, returns number of fields, if false, returns number of tuples
	 * @return
	 */
	public double estimateMissing(Predicate.Op op, int v, boolean granular) {
		double[] missing;

		if (granular) {
			missing = missingFields;
		} else {
			missing = missingTuples;
		}

		double estimate = 0;

		if (v < min) {
			switch(op) {
				case EQUALS:
				case LIKE:
					return 0;
				case GREATER_THAN:
				case GREATER_THAN_OR_EQ:
					return sum(missing);
				case LESS_THAN:
				case LESS_THAN_OR_EQ:
					return 0;
				case NOT_EQUALS:
					return sum(missing);
				default:
					throw new RuntimeException("Unexpected operator.");
			}
		}

		if (v > max) {
			switch(op) {
				case EQUALS:
				case LIKE:
				case GREATER_THAN:
				case GREATER_THAN_OR_EQ:
					return 0;
				case LESS_THAN:
				case LESS_THAN_OR_EQ:
					return sum(missing);
				case NOT_EQUALS:
					return sum(missing);
				default:
					throw new RuntimeException("Unexpected operator.");
			}
		}

		switch(op) {
			case EQUALS:
				estimate += missing[bucketOfValue(v)] / valuesPerBucket;
				break;
			case GREATER_THAN:
				estimate += (missing[bucketOfValue(v)] / valuesPerBucket) * (bucketMax(v) - v);
				for (int b = bucketOfValue(v) + 1; b < missing.length; b++) {
					estimate += missing[b];
				}
				break;
			case GREATER_THAN_OR_EQ:
				estimate += (missing[bucketOfValue(v)] / valuesPerBucket) * (bucketMax(v) - v + 1);
				for (int b = bucketOfValue(v) + 1; b < missing.length; b++) {
					estimate += missing[b];
				}
				break;
			case LESS_THAN:
				estimate += (missing[bucketOfValue(v)] / valuesPerBucket) * (v - bucketMin(v));
				for (int b = bucketOfValue(v) - 1; b >= 0; b--) {
					estimate += missing[b];
				}
				break;
			case LESS_THAN_OR_EQ:
				estimate += (missing[bucketOfValue(v)] / valuesPerBucket) * (v - bucketMin(v) + 1);
				for (int b = bucketOfValue(v) - 1; b >= 0; b--) {
					estimate += missing[b];
				}
				break;
			case LIKE:
				throw new RuntimeException("LIKE not valid for integer values.");
			case NOT_EQUALS:
				estimate += sum(missing) - (missing[bucketOfValue(v)] / valuesPerBucket);
				break;
			default:
				throw new RuntimeException("Unexpected operator.");
		}

		return estimate;
	}

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }

	/**
	 * Return a count of the (non-null) tuples by summing across buckets
	 * @return
	 */
	public double countTuples() {
		return sum(buckets);
	}

	/**
	 * Create a copy of the existing histogram
	 * @return
	 */
	public IntHistogram copyHistogram() {
		double[] copyBuckets = Arrays.copyOf(buckets, buckets.length);
		return new IntHistogram(copyBuckets, min, max, valuesPerBucket);
	}

	/**
	 * Adjust buckets
	 * @param ct count to use for adjustment
	 * @param denom denominator for adjustment
	 * @param add if true, adds to existing value, otherwise overwrites existing value
	 */
	private void adjustBuckets(double ct, double denom, boolean add) {
		for(int i = 0; i < buckets.length; i++) {
			if (add) {
				buckets[i] += (buckets[i] / denom) * ct;
			} else {
				buckets[i] = (buckets[i] / denom) * ct;
			}
		}
	}

	/**
	 * Scale all buckets by a constant factor
	 * @param factor
	 */
	public void scaleBy(double factor) {
		adjustBuckets(factor, 1, false);
		numValues = (int) countTuples();
	}

	/**
	 * Add a number of new tuples to the existing distribution
	 * @param newTuples
	 */
	public void addToDistribution(double newTuples) {
		adjustBuckets(newTuples, countTuples(), true);
		numValues += (int) newTuples;
	}

	/**
	 * Set counts in each bucket according to existing distribution s.t. total number of couts
	 * is equal to ctTuples
	 * @param ctTuples
	 */
	public void scaleTo(double ctTuples) {
		adjustBuckets(ctTuples, countTuples(), false);
		numValues = (int) ctTuples;
	}

	public double[] getBuckets() {
		return buckets;
	}


}
