package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private final double[] buckets;
	private final int min, max;
	private final int valuesPerBucket;
	private int numValues;
	
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
    	this.min = min;
    	this.max = max;
    	valuesPerBucket = (int) Math.ceil((max - min + 1) / (double)this.buckets.length);
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

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	buckets[bucketOfValue(v)]++;
    	numValues++;
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
}
