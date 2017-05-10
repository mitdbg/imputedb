package simpledb;

import java.util.Collection;
import java.util.Random;

public class ImputeTotallyRandom extends Impute {

    private static final long serialVersionUID = 1L;
    private static final long GENERATOR_SEED = 6830L;
    private static final int ALPHABET_SIZE = 26;

    private Random random;

    /**
     * Impute missing data by drawing values totally at random. Integer fields
     * are drawn from a uniform distribution on [Integer.MIN_VALUE,
     * Integer.MAX_VALUE] and String fields are drawn from a uniform
     * distribution on ["a"*maxSize, "z"*maxSize].
     * @param child
     */
    public ImputeTotallyRandom(Collection<String> dropFields, DbIterator child){
        super(dropFields, child);
        initRng();
    }
    
    private void initRng(){
        random = new Random(GENERATOR_SEED);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        initRng();
    }

    /**
     * Fetch next tuple, imputing if necessary. We can do this in a streaming
     * fashion as we simply impute totally random values given any constraints
     * on the column.
     */
    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (child.hasNext()){
            Tuple t = child.next();
            
            // Populate "complete" tuple.
            // TODO should skip creation of new tuple if, regardless of
            // hasMissingFields, none of the missing fields are in dropFields
            // collection.
            if (t.hasMissingFields()){
                Tuple tc = impute(t);
                return tc;
            }
            
            return t;
        }
            
        return null;
    }
    
    private Tuple impute(Tuple t) throws DbException {
        Tuple tc = new Tuple(t);
        for (int i : dropFieldsIndices){
            // Don't impute if not missing.
            if (!tc.getField(i).isMissing())
                continue;
            
            if (t.getField(i).getType().equals(Type.INT_TYPE)){
                int randomInt = random.nextInt();
                tc.setField(i, new IntField(randomInt));
            } else if (t.getField(i).getType().equals(Type.STRING_TYPE)){
                int size = ((StringField) t.getField(i)).getSize();
                String randomString = drawRandomString(size);
                tc.setField(i, new StringField(randomString, size));
                } else {
                    throw new DbException("Something went wrong.");
                }
        }
        
        return tc;
        
    }

    /**
     * Draw random string of fixed length 'length'. Each character lies between
     * 'a' and the character at 'a' + ALPHABET_SIZE in the given character set.
     * @param length of the string
     * @return random string of length 'length'
     */
    private String drawRandomString(int length) {
        char[] result = new char[length];
        for (int i=0; i<length; i++){
            int ci = random.nextInt(ALPHABET_SIZE);
            char c = (char) ('a' - 1 + ci);
            result[i] = c;
        }
        
        return String.valueOf(result);
    }

	@Override
	public double getEstimatedTime(ImputedPlan subplan) {
		return 0;
	}

	@Override
    public double getEstimatedPenalty(ImputedPlan subplan) {
        return 0.5;
    }

}
