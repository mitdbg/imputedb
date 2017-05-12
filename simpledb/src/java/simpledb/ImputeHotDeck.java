package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;


public class ImputeHotDeck extends Impute {

    private static final long serialVersionUID = 1L;
    private static final long GENERATOR_SEED = 6831L;

    private Random random;

    private ArrayList<Tuple> buffer;
    private int nextTupleIndex; // position of next tuple to return

    /**
     * Impute missing data for a column by selecting a value at random from the
     * non-missing elements of the same column. Thus, this method requires that
     * we store all of the child tuples in a buffer until the child is
     * exhausted.
     * @param child
     */
    public ImputeHotDeck(Collection<String> dropFields, DbIterator child){
        super(dropFields, child);
        initRng();

        buffer = new ArrayList<>();
        nextTupleIndex = 0;
    }

    public ImputeHotDeck(DbIterator child) {
        this(null, child);
    }

    private void initRng(){
        random = new Random(GENERATOR_SEED);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        initRng();

        buffer.clear();
        nextTupleIndex = 0;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        // First, we must add all of the child tuples to the buffer.
        // TODO If !child.hasNext(), does that mean that there are no more
        //      tuples at all that will come from this child, or can some be added in
        //      the future?
        while (child.hasNext()){
            buffer.add(child.next());
        }

        // Get tuple if any are remaining in buffer.
        if (nextTupleIndex < buffer.size()){
            // Impute tuple i if necessary.
            Tuple t = buffer.get(nextTupleIndex);
            nextTupleIndex++;
            if (t.hasMissingFields()){
                Tuple tc = impute(t);
                return tc;
            } else {
                return t;
            }
        } else {
            return null;
        }
    }

    private Tuple impute(Tuple t) throws DbException {
        Tuple tc = new Tuple(t);

        for (int j : dropFieldsIndices){
            // Don't impute if not missing.
            if (!tc.getField(j).isMissing())
                continue;

            // Select non-missing field at random.
            int index0 = random.nextInt(buffer.size());
            int index = index0;
            while (buffer.get(index).getField(j).isMissing()){
                index++;

                // Loop around at end of buffer.
                if (index == buffer.size())
                    index = 0;

                // If we're back to beginning, then terminate, as there are no
                // non-missing values at all.
                if (index == index0)
                    throw new DbException("Couldn't impute: No non-missing values for field "+j);
            }

            // Fill in field.
            tc.setField(j, buffer.get(index).getField(j));
        }

        return tc;
    }

    /*
     * Can conceive this as O(n m_i)
     * @see simpledb.Impute#getEstimatedTime()
     */
	@Override
	public double getEstimatedTime(ImputedPlan subplan) {
        int numTuples = (int) subplan.cardinality();
        int numDirty= subplan.getDirtySet().size(); // number of dirty attributes
		double T = numTuples * numDirty;
		
		return T;
	}

	@Override
    public double getEstimatedPenalty(ImputedPlan subplan) {
        return 0.5;
    }

}
