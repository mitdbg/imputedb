package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;


public class ImputeRandom extends Impute {

	private static final long serialVersionUID = 1L;
	private static final long GENERATOR_SEED = 6831L;

	private Random random;

	private ArrayList<Tuple> buffer;
	private int i; // position of next tuple to return
	
	/**
	 * Impute missing data for a column by selecting a value at random from the
	 * non-missing elements of the same column. Thus, this method requires that
	 * we store all of the child tuples in a buffer until the child is
	 * exhausted.
	 * @param child
	 */
    public ImputeRandom(Collection<String> dropFields, DbIterator child){
    	super(dropFields, child);
    	initRng();
    	
    	buffer = new ArrayList<>();
    	i = 0;
	}
    
    private void initRng(){
    	random = new Random(GENERATOR_SEED);
    }

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
		initRng();
		
		buffer.clear();
		i = 0;
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
		if (i < buffer.size()){
			// Impute tuple i if necessary.
			Tuple t = buffer.get(i);
			if (t.hasMissingFields()){
				Tuple tc = impute(t);
				i++;
				return tc;
			} else {
				i++;
				return t;
			}
		} else {
			return null;
		}
	}

	private Tuple impute(Tuple t) throws DbException {
		Tuple tc = new Tuple(t);
		
		List<Integer> missingFieldIndices = t.missingFieldsIndices();
		for (int j : missingFieldIndices){
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

}
