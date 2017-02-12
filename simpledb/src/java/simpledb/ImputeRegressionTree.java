package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import weka.core.Instance;
import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.trees.REPTree;

public class ImputeRegressionTree extends Impute {

    private static final long serialVersionUID = 1L;
    private static final long GENERATOR_SEED = 6832L;
    private static final int NUM_IMPUTATION_EPOCHS = 10;

    private Random random;

    private ArrayList<Tuple> buffer;
    private int nextTupleIndex;

    // Keep track of imputed instances as well as the indices that correspond to
    // these. (Will differ from completeFieldsIndices and dropFieldsIndices if
    // this is a partial impute.)
    private Instances imputedInstances;
    private List<Integer> completeFieldsIndices2;
    private List<Integer> dropFieldsIndices2;

    public ImputeRegressionTree(Collection<String> dropFields, DbIterator child) {
        super(dropFields, child);
        initRng();
        buffer = new ArrayList<>();
        nextTupleIndex = 0;

        imputedInstances = null;
        completeFieldsIndices2 = null;
        dropFieldsIndices2 = null;
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

        imputedInstances = null;
        completeFieldsIndices2 = null;
        dropFieldsIndices2 = null;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (imputedInstances == null){
            // First, we must add all of the child tuples to the buffer.
            while (child.hasNext()){
                buffer.add(child.next());
            }
            
            // Get set of complete columns and missing columns. The complete columns
            // are just those -- columns without any missing data. We can't assume
            // that the complete columns are the set complement of the dropFields.
            // The missing columns -- the ones to impute -- are not necessarily the
            // set of all columns that have any missing data due to allowing partial
            // imputation. We start with the set difference of all columns with the
            // drop fields. Then, we remove any columns that have any missing
            // values.
            List<Integer> completeFieldsIndices = new ArrayList<>();
            for (int i=0; i<td.numFields(); i++){
                if (!dropFieldsIndices.contains(i)){
                    completeFieldsIndices.add(i);
                }
            }
            // Can we populate the missing bit array above and then re-use that here?
			Iterator<Integer> it = completeFieldsIndices.iterator();
			while (it.hasNext()){
				int i = it.next();
				for (int j=0; j<buffer.size(); j++){
					if (buffer.get(j).getField(i).isMissing()){
						it.remove();
						continue;
					}
				}
			}

            // Populate Instances object
            List<Integer> allFieldsToInclude = new ArrayList<Integer>(completeFieldsIndices);
            allFieldsToInclude.addAll(dropFieldsIndices);
            Instances train = WekaUtil.relationToInstances("", buffer, td, allFieldsToInclude);
            
            // Now, the problem is that if we haven't retained every column, our indices will be off.
            // In this silly approach, we'll recreate the lists of indices by comparing the field names.
            List<Integer> completeFieldsIndices2 = new ArrayList<>();
            for (int it : completeFieldsIndices){
                String name = td.getFieldName(it);
                boolean added = false;
                for (int ii=0; ii<train.numAttributes(); ii++){
                    if (train.attribute(ii).name().equals(name)){
                        completeFieldsIndices2.add(ii);
                        added = true;
                        break;
                    }
                }
                if (!added){
                    throw new RuntimeException("Not added.");
                }
            }
            List<Integer> dropFieldsIndices2 = new ArrayList<>();
            for (int it : dropFieldsIndices){
                String name = td.getFieldName(it);
                boolean added = false;
                for (int ii=0; ii<train.numAttributes(); ii++){
                    if (train.attribute(ii).name().equals(name)){
                        dropFieldsIndices2.add(ii);
                        added = true;
                        break;
                    }
                }
                if (!added){
                    throw new RuntimeException("Not added.");
                }
            }
            this.completeFieldsIndices2 = completeFieldsIndices2;
            this.dropFieldsIndices2 = dropFieldsIndices2;

            // Keep record of original dataset before imputation.
            // TODO initialize bit matrix of missing values instead.
            Instances trainCopy = new Instances(train);

            // Initialize all missing values using random-in-column.
            int n = train.numInstances();
            for (int i : dropFieldsIndices2){
                for (int j=0; j<n; j++){
                    if (train.get(j).isMissing(i)){
                        int ind = random.nextInt(n);
                        int ind0 = ind;
                        while (trainCopy.get(ind).isMissing(i)){
                            ind++;
                            if (ind == n)
                                ind = 0;
                            if (ind == ind0)
                                throw new DbException("Couldn't initialize impute: no non-missing values for field.");
                        }
                        train.get(j).setValue(i, trainCopy.get(ind).value(i));
                    }
                }
            }

            // // debug: print header and instances.
            // System.out.println("\ndataset:\n");
            // System.out.println(train);    

            // Iterate creation of trees for each missing column. This is the
            // meat of the chained-equation regression trees method.
            for (int j=0; j<NUM_IMPUTATION_EPOCHS; j++){
                for (int imputationColumn : dropFieldsIndices2){
                    train.setClassIndex(imputationColumn);
                
                    // Create and train tree. Note that use of "Class"/"Classifier" everywhere is a
                    // misnomer; the REPTree will produce numerical outputs if the "Class"
                    // column is numerical.
                    Classifier tree = new REPTree();
                    try {
                        tree.buildClassifier(train);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new DbException("Failed to train classifier.");
                    }

                    // Replace all the originally missing values in imputationColumn with
                    // predicted values.
                    for (int i=0; i<train.size(); i++){
                        if (trainCopy.get(i).isMissing(imputationColumn)){
                            try {
                                double pred = tree.classifyInstance(train.get(i));
                                train.get(i).setValue(imputationColumn, pred);
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new DbException("Failed to classify instance.");
                            }
                        }
                    }
                }
            }
            
            imputedInstances = train;
        }
        
        // We've imputed values, so we can actually return them now.
        if (nextTupleIndex < buffer.size()){
            Tuple original = buffer.get(nextTupleIndex);
            Instance inst = imputedInstances.get(nextTupleIndex);

            Tuple imputed = new Tuple(original);

            // Naive merge of the tuple and instance. The instance may have
            // fewer columns and may have out-of-order columns. We can simplify
            // because we have the guarantee that only values in dropFields will
            // change, and every other value will stay the same.
            for (int it : dropFieldsIndices){
                String field = td.getFieldName(it);
                for (int ii : dropFieldsIndices2){
                    if (inst.attribute(ii).name().equals(field)){
                        double value = inst.value(ii);
                        if (td.getFieldType(it) == Type.INT_TYPE){
                            imputed.setField(it, new IntField((int) value));
                        } else if (td.getFieldType(it) == Type.DOUBLE_TYPE){
                            imputed.setField(it, new DoubleField(value));
                        } else {
                            throw new RuntimeException("Not implemented.");
                        }
                    }
                }
            }
            
            nextTupleIndex++;
            
            return imputed;
        } else {
            return null;
        }
    }

    /*
     * For impute regression tree, we have that
     * - fitting one decision tree is O(nm log n) where m is the number of dependent attributes
     * - fitting one decision tree where we have m_c complete attributes and m_i
     *   dirty attributes (thus subtract one which is being imputed) is O(n (m_c +
     *   m_i - 1) log n)
     * - fitting m_i decision trees is O(m_i n (m_c + m_i - 1) log (n))
     * - fitting m_i decision trees, k times (where k is the number of
     *   imputation epochs) is O(k * m_i * n * (m_c + m_i - 1) log (n))
     * Note that these computations ignore
     * - the cost of initializing the dirty attributes with an "impute random" strategy
     * - the cost of pruning
     * - a more sophisticated cost complexity calculation
     * @see simpledb.Impute#getEstimatedCost(int, int, int)
     */
	@Override
	public double getEstimatedCost(int numDirty, int numComplete, int numTuples) {
		int m_c = numComplete;
		int m_i = numDirty;
		int n = numTuples;
		int k = NUM_IMPUTATION_EPOCHS;
		double T = k * m_i * n * (m_c + m_i - 1) * Math.log(n);
		
		return T;
	}

}
