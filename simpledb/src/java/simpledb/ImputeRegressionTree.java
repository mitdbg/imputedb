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
	private Instances imputedInstances;
	private int nextTupleIndex;

	public ImputeRegressionTree(Collection<String> dropFields, DbIterator child) {
		super(dropFields, child);
		initRng();
		buffer = new ArrayList<>();
		imputedInstances = null;
		nextTupleIndex = 0;
	}
	
	private void initRng(){
		random = new Random(GENERATOR_SEED);
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
		initRng();
		buffer.clear();
		imputedInstances = null;
		nextTupleIndex = 0;
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
			for (int j=0; j<buffer.size(); j++){
				Tuple t = buffer.get(j);
				Iterator<Integer> it = completeFieldsIndices.iterator();
				while (it.hasNext()){
					int i = it.next();
					if (t.getField(i).isMissing()){
						it.remove();
					}
				}
			}

			// Populate Instances object
			List<Integer> allFieldsToInclude = new ArrayList<Integer>(completeFieldsIndices);
			allFieldsToInclude.addAll(dropFieldsIndices);
			Instances train = WekaUtil.relationToInstances("", buffer, td, allFieldsToInclude);
			
			// Now, the problem is that if we haven't retained every column, our indices will be slightly off.
			// In this silly approach, we'll compare the field names to the attribute names.
			List<Integer> completeFieldsIndices2 = new ArrayList<>();
			for (int i : completeFieldsIndices){
				String name = td.getFieldName(i);
				for (int j=0; j<train.numAttributes(); j++){
					if (train.attribute(j).name().equals(name)){
						completeFieldsIndices2.add(j);
						continue;
					}
				}
			}
			List<Integer> dropFieldsIndices2 = new ArrayList<>();
			for (int i : dropFieldsIndices){
				String name = td.getFieldName(i);
				for (int j=0; j<train.numAttributes(); j++){
					if (train.attribute(j).name().equals(name)){
						dropFieldsIndices2.add(j);
						continue;
					}
				}
			}

			// Used as reference for missing values/original dataset.
			// TODO initialize bit matrix of missing values.
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

			// DEBUG: Print header and instances.
			System.out.println("\nDataset:\n");
			System.out.println(train);	

			// Iterate creation of trees for each missing column.
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
		
		if (nextTupleIndex < buffer.size()){
			Instance inst = imputedInstances.get(nextTupleIndex);
			Tuple t = buffer.get(nextTupleIndex);
			Tuple nextTuple = new Tuple(t);

			// Naive merge of the tuple and instance.
			for (int i=0; i<td.numFields(); i++){
				String field = td.getFieldName(i);
				for (int j=0; j<inst.numAttributes(); j++){
					if (inst.attribute(j).name().equals(field)){
						double value = inst.value(j);
						if (td.getFieldType(i) == Type.INT_TYPE){
							nextTuple.setField(i, new IntField((int) value));
						} else if (td.getFieldType(i) == Type.DOUBLE_TYPE){
							nextTuple.setField(i, new DoubleField(value));
						} else {
							throw new RuntimeException("Not implemented.");
						}
					}
				}
			}
			
			nextTupleIndex++;
			
			return nextTuple;
		} else {
			return null;
		}
	}

}
