package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import weka.core.Instance;
import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.REPTree;

public class ImputeRegressionTree extends Impute {

	private static final long serialVersionUID = 1L;
	private static final long GENERATOR_SEED = 6832L;
	private Random random;
	private ArrayList<Tuple> buffer;

	public ImputeRegressionTree(Collection<String> dropFields, DbIterator child) {
		super(dropFields, child);
		initRng();
		buffer = new ArrayList<>();
	}
	
	private void initRng(){
		random = new Random(GENERATOR_SEED);
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
		initRng();
		buffer.clear();
	}

	@Override
	protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		// First, we must add all of the child tuples to the buffer.
		while (child.hasNext()){
			buffer.add(child.next());
		}

		//TODO
		int imputationColumn = td.numFields() - 1;

		Instances train = WekaUtil.relationToInstances("", buffer, td);
		train.setClassIndex(imputationColumn);
		
		// Print header and instances.
		System.out.println("\nDataset:\n");
		System.out.println(train);	
		
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
		
		// Replace all the values in imputationColumn with predicted values.
		ArrayList<Double> predictions = new ArrayList<>();
		for (int i=0; i<train.size(); i++){
			Instance testInstance = train.get(i);
			try {
				double pred = tree.classifyInstance(testInstance);
				predictions.add(pred);
				System.out.println("Predicted value: "+pred);

				//train.get(i).setValue(imputationColumn, pred);
			} catch (Exception e) {
				e.printStackTrace();
				throw new DbException("Failed to classify instance.");
			}
			
		}
		
		
		
		return null;
	}

}
