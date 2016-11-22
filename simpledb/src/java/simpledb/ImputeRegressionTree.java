package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

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

		Instances train = WekaUtil.relationToInstances("", buffer, getTupleDesc());
		
		// Make the last attribute be the class
		train.setClassIndex(train.numAttributes() - 1);
		
		// Print header and instances.
		System.out.println("\nDataset:\n");
		System.out.println(train);	
		
		Classifier tree = new REPTree();
		try {
			tree.buildClassifier(train);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DbException("Failed to train classifier.");
		}
		
		return null;
	}

}
