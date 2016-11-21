package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.trees.J48;

public class ImputeRegressionTree extends Impute {

	private static final long serialVersionUID = 1L;
	private static final long GENERATOR_SEED = 6832L;
	private Random random;
	private ArrayList<Tuple> buffer;

	public ImputeRegressionTree(Collection<String> dropFields, DbIterator child) {
		super(dropFields, child);
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
		
	}

	@Override
	protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		Instances train = null;
		Classifier cls = new J48();
		try {
			cls.buildClassifier(train);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Evaluation eval;

		return null;
		
	}

}
