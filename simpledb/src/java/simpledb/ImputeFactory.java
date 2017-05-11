package simpledb;

import java.util.Collection;

public class ImputeFactory {
	public static Impute newImpute(String imputationMethod, Collection<String> arg1, DbIterator arg2){
		if (imputationMethod.equals("REGRESSION_TREE")){
			return new ImputeRegressionTree(arg1, arg2);
		} else if (imputationMethod.equals("HOTDECK")){
			return new ImputeHotDeck(arg1, arg2);
		} else if (imputationMethod.equals("MEAN")){
			return new ImputeMean(arg1, arg2);
		} else {
			throw new UnsupportedOperationException();
		}
	}
}
