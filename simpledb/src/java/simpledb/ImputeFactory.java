package simpledb;

import java.util.Collection;

public class ImputeFactory {
	public static final String DEFAULT_IMPUTATION_METHOD = "REGRESSION_TREE";
	public static String currentImputationMethod = DEFAULT_IMPUTATION_METHOD;
	
	public static void setImputationMethod(String imputationMethod){
		ImputeFactory.currentImputationMethod = imputationMethod;
	}
	
	public static Impute newImpute(Collection<String> arg1, DbIterator arg2){
		return newImpute(currentImputationMethod, arg1, arg2);
	}
	
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
