package simpledb;

import java.util.Collection;

public class ImputeRegressionTree extends Impute {

	private static final long serialVersionUID = 1L;

	public ImputeRegressionTree(Collection<String> dropFields, DbIterator child) {
		super(dropFields, child);
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return null;
	}

}
