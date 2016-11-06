package simpledb;

public class ImputeRandom extends Impute {

	private static final long serialVersionUID = 1L;

    public ImputeRandom(DbIterator child){
    	super(child);
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
