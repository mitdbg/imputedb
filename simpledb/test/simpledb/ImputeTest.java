package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class ImputeTest extends SimpleDbTestBase {

  int testWidth = 3;
  DbIterator scan;

  /**
   * Initialize each unit test
   */
  @Before public void setUp() {
	  this.scan = new TestUtil.MockScanWithMissing(-5, 5, testWidth);
  }

  /**
   * Unit test for Impute.getTupleDesc()
   */
  @Test public void getTupleDesc() {
  }

  /**
   * Unit test for Impute.rewind()
   */
  @Test public void rewind() throws Exception {
  }
  
  @Test public void imputeTotallyRandom() throws Exception {
	  Impute op= new ImputeTotallyRandom(scan);
	  op.open();
	  while (op.hasNext()){
		  Tuple t = op.next();
		  assertTrue(!t.hasMissingFields());
	  }
	  op.close();
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ImputeTest.class);
  }
}
