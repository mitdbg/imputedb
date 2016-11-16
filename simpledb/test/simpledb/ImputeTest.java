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
   * Unit test for Impute.getTupleDesc(). TupleDesc should be identical as before.
   */
  @Test public void getTupleDesc() {
	  Impute op = new ImputeTotallyRandom(scan);
	  TupleDesc expected = Utility.getTupleDesc(testWidth);
	  TupleDesc actual = op.getTupleDesc();
	  assertEquals(expected, actual);
  }

  /**
   * Unit tests for Impute.rewind(). This doesn't do much. We get the first tuple
   * and after a rewind, confirm that we get the same first tuple.
   */
  @Test public void rewindTotallyRandom() throws Exception {
	  Impute op = new ImputeTotallyRandom(scan);
	  op.open();
	  assertTrue(op.hasNext());
	  Tuple expected = op.next();
	  assertNotNull(expected);
	  
	  op.rewind();
	  assertTrue(op.hasNext());
	  Tuple actual = op.next();
	  assertTrue(TestUtil.compareTuples(expected, actual));
	  op.close();
  }

  @Test public void rewindRandom() throws Exception {
	  Impute op = new ImputeRandom(scan);
	  op.open();
	  assertTrue(op.hasNext());
	  Tuple expected = op.next();
	  assertNotNull(expected);
	  
	  op.rewind();
	  assertTrue(op.hasNext());
	  Tuple actual = op.next();
	  assertTrue(TestUtil.compareTuples(expected, actual));
	  op.close();
  }
  
  @Test public void imputeTotallyRandom() throws Exception {
	  Impute op = new ImputeTotallyRandom(scan);
	  op.open();
	  while (op.hasNext()){
		  Tuple t = op.next();
		  assertTrue(!t.hasMissingFields());
	  }
	  op.close();
  }

  @Test public void imputeRandom() throws Exception {
	  Impute op = new ImputeRandom(scan);
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
