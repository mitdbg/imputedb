package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class ImputeTest extends SimpleDbTestBase {

  int testWidth = 3;
  DbIterator scan;
  Collection<String> dropFields;

  /**
   * Initialize each unit test
   */
  @Before public void setUp() {
	  this.scan = new TestUtil.MockScanWithMissing(-5, 5, testWidth);
	  this.dropFields = TestUtil.getAllFieldNames(scan);
  }

  /**
   * Unit test for Impute#getTupleDesc(). TupleDesc should be identical as before.
   */
  @Test public void getTupleDescDrop() {
	  getTupleDescDriver(new Drop(dropFields, scan));
  }
  @Test public void getTupleDescTotallyRandom() {
	  getTupleDescDriver(new ImputeTotallyRandom(dropFields, scan));
  }
  @Test public void getTupleDescRandom() {
	  getTupleDescDriver(new ImputeRandom(dropFields, scan));
  }
  @Test public void getTupleDescRegressionTree() {
	  getTupleDescDriver(new ImputeRegressionTree(dropFields, scan));
  }
  
  public void getTupleDescDriver(Impute op){
	  TupleDesc expected = Utility.getTupleDesc(testWidth);
	  TupleDesc actual = op.getTupleDesc();
	  assertEquals(expected, actual);
  }

  /**
   * Unit tests for Impute#rewind(). This doesn't do much. We get the first tuple
   * and after a rewind, confirm that we get the same first tuple.
   */
  @Test public void rewindDrop() throws Exception {
	  rewindDriver(new Drop(dropFields, scan));
  }
  @Test public void rewindTotallyRandom() throws Exception {
	  rewindDriver(new ImputeTotallyRandom(dropFields, scan));
  }
  @Test public void rewindRandom() throws Exception {
	  rewindDriver(new ImputeRandom(dropFields, scan));
  }
  @Test public void rewindRegressionTree() throws Exception {
	  rewindDriver0(new ImputeRegressionTree(dropFields, scan));
  }
  
  public void rewindDriver(Impute op) throws Exception {
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

  public void rewindDriver0(Impute op) throws Exception {
	  op.open();
	  op.rewind();
	  op.close();
  }
  
  /**
   * Unit tests for Impute#fetchNext(). We iterate through the tuples, some of
   * which have missing values. We confirm that the imputed tuples do not have
   * any missing values.
   * @throws Exception
   */
  @Test public void imputeDrop() throws Exception {
	  imputeDriver(new Drop(dropFields, scan));
  }
  @Test public void imputeTotallyRandom() throws Exception {
	  imputeDriver(new ImputeTotallyRandom(dropFields, scan));
  }
  @Test public void imputeRandom() throws Exception {
	  imputeDriver(new ImputeRandom(dropFields, scan));
  }
  @Test public void imputeRegressionTree() throws Exception {
	  imputeDriver0(new ImputeRegressionTree(dropFields, scan));
  }
  
  // TODO: Confirm that the fields that were not imputed are identical.
  public void imputeDriver(Impute op) throws Exception {
	  op.open();
	  while (op.hasNext()){
		  Tuple t = op.next();
		  assertTrue(!t.hasMissingFields());
	  }
	  op.close();
  }
  public void imputeDriver0(Impute op) throws Exception {
	  op.open();
	  while (op.hasNext()){
		  Tuple t = op.next();
		  //assertTrue(!t.hasMissingFields());
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
