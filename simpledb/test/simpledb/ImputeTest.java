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

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ImputeTest.class);
  }
}
