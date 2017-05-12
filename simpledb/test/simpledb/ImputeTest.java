package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.MockScanWithMissing;
import simpledb.systemtest.SimpleDbTestBase;

public class ImputeTest extends SimpleDbTestBase {

    int testWidth = 3;
    int dropFieldsInterval = 2;
    int testWidth3 = 20;
    int dropFields3Interval = 7;
    int missingInterval = 30;

    DbIterator scan1;
    DbIterator scan1copy;
    DbIterator scan2;
    DbIterator scan2copy;
    DbIterator scan3;
    DbIterator scan3copy;
    Collection<String> dropFields1;
    Collection<String> dropFields2;
    Collection<String> dropFields3;
  
    /**
     * Initialize each unit test
     */
    @Before public void setUp() {
        // Case 1.
        // 11 rows, 3 fields. Fields 1+4i for i>=0 are complete. All other
        // fields have one missing value. We impute values in all columns.
        this.scan1 = new TestUtil.MockScanWithMissing(-5, 5, testWidth);
        this.scan1copy = new TestUtil.MockScanWithMissing(-5, 5, testWidth);
        this.dropFields1 = TestUtil.getAllFieldNames(scan1);
        
        // Case 2.
        // Same as Case 1, but we impute a subset (half) of the columns.
        this.scan2 = new TestUtil.MockScanWithMissing(-5, 5, testWidth);
        this.scan2copy = new TestUtil.MockScanWithMissing(-5, 5, testWidth);
        this.dropFields2 = TestUtil.getAllFieldNames(scan2);
        Iterator<String> it = dropFields2.iterator(); int i=0;
        while(it.hasNext()){
            it.next();
            if (i++ % dropFieldsInterval == 0){
                it.remove();
            }
        }
        
        // Case 3.
        // Similar to Case 2, but our scan is much larger and our subset of columns is smaller.
        this.scan3 = new TestUtil.MockScanWithMissing(-5000, 5000, testWidth3, missingInterval);
        this.scan3copy = new TestUtil.MockScanWithMissing(-5000, 5000, testWidth3, missingInterval);
        this.dropFields3 = TestUtil.getAllFieldNames(scan3);
        Iterator<String> it3 = dropFields3.iterator(); int i3=0;
        while(it3.hasNext()){
            it3.next();
            if (i3++ % dropFields3Interval == 0){
                it3.remove();
            }
        }
    }
  
    /**
     * Unit test for Impute#getTupleDesc(). TupleDesc should be identical as before.
     */
    @Test public void getTupleDescDrop() {
        getTupleDescDriver(new Drop(dropFields1, scan1));
    }
    @Test public void getTupleDescTotallyRandom() {
        getTupleDescDriver(new ImputeTotallyRandom(dropFields1, scan1));
    }
    @Test public void getTupleDescRandom() {
        getTupleDescDriver(new ImputeHotDeck(dropFields1, scan1));
    }
    @Test public void getTupleDescRegressionTree() {
        getTupleDescDriver(new ImputeRegressionTree(dropFields1, scan1));
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
        rewindDriver(new Drop(dropFields1, scan1));
    }
    @Test public void rewindTotallyRandom() throws Exception {
        rewindDriver(new ImputeTotallyRandom(dropFields1, scan1));
    }
    @Test public void rewindRandom() throws Exception {
        rewindDriver(new ImputeHotDeck(dropFields1, scan1));
    }
    @Test public void rewindRegressionTree() throws Exception {
        rewindDriver0(new ImputeRegressionTree(dropFields1, scan1));
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
        System.out.println("Drop test");
        doImpute1(new Drop(dropFields1, scan1));
    }
    @Test public void imputeTotallyRandom() throws Exception {
        System.out.println("Totally random test");
        doImpute1(new ImputeTotallyRandom(dropFields1, scan1));
        doImpute2(new ImputeTotallyRandom(dropFields2, scan2), scan2copy);
    }
    @Test public void imputeRandom() throws Exception {
        System.out.println("Random test");
        doImpute1(new ImputeHotDeck(dropFields1, scan1));
        doImpute2(new ImputeHotDeck(dropFields2, scan2), scan2copy);
    }
    @Test public void imputeRegressionTree() throws Exception {
        System.out.println("Regression tree test");
        long start1 = System.currentTimeMillis();
        doImpute2(new ImputeRegressionTree(dropFields1, scan1), scan1copy);
        long start2 = System.currentTimeMillis();
        doImpute2(new ImputeRegressionTree(dropFields2, scan2), scan2copy);
        long start3 = System.currentTimeMillis();
        doImpute2(new ImputeRegressionTree(dropFields3, scan3), scan3copy);
        long end = System.currentTimeMillis();
        
        System.out.println("\tTest 1: " + (start2-start1) + " (ms)");
        System.out.println("\tTest 2: " + (start3-start1) + " (ms)");
        System.out.println("\tTest 3: " + (end-start1) + " (ms)");
    }
    
    // Test that we can iterate through the tuples returned by the Impute
    // operator, and that for each tuple, none of the columns identified by
    // dropFields have missing values.
    public void doImpute1(Impute op) throws Exception {
        Collection<String> dropFields = op.dropFields;

        op.open();

        while (op.hasNext()){
            Tuple imputed = op.next();

            // Confirm that no columns in dropFields still have missing values.
            assertTrue(!imputed.hasMissingFields(dropFields));
        }

        op.close();
    }
    
    // Same as doImpute1, but also confirm that the fields that were not imputed
    // are identical to original scan. (This doesn't work for Drop because that
    // will eliminate tuples entirely, mis-aligning scans.)
    public void doImpute2(Impute op, DbIterator scan) throws Exception{
        Collection<String> dropFields = op.dropFields;
        Collection<Integer> dropFieldsIndices = Impute.extractDropFieldsIndices(dropFields, op.getTupleDesc());
        
        op.open();
        scan.open();

        while (op.hasNext()){
            if (!scan.hasNext())
                throw new RuntimeException("Length of DbIterators do not match.");

            Tuple imputed = op.next();
            Tuple original = scan.next();

            // Confirm that no columns in dropFields still have missing values.
            assertTrue(!imputed.hasMissingFields(dropFields));
            
            // Confirm that any value that was not missing originally is the same as before.
            int n = imputed.getTupleDesc().numFields();
            for (int i=0; i<n; i++){
                Field fo = original.getField(i);
                if (!fo.isMissing()){
                    Field fi = imputed.getField(i);
                    assertTrue(Field.areEqual(fo, fi));
                }
            }
        }

        op.close();
        scan.close();
    }

  
    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(ImputeTest.class);
    }
}
