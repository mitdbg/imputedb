package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class TableStatsTest extends SimpleDbTestBase {
	public static final int IO_COST = 71;
	
	ArrayList<ArrayList<Integer>> tuples;
	HeapFile f;
	String tableName;
	int tableId;

	ArrayList<ArrayList<Integer>> tuples2;
	HeapFile f2;
	String tableName2;
	int tableId2;
	
	@Before public void setUp() throws Exception {
		super.setUp();
		this.tuples = new ArrayList<ArrayList<Integer>>();
		this.f = SystemTestUtil.createRandomHeapFile(10, 10200, 32, null, tuples);
		
		this.tableName = SystemTestUtil.getUUID();
		Database.getCatalog().addTable(f, tableName);
		this.tableId = Database.getCatalog().getTableId(tableName);

		// create a second table
		this.tuples2 = new ArrayList<ArrayList<Integer>>();
		this.f2 = SystemTestUtil.createRandomHeapFile(10, 10200, 32, null, tuples2);
		this.tableName2 = SystemTestUtil.getUUID();
		Database.getCatalog().addTable(f2, tableName2);
		this.tableId2 = Database.getCatalog().getTableId(tableName2);
	}
	
	private double[] getRandomTableScanCosts(int[] pageNums, int[] ioCosts) throws IOException, DbException, TransactionAbortedException {
		double[] ret = new double[ioCosts.length];
		for(int i = 0; i < ioCosts.length; ++i) {
			HeapFile hf = SystemTestUtil.createRandomHeapFile(1, 992*pageNums[i], 32, null, tuples);
			Assert.assertEquals(pageNums[i], hf.numPages);			
			String tableName = SystemTestUtil.getUUID();
			Database.getCatalog().addTable(hf, tableName);
			int tableId = Database.getCatalog().getTableId(tableName);
			ret[i] = (new TableStats(tableId, ioCosts[i])).estimateScanCost();
		}
		return ret;
	}
	/**
	 * Verify the cost estimates of scanning various numbers of pages from a HeapFile
	 * This test checks that the estimateScanCost is: 
	 *   +linear in numPages when IO_COST is constant
	 *   +linear in IO_COST when numPages is constant
	 *   +quadratic when IO_COST and numPages increase linearly.
	 */
	@Test public void estimateScanCostTest() throws IOException, DbException, TransactionAbortedException {
		Object[] ret;
		int[] ioCosts = new int[20];
		int[] pageNums = new int[ioCosts.length];
		// IO_COST constant, numPages change
		for(int i = 0; i < ioCosts.length; ++i) {
			ioCosts[i] = 1;
			pageNums[i] = 3*(i+1);
		}
		double stats[] = getRandomTableScanCosts(pageNums, ioCosts);
		ret = SystemTestUtil.checkConstant(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals(ret[0], Boolean.TRUE);
		// numPages constant, IO_COST change
		for(int i = 0; i < ioCosts.length; ++i) {
			ioCosts[i] = 10*(i + 1);
			pageNums[i] = 3;
		}
		stats = getRandomTableScanCosts(pageNums, ioCosts);
		ret = SystemTestUtil.checkConstant(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals(ret[0], Boolean.TRUE);
		//numPages & IO_COST increase linearly
		for(int i = 0; i < ioCosts.length; ++i) {
			ioCosts[i] = 3*(i + 1);
			pageNums[i] = (i+1);
		}
		stats = getRandomTableScanCosts(pageNums, ioCosts);
		ret = SystemTestUtil.checkConstant(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkQuadratic(stats);
		Assert.assertEquals(ret[0], Boolean.TRUE);
		
	}
	
	/**
	 * Verify the table-cardinality estimates based on a selectivity estimate
	 */
	@Test public void estimateTableCardinalityTest() {
		TableStats s = new TableStats(this.tableId, IO_COST);
		
		// Try a random selectivity
		Assert.assertEquals(3060, s.estimateTableCardinality(0.3));
		
		// Make sure we get all rows with 100% selectivity, and none with 0%
		Assert.assertEquals(10200, s.estimateTableCardinality(1.0));
		Assert.assertEquals(0, s.estimateTableCardinality(0.0));
	}
	
	/**
	 * Verify that selectivity estimates do something reasonable.
	 * Don't bother splitting this into N different functions for
	 * each possible Op because we will probably catch any bugs here in
	 * IntHistogramTest, so we hopefully don't need all the JUnit checkboxes.
	 */
	@Test public void estimateSelectivityTest() {
		final int maxCellVal = 32;	// Tuple values are randomized between 0 and this number
		
		final Field aboveMax = new IntField(maxCellVal + 10);
		final Field atMax = new IntField(maxCellVal);
		final Field halfMaxMin = new IntField(maxCellVal/2);
		final Field atMin = new IntField(0);
		final Field belowMin = new IntField(-10);
		
		TableStats s = new TableStats(this.tableId, IO_COST);
		
		for (int col = 0; col < 10; col++) {
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.EQUALS, aboveMax), 0.001);			
			Assert.assertEquals(1.0/32.0, s.estimateSelectivity(col, Predicate.Op.EQUALS, halfMaxMin), 0.015);
			Assert.assertEquals(0, s.estimateSelectivity(col, Predicate.Op.EQUALS, belowMin), 0.001);

			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.NOT_EQUALS, aboveMax), 0.001);
			Assert.assertEquals(31.0/32.0, s.estimateSelectivity(col, Predicate.Op.NOT_EQUALS, halfMaxMin), 0.015);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.NOT_EQUALS, belowMin), 0.015);

			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, aboveMax), 0.001);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, atMax), 0.001);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, halfMaxMin), 0.1);
			Assert.assertEquals(31.0/32.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, atMin), 0.05);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, belowMin), 0.001);
			
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, aboveMax), 0.001);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, atMax), 0.015);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, halfMaxMin), 0.1);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, atMin), 0.001);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, belowMin), 0.001);
			
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, aboveMax), 0.001);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, atMax), 0.015);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, halfMaxMin), 0.1);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, atMin), 0.015);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, belowMin), 0.001);
			
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, aboveMax), 0.001);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, atMax), 0.015);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, halfMaxMin), 0.1);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, atMin), 0.05);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, belowMin), 0.001);
		}
	}


	/* Table Stats tests for imputation */
	public TableStats statsWithNulls(int tableId, int ctNullsPerCol) {
		TableStats s = new TableStats(tableId, IO_COST);
		TupleDesc schema = Database.getCatalog().getTupleDesc(tableId);
		int[] nulls = new int[schema.numFields()];
		for(int i = 0; i < nulls.length; i++) {
			nulls[i] = ctNullsPerCol;
		}

		TableStats withNulls = s.setNullStats(nulls);
		assert(withNulls.totalTuples() == s.totalTuples() + ctNullsPerCol);
		return withNulls;
	}

	public List<Integer> getAllIndices(int tableId) {
		TupleDesc schema = Database.getCatalog().getTupleDesc(tableId);
		List<Integer> indices = new ArrayList<>();
		for(int i = 0 ; i < schema.numFields(); i++) {
			indices.add(i);
		}
		return indices;
	}


	public void checkDistributionEquivalence(TableStats dist1, TableStats dist2, int col1, int col2, int len) {
		final int maxCellVal = 32;	// Tuple values are randomized between 0 and this number

		final Field aboveMax = new IntField(maxCellVal + 10);
		final Field atMax = new IntField(maxCellVal);
		final Field halfMaxMin = new IntField(maxCellVal/2);
		final Field atMin = new IntField(0);
		final Field belowMin = new IntField(-10);

		for ( ; len >= 1; col1++, col2++, len--) {
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.EQUALS, aboveMax), dist2.estimateSelectivity(col2, Predicate.Op.EQUALS, aboveMax), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.EQUALS, halfMaxMin), dist2.estimateSelectivity(col2, Predicate.Op.EQUALS, halfMaxMin), 0.015);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.EQUALS, belowMin), dist2.estimateSelectivity(col2, Predicate.Op.EQUALS, belowMin), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.NOT_EQUALS, aboveMax), dist2.estimateSelectivity(col2, Predicate.Op.NOT_EQUALS, aboveMax), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.NOT_EQUALS, halfMaxMin), dist2.estimateSelectivity(col2, Predicate.Op.NOT_EQUALS, halfMaxMin), 0.015);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.NOT_EQUALS, belowMin), dist2.estimateSelectivity(col2, Predicate.Op.NOT_EQUALS, belowMin), 0.015);

			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN, aboveMax), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN, aboveMax), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN, atMax), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN, atMax), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN, halfMaxMin), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN, halfMaxMin), 0.1);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN, atMin), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN, atMin), 0.05);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN, belowMin), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN, belowMin), 0.001);

			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN, aboveMax), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN, aboveMax), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN, atMax), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN, atMax), 0.015);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN, halfMaxMin), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN, halfMaxMin), 0.1);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN, atMin), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN, atMin), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN, belowMin), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN, belowMin), 0.001);

			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN_OR_EQ, aboveMax), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN_OR_EQ, aboveMax), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN_OR_EQ, atMax), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN_OR_EQ, atMax), 0.015);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN_OR_EQ, halfMaxMin), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN_OR_EQ, halfMaxMin), 0.1);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN_OR_EQ, atMin), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN_OR_EQ, atMin), 0.015);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.GREATER_THAN_OR_EQ, belowMin), dist2.estimateSelectivity(col2, Predicate.Op.GREATER_THAN_OR_EQ, belowMin), 0.001);

			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN_OR_EQ, aboveMax), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN_OR_EQ, aboveMax), 0.001);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN_OR_EQ, atMax), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN_OR_EQ, atMax), 0.015);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN_OR_EQ, halfMaxMin), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN_OR_EQ, halfMaxMin), 0.1);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN_OR_EQ, atMin), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN_OR_EQ, atMin), 0.05);
			Assert.assertEquals(dist1.estimateSelectivity(col1, Predicate.Op.LESS_THAN_OR_EQ, belowMin), dist2.estimateSelectivity(col2, Predicate.Op.LESS_THAN_OR_EQ, belowMin), 0.001);
		}
	}


	// checks invariant that imputations etc result in same distribution
	@Test public void adjustForImputeTest() {
		int ctNullPerCol = 100;
		TableStats withNulls = statsWithNulls(this.tableId, ctNullPerCol);
		// original distribution without nulls
		TableStats orig = new TableStats(this.tableId, IO_COST);

		// impute all columns
		List<Integer> imputeIndices = getAllIndices(this.tableId);

		// dropping missing values
		TableStats drop = withNulls.adjustForImpute(ImputationType.DROP, imputeIndices);
		Assert.assertEquals(withNulls.totalTuples() - ctNullPerCol, drop.totalTuples(), 1);
		checkDistributionEquivalence(orig, drop, 0, 0, 10);

		// minimal imputation
		TableStats min = withNulls.adjustForImpute(ImputationType.MINIMAL, imputeIndices);
		Assert.assertEquals(withNulls.totalTuples(), min.totalTuples(), 1);
		checkDistributionEquivalence(orig, min, 0, 0, 10);
		// maximal imputation
		TableStats max = withNulls.adjustForImpute(ImputationType.MAXIMAL, imputeIndices);
		Assert.assertEquals(withNulls.totalTuples(), max.totalTuples(), 1);
		checkDistributionEquivalence(orig, max, 0, 0, 10);
	}


	@Test public void adjustForSelectivityTest() {
		int ctNullPerCol = 100;
		TableStats withNulls = statsWithNulls(this.tableId, ctNullPerCol);
		TableStats orig = new TableStats(this.tableId, IO_COST);

		double selectivity = 0.5;
		// adjusted without nulls
		TableStats adjNoNulls = orig.adjustForSelectivity(selectivity);
		Assert.assertEquals(orig.totalTuples() * selectivity, adjNoNulls.totalTuples(), 1);
		checkDistributionEquivalence(orig, adjNoNulls, 0, 0, 10);

		// adjusted with nulls
		TableStats adjNulls = withNulls.adjustForSelectivity(selectivity);
		// check nulls
		List<Integer> ix0 = Collections.singletonList(0);
		double expectedNulls = withNulls.estimateTotalNull(ix0) * selectivity;
		Assert.assertEquals(expectedNulls, adjNulls.estimateTotalNull(ix0), 0.01);
		// check total count
		Assert.assertEquals(withNulls.totalTuples() * selectivity, adjNulls.totalTuples(), 1);
		checkDistributionEquivalence(orig, adjNulls, 0, 0, 10);
	}

	@Test public void adjustToTotalTest() {
		int ctNullPerCol = 100;
		TableStats withNulls = statsWithNulls(this.tableId, ctNullPerCol);
		TableStats orig = new TableStats(this.tableId, IO_COST);
		int scaledPopulation = 1000000;

		// adjusted without nulls
		TableStats adjNoNulls = orig.adjustToTotal(scaledPopulation);
		Assert.assertEquals(scaledPopulation, adjNoNulls.totalTuples(), 1);
		checkDistributionEquivalence(orig, adjNoNulls, 0, 0, 10);

		// adjusted with nulls
		TableStats adjNulls = withNulls.adjustToTotal(scaledPopulation);
		Assert.assertEquals(adjNulls.totalTuples(), scaledPopulation, 1);
		// check nulls
		List<Integer> ix0 = Collections.singletonList(0);
		double expectedNulls = (withNulls.estimateTotalNull(ix0) / withNulls.totalTuples()) * scaledPopulation;
		// check total count
		Assert.assertEquals(adjNulls.estimateTotalNull(Collections.singletonList(1)), expectedNulls, 1);
		checkDistributionEquivalence(orig, adjNulls, 0, 0, 10);

	}

	@Test public void mergeTest() {
		TableStats t1 = new TableStats(this.tableId, IO_COST);
		TableStats t2 = new TableStats(this.tableId2, IO_COST);
		TableStats merged = t1.merge(t2);
		Assert.assertEquals(t1.totalTuples(), merged.totalTuples());
		Assert.assertEquals(t2.totalTuples(), merged.totalTuples());

		// check that first half is the same as t1
		checkDistributionEquivalence(t1, merged, 0, 0, 10);

		// check that second half is the same as t2
		checkDistributionEquivalence(t2, merged, 0, 10, 10);

	}
}
