package acstest;

import java.io.*;
import java.util.*;
import java.util.function.*;

import org.junit.*;
import Zql.*;
import simpledb.*;

public class DirtyTest {
	private static class SmudgeRandom extends SmudgePredicate {
		private static final long serialVersionUID = -7180182613916428629L;

		public SmudgeRandom(DbIterator child, QuantifiedName dirtyName, double d) {
			super(child, dirtyName, d, x -> true);
		}
	}
	
	private static final int TABLE_SIZE = 1000;
	
	@ClassRule public static final AcsTestRule TEST_DB = new AcsTestRule();
	
	private static DbIterator planQuery(String query, Function<Void, LogicalPlan> planFactory) throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
		ZqlParser p = new ZqlParser(new ByteArrayInputStream(query.getBytes("UTF-8")));
		ZStatement s = p.readStatement();
		Parser pp = new Parser(planFactory);
		return pp.handleQueryStatement((ZQuery)s, new TransactionId()).getPhysicalPlan();
	}
	
	@BeforeClass
	public static void Before() throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
		System.err.println("Creating new table with dirty data...");
		Database.getCatalog().addTable("acs_small_clean", "", AcsTestRule.SCHEMA);
		Database.getCatalog().addTable("acs_small_dirty", "", AcsTestRule.SCHEMA);
		int oldTblId = Database.getCatalog().getTableId("acs"),
				newTblIdC = Database.getCatalog().getTableId("acs_small_clean"),
				newTblIdD = Database.getCatalog().getTableId("acs_small_dirty");
		
		TransactionId tid = new TransactionId();
		new Insert(tid, new Limit(new SeqScan(tid, oldTblId), TABLE_SIZE), newTblIdC).next();
		tid = new TransactionId();
		new Insert(tid, new SmudgeRandom(new SeqScan(tid, newTblIdC), new QuantifiedName("acs_small_clean", "BDSP"), 0.1), newTblIdD).next();
		TableStats.computeStatistics();
		System.err.println("Done.");
	}
	
	@Test 
	public void ErrorTest() throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
		final Function<String, String> query = 
				tbl -> String.format("SELECT BLD as units_in_structure, AVG(BDSP) as estimate FROM %s GROUP BY BLD;", tbl);
		
		System.err.println("Planning queries...");
		DbIterator clean = planQuery(query.apply("acs_small_clean"), x -> new LogicalPlan());
		DbIterator imputedDirty = planQuery(query.apply("acs_small_dirty"), x -> new ImputedLogicalPlan(0.0)); 
		DbIterator dirty = planQuery(query.apply("acs_small_dirty"), x -> new LogicalPlan());
		System.err.println("Done."); 
		
		clean.open();
		imputedDirty.open();
		dirty.open();
		
		clean.print();
		System.out.println();
		imputedDirty.print();
		System.out.println();
		dirty.print();
		
		clean.rewind();
		imputedDirty.rewind();
		dirty.rewind();
		
		double baseErr = clean.error(dirty);
		clean.rewind();
		double imputeErr = clean.error(imputedDirty);
		Assert.assertTrue("Base error should be higher than imputed error.", baseErr >= imputeErr);
	}
	
	@Test
	public void SwitchFromDropToImputeTest() throws NoSuchElementException, DbException, TransactionAbortedException, IOException, ParseException, ParsingException {
		final double step = 0.1;
		final Function<String, String> query = 
				tbl -> String.format("SELECT BLD as units_in_structure, AVG(BDSP) as estimate FROM %s GROUP BY BLD;", tbl);
		
		System.err.println("Planning queries...");
		
		DbIterator imputedDirty = planQuery(query.apply("acs_small_dirty"), x -> new ImputedLogicalPlan(0.0));
		imputedDirty.open();
		Assert.assertTrue("Plan does not use drop at lowest quality setting.", imputedDirty.contains(Drop.class));
		
		for (double a = step; a <= 1.0; a += step) {
			final double aa = a;
			imputedDirty = planQuery(query.apply("acs_small_dirty"), x -> {
				return new ImputedLogicalPlan(aa);
			});
			imputedDirty.open();
			if (imputedDirty.contains(ImputeRandom.class) || imputedDirty.contains(ImputeRegressionTree.class)) {
				System.err.format("Switched from drop to impute at alpha %f.\n", aa);
				return;
			}
			imputedDirty.close();
			Assert.assertTrue("Plan contains neither drop nor impute.", imputedDirty.contains(Drop.class));
		}
		Assert.fail("Never switched from drop to impute.");
	}
}
