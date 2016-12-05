package acstest;

import java.io.*;
import java.time.*;
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
	
	private static final int TABLE_SIZE = 5000;
	
	@ClassRule public static final AcsTestRule TEST_DB = new AcsTestRule();
	
	private static DbIterator planQuery(String query, Function<Void, LogicalPlan> planFactory) throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
		ZqlParser p = new ZqlParser(new ByteArrayInputStream(query.getBytes("UTF-8")));
		ZStatement s = p.readStatement();
		Parser pp = new Parser(planFactory);
		return pp.handleQueryStatement((ZQuery)s, new TransactionId()).getPhysicalPlan();
	}
	
	private static void drain(DbIterator iter) throws DbException, TransactionAbortedException {
		while (iter.hasNext()) {
			try {
				iter.next();
			} catch(NoSuchElementException e) {
				e.printStackTrace();
			}
		}
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
	
	private final static Function<String, String> QUERY = 
			tbl -> String.format("SELECT BLD as units_in_structure, AVG(BDSP) as estimate FROM %s GROUP BY BLD;", tbl);
	
	@Test 
	public void ErrorTest1() throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
		System.err.println("Planning queries...");
		DbIterator clean = planQuery(QUERY.apply("acs_small_clean"), x -> new LogicalPlan());
		DbIterator imputedDirty = planQuery(QUERY.apply("acs_small_dirty"), x -> new ImputedLogicalPlan(0.0)); 
		DbIterator dirty = planQuery(QUERY.apply("acs_small_dirty"), x -> new LogicalPlan());
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
	
	@Test
	public void TimeImputation() throws NoSuchElementException, DbException, TransactionAbortedException, IOException, ParseException, ParsingException {
		final double step = 0.1;
		final Duration[] times = new Duration[(int)(1.0 / step) + 1];
		Instant start, end;
		
		System.err.println("Planning queries...");
		
		start = Instant.now();
		DbIterator imputedDirty = planQuery(QUERY.apply("acs_small_dirty"), x -> new ImputedLogicalPlan(0.0));
		imputedDirty.open();
		drain(imputedDirty);
		end = Instant.now();
		times[0] = Duration.between(start, end);
		
		int t = 1;
		for (double a = step; a <= 1.0; a += step) {
			final double aa = a;
			imputedDirty = planQuery(QUERY.apply("acs_small_dirty"), x -> {
				return new ImputedLogicalPlan(aa);
			});
			
			start = Instant.now();
			imputedDirty.open();
			drain(imputedDirty);
			imputedDirty.close();
			end = Instant.now();
			times[t] = Duration.between(start, end);
			
			t++;
		}
		
		Assert.assertTrue("Imputation should take longer than dropping.",
				times[0].compareTo(times[times.length -1]) < 0);
	}
}
