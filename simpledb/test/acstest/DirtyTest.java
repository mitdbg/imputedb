package acstest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.*;
import java.util.function.Predicate;

import org.junit.*;

import Zql.ParseException;
import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;
import simpledb.*;

public class DirtyTest {
	private static class SmudgePredicate extends Operator {
		private static final long serialVersionUID = -5585085012061351883L;
		private final DbIterator child;
		private final int dirtyIndex;
		private final double dirtyPerc;
		private final Predicate<Tuple> shouldDirty;
		
		private final Random rand = new Random(0);
		
		private static final Field missing = new IntField();
		
		public SmudgePredicate(DbIterator child, int dirtyIndex, double dirtyPerc, Predicate<Tuple> shouldDirty) {
			this.child = child;
			this.dirtyIndex = dirtyIndex;
			this.dirtyPerc = dirtyPerc;
			this.shouldDirty = shouldDirty;
		}
		
		public SmudgePredicate(DbIterator child, QuantifiedName dirtyName, double dirtyPerc, Predicate<Tuple> shouldDirty) {
			this(child, child.getTupleDesc().fieldNameToIndex(dirtyName), dirtyPerc, shouldDirty);
		}
		
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			child.rewind();
		}
		
		@Override
	    public void open() throws DbException, NoSuchElementException,
	            TransactionAbortedException {
	    	super.open();
	        child.open();
	    }
		
		@Override
	    public void close() {
	    	super.close();
	    	child.close();
	    }

		@Override
		protected Tuple fetchNext() throws DbException, TransactionAbortedException {
			while (child.hasNext()) {
	        	Tuple t = child.next();
	        	if (rand.nextDouble() < dirtyPerc) {
	        		if (shouldDirty.test(t)) {
	        			Tuple tt = new Tuple(t);
	        			tt.setField(dirtyIndex, missing);
	        			return tt;
	        		}
	        	} else {
	        		return t;
	        	}
	        }
	        return null;
		}

		@Override
		public DbIterator[] getChildren() {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public void setChildren(DbIterator[] children) {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public TupleDesc getTupleDesc() {
			return child.getTupleDesc();
		}	
	}
	
	private static class SmudgeRandom extends SmudgePredicate {
		private static final long serialVersionUID = -7180182613916428629L;

		public SmudgeRandom(DbIterator child, QuantifiedName dirtyName, double d) {
			super(child, dirtyName, d, x -> true);
		}
	}
	
	private static class Limit extends Operator {
		private final DbIterator child;
		private final int limit;
		private int count = 0;
		
		public Limit(DbIterator child, int limit) {
			this.child = child;
			this.limit = limit;
		}
		
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			child.rewind();
			count = 0;
		}
		
		@Override
	    public void open() throws DbException, NoSuchElementException,
	            TransactionAbortedException {
	    	super.open();
	        child.open();
	    }
		
		@Override
	    public void close() {
	    	super.close();
	    	child.close();
	    }

		@Override
		protected Tuple fetchNext() throws DbException, TransactionAbortedException {
			if (count >= limit || !child.hasNext()) {
				return null;
			}
			count++;
			return child.next();
		}

		@Override
		public DbIterator[] getChildren() {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public void setChildren(DbIterator[] children) {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public TupleDesc getTupleDesc() {
			return child.getTupleDesc();
		}
	}
	
	private double error(DbIterator expected, DbIterator actual) throws DbException, TransactionAbortedException {
		double err = 0.0;
		while(expected.hasNext()) {
			try {
				if (!actual.hasNext()) {
					throw new RuntimeException("BUG: Iterators have different lengths.");
				}
				err += expected.next().error(actual.next());
			} catch (NoSuchElementException e) {
				throw new RuntimeException("BUG: No more tuples.");
			}
		}
		return err;
	}
	
	@Rule public AcsTestRule testDb = new AcsTestRule();
	
	private static DbIterator planQuery(String query, Function<Void, LogicalPlan> planFactory) throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
		ZqlParser p = new ZqlParser(new ByteArrayInputStream(query.getBytes("UTF-8")));
		ZStatement s = p.readStatement();
		Parser pp = new Parser(planFactory);
		return pp.handleQueryStatement((ZQuery)s, new TransactionId()).getPhysicalPlan();
	}
	
	@Test 
	public void BldTest() throws DbException, TransactionAbortedException, IOException, ParseException, ParsingException {
		final Function<String, String> query = 
				tbl -> String.format("SELECT BLD as units_in_structure, AVG(BDSP) as estimate FROM %s GROUP BY BLD;", tbl);
		
		System.err.println("Creating new table with dirty data...");
		TransactionId tid = new TransactionId();
		Database.getCatalog().addTable("acsd", "", AcsTestRule.schema);
		int oldTblId = Database.getCatalog().getTableId("acs"),
				newTblId = Database.getCatalog().getTableId("acsd");
		new Insert(tid, new SmudgeRandom(new Limit(new SeqScan(tid, oldTblId), 100), new QuantifiedName("acs", "BDSP"), 0.1), newTblId).next();
		TableStats.computeStatistics();
		System.err.println("Done.");
		
		System.err.println("Planning queries...");
		DbIterator clean = planQuery(query.apply("acs"), x -> new LogicalPlan());
		DbIterator imputedDirty = planQuery(query.apply("acsd"), x -> new ImputedLogicalPlan(0.5)); 
		DbIterator dirty = planQuery(query.apply("acs"), x -> new LogicalPlan());
		System.err.println("Done.");
		
		clean.open();
		imputedDirty.open();
		dirty.open();
		double imputeErr = error(clean, imputedDirty);
		clean.rewind();
		double baseErr = error(clean, dirty);
		Assert.assertTrue("Base error should be higher than imputed error.", baseErr >= imputeErr);
	}
}
