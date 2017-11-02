package acstest;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.junit.Assert;

import Zql.ParseException;
import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;
import simpledb.BadErrorException;
import simpledb.Database;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.HeapFile;
import simpledb.ImputedLogicalPlan;
import simpledb.Insert;
import simpledb.LogicalPlan;
import simpledb.Parser;
import simpledb.ParsingException;
import simpledb.SeqScan;
import simpledb.TableStats;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.TupleDesc;
import simpledb.Type;

public class RunQueries {
	private static final String OUT_FN = "queries.csv";
	
	private static FileWriter outFile;
	
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
	
	private static void runQuery(String query) throws IOException, ParseException, TransactionAbortedException, DbException, ParsingException {
		String dirtyQuery = query.replaceAll("acs", "acs_dirty");
		
		Instant startPlan, endPlan, startRun, endRun;
		
		System.err.println("Planning queries...");
		DbIterator clean = planQuery(query, x -> new LogicalPlan());
		DbIterator dirty = planQuery(dirtyQuery, x -> new LogicalPlan());
		clean.open();
		
		dirty.open();
		Double baseErr = null;
		try {
			baseErr = clean.error(dirty);
		} catch (BadErrorException e) {}
		dirty.close();
		
		for (double a = 0.0; a <= 1.0; a += 1.0) {
			final double aa = a;
			startPlan = Instant.now();
			DbIterator imputedDirty = planQuery(dirtyQuery, x -> new ImputedLogicalPlan(aa));
			endPlan = Instant.now();
			
			startRun = Instant.now();
			imputedDirty.open();
			drain(imputedDirty);
			endRun = Instant.now();
			
			imputedDirty.rewind();
			clean.rewind();
			Double imputeErr = null;
			try {
				imputeErr = clean.error(imputedDirty);
			} catch (BadErrorException e) {}
			
			outFile.write(String.format("\"%s\",%f,%f,%d,%d,%f\n", query, baseErr, imputeErr,
					Duration.between(startPlan, endPlan).getSeconds(), Duration.between(startRun, endRun).getSeconds(), a));
			outFile.flush();
			imputedDirty.close();
		}
		clean.close();
	}
	
	public static void main(String args[]) throws IOException, NoSuchElementException, DbException, TransactionAbortedException, ParseException, ParsingException {
		(new AcsTestRule()).initializer.before();
		
		System.err.println("Creating new table with dirty data...");
		Database.getCatalog().addTable("acs_dirty", "", AcsTestRule.SCHEMA);
		int oldTblId = Database.getCatalog().getTableId("acs"),
			newTblId = Database.getCatalog().getTableId("acs_dirty");

		System.err.println("Adding synthetic data");
		int nfields = 10;
		Type[] types = new Type[nfields];
		String[] fields = new String[nfields];
		for (int i = 0; i < types.length; i++) {
			types[i] = Type.INT_TYPE;
			fields[i] = "f" + i;
		}
		TupleDesc schema = new TupleDesc(types, fields);
		ClassLoader loader = CleanTest.class.getClassLoader();
		File cleanData = new File(loader.getResource("testdata/clean.dat").getFile());
		Database.getCatalog().addTable(new HeapFile(cleanData, schema), "clean", "");
		File dirtyData = new File(loader.getResource("testdata/dirty.dat").getFile());
		Database.getCatalog().addTable(new HeapFile(dirtyData, schema), "dirty", "");
		
		System.err.println("Inserting dirty acs data...");
		TransactionId tid = new TransactionId();
		new Insert(tid, new Smudge(new SeqScan(tid, oldTblId), 0.1), newTblId).next();
		tid = null;
		
		TableStats.computeStatistics();
		System.err.println("Done.");

		outFile = new FileWriter(OUT_FN); 		
		outFile.write("query,base_err,impute_err,plan_time,run_time,alpha\n");
		
		for (Object[] query : DirtyTest.data()) {
			runQuery((String)query[0]);
		}
		outFile.close();
	}
}
