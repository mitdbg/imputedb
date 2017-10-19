package simpledb.systemtest;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Ignore;
import org.junit.Test;

import simpledb.*;

public class QueryTest {
	
	/**
	 * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create an identical HeapFile table
	 * @param tuples Tuples to create a HeapFile from
	 * @param columns Each entry in tuples[] must have "columns == tuples.get(i).size()"
	 * @param colPrefix String to prefix to the column names (the columns are named after their column number by default)
	 * @return a new HeapFile containing the specified tuples
	 * @throws IOException if a temporary file can't be created to hand to HeapFile to open and read its data
	 */
	public static HeapFile createDuplicateHeapFile(ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix) throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(temp));
        HeapFileEncoder.convert(tuples, out, BufferPool.getPageSize(), columns, false);
        return Utility.openHeapFile(columns, colPrefix, temp);
	}
	
	@Test(timeout=20000) public void queryTest() throws IOException, DbException, TransactionAbortedException {
		// This test is intended to approximate the join described in the
		// "Query Planning" section of 2009 Quiz 1,
		// though with some minor variation due to limitations in simpledb
		// and to only test your integer-heuristic code rather than
		// string-heuristic code.		
		final int IO_COST = 101;
		
//		HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
		
		// Create all of the tables, and add them to the catalog
		ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null, empTuples, "c");	
		Database.getCatalog().addTable(emp, "emp");
		
		ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null, deptTuples, "c");	
		Database.getCatalog().addTable(dept, "dept");
		
		ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null, hobbyTuples, "c");
		Database.getCatalog().addTable(hobby, "hobby");
		
		ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null, hobbiesTuples, "c");
		Database.getCatalog().addTable(hobbies, "hobbies");
		
		// Get TableStats objects for each of the tables that we just generated.
		TableStats.setTableStats("emp", new TableStats(Database.getCatalog().getTableId("emp"), IO_COST));
		TableStats.setTableStats("dept", new TableStats(Database.getCatalog().getTableId("dept"), IO_COST));
		TableStats.setTableStats("hobby", new TableStats(Database.getCatalog().getTableId("hobby"), IO_COST));
		TableStats.setTableStats("hobbies", new TableStats(Database.getCatalog().getTableId("hobbies"), IO_COST));

//		Parser.setStatsMap(stats);
		
		Transaction t = new Transaction();
		t.start();
		Parser p = new Parser();
		p.setTransaction(t);
		
		// Each of these should return around 20,000
		// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
		// So, don't bother for now; future TODO.
		// Regardless, each of the following should be optimized to run quickly,
		// even though the worst case takes a very long time.
		PrintTupleFormatter f = new PrintTupleFormatter(System.out);
		p.processNextStatement("SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND emp.c3 < 1000;", f);
	}
	
	/**
	 * Build a large series of tables; then run the command-line query code and execute a query.
	 * The number of tables is large enough that the query will only succeed within the
	 * specified time if a join method faster than nested-loops join is available.
	 * The tables are also too big for a query to be successful if its query plan isn't reasonably efficient,
	 * and there are too many tables for a brute-force search of all possible query plans.
	 */
	// Not required for Lab 4
	@Ignore
	@Test(timeout=600000) public void largeJoinTest() throws IOException, DbException, TransactionAbortedException {
		final int IO_COST = 103;
						
		ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100, Integer.MAX_VALUE, null, smallHeapFileTuples, "c");		
		HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileO = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileP = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileQ = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileR = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileS = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileT = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		
		ArrayList<ArrayList<Integer>> bigHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < 1000; i++) {
			bigHeapFileTuples.add( smallHeapFileTuples.get( i%100 ) );
		}
		HeapFile bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2, "c");
		Database.getCatalog().addTable(bigHeapFile, "bigTable");

		// We want a bunch of these guys
		Database.getCatalog().addTable(smallHeapFileA, "a");
		Database.getCatalog().addTable(smallHeapFileB, "b");
		Database.getCatalog().addTable(smallHeapFileC, "c");
		Database.getCatalog().addTable(smallHeapFileD, "d");
		Database.getCatalog().addTable(smallHeapFileE, "e");
		Database.getCatalog().addTable(smallHeapFileF, "f");
		Database.getCatalog().addTable(smallHeapFileG, "g");
		Database.getCatalog().addTable(smallHeapFileH, "h");
		Database.getCatalog().addTable(smallHeapFileI, "i");
		Database.getCatalog().addTable(smallHeapFileJ, "j");
		Database.getCatalog().addTable(smallHeapFileK, "k");
		Database.getCatalog().addTable(smallHeapFileL, "l");
		Database.getCatalog().addTable(smallHeapFileM, "m");
		Database.getCatalog().addTable(smallHeapFileN, "n");
		Database.getCatalog().addTable(smallHeapFileO, "o");
		Database.getCatalog().addTable(smallHeapFileP, "p");
		Database.getCatalog().addTable(smallHeapFileQ, "q");
		Database.getCatalog().addTable(smallHeapFileR, "r");
		Database.getCatalog().addTable(smallHeapFileS, "s");
		Database.getCatalog().addTable(smallHeapFileT, "t");
		
		TableStats.setTableStats("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
		TableStats.setTableStats("a", new TableStats(smallHeapFileA.getId(), IO_COST));
		TableStats.setTableStats("b", new TableStats(smallHeapFileB.getId(), IO_COST));
		TableStats.setTableStats("c", new TableStats(smallHeapFileC.getId(), IO_COST));
		TableStats.setTableStats("d", new TableStats(smallHeapFileD.getId(), IO_COST));
		TableStats.setTableStats("e", new TableStats(smallHeapFileE.getId(), IO_COST));
		TableStats.setTableStats("f", new TableStats(smallHeapFileF.getId(), IO_COST));
		TableStats.setTableStats("g", new TableStats(smallHeapFileG.getId(), IO_COST));
		TableStats.setTableStats("h", new TableStats(smallHeapFileH.getId(), IO_COST));
		TableStats.setTableStats("i", new TableStats(smallHeapFileI.getId(), IO_COST));
		TableStats.setTableStats("j", new TableStats(smallHeapFileJ.getId(), IO_COST));
		TableStats.setTableStats("k", new TableStats(smallHeapFileK.getId(), IO_COST));
		TableStats.setTableStats("l", new TableStats(smallHeapFileL.getId(), IO_COST));
		TableStats.setTableStats("m", new TableStats(smallHeapFileM.getId(), IO_COST));
		TableStats.setTableStats("n", new TableStats(smallHeapFileN.getId(), IO_COST));
		TableStats.setTableStats("o", new TableStats(smallHeapFileO.getId(), IO_COST));
		TableStats.setTableStats("p", new TableStats(smallHeapFileP.getId(), IO_COST));
		TableStats.setTableStats("q", new TableStats(smallHeapFileQ.getId(), IO_COST));
		TableStats.setTableStats("r", new TableStats(smallHeapFileR.getId(), IO_COST));
		TableStats.setTableStats("s", new TableStats(smallHeapFileS.getId(), IO_COST));
		TableStats.setTableStats("t", new TableStats(smallHeapFileT.getId(), IO_COST));
		
		Parser p = new Parser();		
		Transaction t = new Transaction();
		t.start();
		p.setTransaction(t);
		
		// Each of these should return around 20,000
		// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
		// So, don't bother for now; future TODO.
		// Regardless, each of the following should be optimized to run quickly,
		// even though the worst case takes a very long time.
		PrintTupleFormatter f = new PrintTupleFormatter(System.out);
		p.processNextStatement("SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1 AND n.c0 = o.c0 AND o.c1 = p.c1 AND p.c1 = q.c1 AND q.c0 = r.c0 AND r.c1 = s.c1 AND s.c0 = t.c0;", f);
	}
}
