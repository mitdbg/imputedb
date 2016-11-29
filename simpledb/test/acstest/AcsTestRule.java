package acstest;

import java.io.File;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import simpledb.Database;
import simpledb.HeapFile;
import simpledb.TableStats;
import simpledb.TupleDesc;
import simpledb.Type;

public class AcsTestRule implements TestRule {
	public static final TupleDesc SCHEMA;
	static {
		String[] fields = new String[] {
				"ST",
				"NP",
				"ACR",
				"AGS",
				"BATH",
				"BDSP",
				"BLD",
				"BUS",
				"REFR",
				"RMSP",
				"RWAT",
				"SINK",
				"STOV",
				"TEL",
				"TEN",
				"TOIL",
				"VEH",
				"YBL",
				"HHL",
				"HHT",
				"HUGCLNPP",
				"HUPAC",
				"HUPAOC",
				"HUPARC",
				"LNGI",
				"MULTG",
				"MV",
				"NR",
				"NRC",
				"PARTNER",
				"PSF",
				"R18",
				"R65",
				"SRNTVAL",
				"WIF",
				"WKEXREL",
				"WORKSTAT"
		};
		Type[] types = new Type[fields.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = Type.INT_TYPE;
		}
		SCHEMA = new TupleDesc(types, fields);
	}
	
	private class DbInitializer extends ExternalResource {
		@Override 
		protected void before() {
			Database.getCatalog().clear();
			ClassLoader loader = CleanTest.class.getClassLoader();
			File acsData = new File(loader.getResource("acstest/acs.dat").getFile());
			Database.getCatalog().addTable(new HeapFile(acsData, SCHEMA), "acs", "");
			TableStats.computeStatistics();
		}
	}
	
	private final DbInitializer initializer = new DbInitializer();

	@Override
	public Statement apply(Statement arg0, Description arg1) {
		return initializer.apply(arg0, arg1);
	}
}