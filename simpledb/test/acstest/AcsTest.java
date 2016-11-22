package acstest;

import java.util.*;
import java.io.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import Zql.ParseException;
import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;
import simpledb.*;

@RunWith(Parameterized.class)
public class AcsTest {
	private final String query;
	
	public AcsTest(String query) {
		this.query = query;
	}
	
	@Parameters(name = "{index}: {0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{ "SELECT BLD as units_in_structure, COUNT(*) as estimate FROM acs GROUP BY BLD;" },
			{ "SELECT BATH as has_bath, COUNT(*) as ct FROM acs GROUP BY BATH;" },
			{ "SELECT ACR as lotsize, AVG(BDSP) as avg_num_bedrooms FROM acs GROUP BY ACR;" }
		});
	}
	
	@Before
	public void loadData() {
		Database.getCatalog().clear();
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
		TupleDesc schema = new TupleDesc(types, fields);
		Database.getCatalog().addTable(new HeapFile(new File("acs.dat"), schema), "acs");
	}
	
	@Test
	public void runQuery() throws TransactionAbortedException, DbException, IOException, ParsingException, ParseException {
		ZqlParser p = new ZqlParser(new ByteArrayInputStream(query.getBytes("UTF-8")));
        ZStatement s = p.readStatement();
        Parser pp = new Parser();
        pp.handleQueryStatement((ZQuery)s, new TransactionId());
	}
}
