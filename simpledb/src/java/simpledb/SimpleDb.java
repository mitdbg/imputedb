package simpledb;

import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.List;

public class SimpleDb {
	public static void main(String args[]) throws DbException, TransactionAbortedException, IOException {
		// convert a file
		if (args[0].equals("convert")) {
			BufferedReader in = null;
			BufferedOutputStream out = null;
			try {
				if (args.length < 3 || args.length > 5) {
					System.err.println("Usage: simpledb convert FILE NUM_COLUMNS");
					System.err.println("Usage: simpledb convert FILE TYPE_STRING [FIELD_SEP]");
					System.exit(-1);
				}
				
				File inFile = new File(args[1]);
				if (!inFile.canRead()) {
					System.err.format("Cannot read input file: %s", inFile);
					System.exit(-1);
				}
				in = new BufferedReader(new FileReader(args[1]));
				
				out = new BufferedOutputStream(System.out);
				
				int numOfAttributes = Integer.parseInt(args[2]);
				Type[] ts = new Type[numOfAttributes];
				char fieldSeparator = ',';

				if (args.length == 3)
					for (int i = 0; i < numOfAttributes; i++)
						ts[i] = Type.INT_TYPE;
				else {
					String typeString = args[3];
					String[] typeStringAr = typeString.split(",");
					if (typeStringAr.length != numOfAttributes) {
						System.err.println("The number of types does not agree with the number of columns");
						return;
					}
					int index = 0;
					for (String s : typeStringAr) {
						try {
							ts[index++] = Type.ofString(s);
						} catch (ParseException ex) {
							System.err.println(ex.getMessage());
							return;
						}
					}
					if (args.length == 5)
						fieldSeparator = args[4].charAt(0);
				}

				HeapFileEncoder.convert(in, out, BufferPool.getPageSize(), numOfAttributes, ts, fieldSeparator);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				if (in != null) {
					in.close();
				}
				if (out != null) {
					out.close();
				}
			}
		} else if (args[0].equals("print")) {
			if (args.length != 3) {
				System.out.println("Usage: simpledb print FILE NUM_COLUMNS");
				System.exit(-1);
			}
			
			File tableFile = new File(args[1]);
			int columns = Integer.parseInt(args[2]);
			DbFile table = Utility.openHeapFile(columns, tableFile);
			
			TransactionId tid = new TransactionId();
			DbFileIterator it = table.iterator(tid);
			if (null == it) {
				System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
			} else {
				it.open();
				while (it.hasNext()) {
					Tuple t = it.next();
					System.out.println(t);
				}
				it.close();
			}
		} else if (args[0].equals("parser")) {
			// Strip the first argument and call the parser
			String[] newargs = new String[args.length - 1];
			for (int i = 1; i < args.length; ++i) {
				newargs[i - 1] = args[i];
			}

			try {
				// dynamically load Parser -- if it doesn't exist, print error
				// message
				Class<?> c = Class.forName("simpledb.Parser");
				Class<?> s = String[].class;

				java.lang.reflect.Method m = c.getMethod("main", s);
				m.invoke(null, (java.lang.Object) newargs);
			} catch (ClassNotFoundException cne) {
				System.out.println(
						"Class Parser not found -- perhaps you are trying to run the parser as a part of lab1?");
			} catch (Exception e) {
				System.out.println("Error in parser.");
				e.printStackTrace();
			}

		} else if (args[0].equals("draw")) {
			if (args.length != 4) {
				System.out.println("Usage: simpledb draw catalog query_file output_prefix");
				System.exit(-1);
			}

			Database.getCatalog().loadSchema(args[1]);
			TableStats.computeStatistics();

			Path path = Paths.get(args[2]);
			String outputPrefix = args[3];

			List<String> lines = Files.readAllLines(path);
			StringBuilder sb = new StringBuilder();
			for(String line : lines) {
				sb.append(line);
			}
			String text = sb.toString();
			String[] queries = text.split(";");
			for(int i = 0; i < queries.length; i++) {
				try {
					String query = queries[i] + ";";
					System.out.println("Query: " + query);
					ZqlParser p = new ZqlParser(new ByteArrayInputStream(query.getBytes("UTF-8")));
					ZStatement s = p.readStatement();
					Parser pp = new Parser();
					Query plan = pp.handleQueryStatement((ZQuery)s, new TransactionId());
					String fileName = outputPrefix + "_" + i + ".dot";
					QueryPlanDotter.print(plan.getPhysicalPlan(), fileName);
				} catch (Zql.ParseException | ParsingException e) {
					e.printStackTrace();
				}
			}
		} else {
			System.err.println("Unknown command: " + args[0]);
			System.exit(1);
		}
	}

}
