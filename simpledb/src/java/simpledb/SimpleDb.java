package simpledb;

import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.*;

public class SimpleDb {
	public static void main(String args[]) throws DbException, TransactionAbortedException, IOException {
		String usage =
				"Usage: simpledb load [OPTION]...\n" +
				"       simpledb query [OPTION]...\n" +
				"       simpledb experiment [OPTION]...\n" +
				"       simpledb print [OPTION]..."
				;

		if (args.length < 1) {
			System.err.println(usage);
			System.exit(1);
		}

		String command = args[0];
		String[] rest = Arrays.copyOfRange(args, 1, args.length);
		int ret;
		if (command.equals("load")) {
			ret = load(rest);
		} else if (command.equals("print")) {
			ret = print(rest);
		} else if (command.equals("query")) {
			ret = query(rest);
		} else if (command.equals("experiment")) {
			ret = experiment(rest);
		} else if (command.equals("draw")) {
			ret = draw(rest);
		} else {
			ret = 1;
			System.err.println(usage);
		}
		System.exit(ret);
	}

	private static int draw(String[] args) throws IOException, TransactionAbortedException, DbException {
		if (args.length != 5) {
            System.out.println("Usage: simpledb draw catalog query_file output_prefix alpha");
            System.exit(-1);
        }

		Database.getCatalog().loadSchema(args[1]);
		TableStats.computeStatistics();

		Path path = Paths.get(args[2]);
		String outputPrefix = args[3];
		double alpha = Double.parseDouble(args[4]);

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
                Parser pp = new Parser(x -> new ImputedLogicalPlan(alpha));
                Query plan = pp.handleQueryStatement((ZQuery)s, new TransactionId());
                String fileName = outputPrefix + "_" + i + ".dot";
                QueryPlanDotter.print(plan.getPhysicalPlan(), fileName);
            } catch (Zql.ParseException | ParsingException e) {
                e.printStackTrace();
            }
        }
        return 0;
	}

	private static int experiment(String[] args) throws IOException {
		if (args.length < 6) {
//                                         0          1
            System.err.println("Usage: java -jar <JAR> experiment <catalog> "
//    2         3            4
+"<queries> <output-dir> <iters> "
//    5           6          7
+ "<minAlpha> <maxAlpha> <step> "
//    8                  9
+ "[--planOnly=BOOL] [--imputationMethod=METHOD]");

//                                         0          1
            System.err.println("Usage: java -jar <JAR> experiment <catalog> "
//    2          3            4
+ "<queries> <output-dir> <iters> "
//    5       6
+ "--base [--imputationMethod=METHOD]");
            System.exit(1);
        }

		String catalog = args[1];
		String queries = args[2];
		String outputDir = args[3];
		int iters = Integer.parseInt(args[4]);

		ExperimentRunner runner;
		if (args[5].equalsIgnoreCase("--base")) {
            String imputationMethod = ImputeFactory.DEFAULT_IMPUTATION_METHOD;
            if (args.length >= 7 && args[6].startsWith("--imputationMethod=")){
                imputationMethod = args[6].split("=")[1];
            }
            runner = new ExperimentRunner(iters, catalog, queries, outputDir, imputationMethod);
        } else {
            double minAlpha = Double.parseDouble(args[5]);
            double maxAlpha = Double.parseDouble(args[6]);
            double step = Double.parseDouble(args[7]);
            boolean planOnly = false;
            if (args.length >= 9  && args[8].startsWith("--planOnly=")) {
                planOnly = Boolean.parseBoolean(args[8].split("=")[1]);
            }
            String imputationMethod = ImputeFactory.DEFAULT_IMPUTATION_METHOD;
            if (args.length >= 10 && args[9].startsWith("--imputationMethod=")) {
                imputationMethod = args[9].split("=")[1];
            }
runner = new ExperimentRunner(minAlpha, maxAlpha, step, iters,
catalog, queries, outputDir, planOnly,
imputationMethod);
        }
		try {
            runner.runExperiments();
        } catch (Exception e) {
            System.err.println("Failed to run experiments: " + e.getMessage());
            e.printStackTrace();
        }

        return 0;
	}

	private static int query(String[] args) {
	    String defaultAlpha = "0.0";

	    Options options = new Options();
	    options.addOption(Option.builder()
                .longOpt("alpha")
                .argName("value")
                .hasArg()
                .type(Double.class)
                .desc(String.format("the level of imputation (default: %s)", defaultAlpha))
                .build());
        options.addOption(Option.builder()
                .longOpt("catalog")
                .argName("file")
                .hasArg()
                .required()
                .type(File.class)
                .desc("the catalog file describing the tables in the database")
                .build());

	    CommandLineParser parser = new DefaultParser();
	    try {
	        CommandLine line = parser.parse(options, args);
            double alpha = Double.parseDouble(line.getOptionValue("alpha", defaultAlpha));
            File catalogFile = new File(line.getOptionValue("catalog"));

            Parser sqlParser = new Parser(alpha, false);
            return sqlParser.start(catalogFile, false);
        } catch (org.apache.commons.cli.ParseException exp) {
	        System.err.println(exp.getMessage());
	        HelpFormatter formatter = new HelpFormatter();
	        formatter.printHelp("simpledb query", options);
	        return 1;
        }
	}

	private static int print(String[] args) throws DbException, TransactionAbortedException {
		if (args.length != 3) {
            System.out.println("Usage: simpledb print FILE NUM_COLUMNS");
            return 1;
        }

		File tableFile = new File(args[1]);
		int columns = Integer.parseInt(args[2]);
		DbFile table = Utility.openHeapFile(columns, tableFile);

		TransactionId tid = new TransactionId();
		DbFileIterator it = table.iterator(tid);
		if (null == it) {
            System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
            return 1;
        } else {
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                System.out.println(t);
            }
            it.close();
        }
        return 0;
	}

	private static int load(String[] args) throws IOException {
		BufferedReader in = null;
		BufferedOutputStream out = null;
		try {
            if (args.length < 3 || args.length > 5) {
                System.err.println("Usage: simpledb convert FILE NUM_COLUMNS");
                System.err.println("Usage: simpledb convert FILE NUM_COLUMNS TYPE_STRING [FIELD_SEP]");
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
                    return 1;
                }
                int index = 0;
                for (String s : typeStringAr) {
                    try {
                        ts[index++] = Type.ofString(s);
                    } catch (ParseException ex) {
                        System.err.println(ex.getMessage());
                        return 1;
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
        return 0;
	}

}
