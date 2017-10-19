package simpledb;

import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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
			ret = experiment(args); // TODO: Redo argument parsing.
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
                .longOpt("db")
                .argName("path")
                .hasArg()
                .required()
                .type(File.class)
                .desc("the directory which contains the database")
                .build());
        options.addOption(Option.builder("c")
                .optionalArg(true)
                .hasArg()
                .argName("command")
                .type(String.class)
                .desc("run a single query and exit")
                .build());
        options.addOption(Option.builder()
                .longOpt("csv")
                .optionalArg(true)
                .build());

	    CommandLineParser parser = new DefaultParser();
	    try {
	        CommandLine line = parser.parse(options, args);
            double alpha = Double.parseDouble(line.getOptionValue("alpha", defaultAlpha));
            File catalogFile = new File(line.getOptionValue("db") + "/catalog.txt");
            String query = line.getOptionValue("c", null);
            boolean useCsv = line.hasOption("csv");

            Parser sqlParser = new Parser(alpha, false);

            if (query == null) {
                return sqlParser.start(catalogFile, false);
            } else {
                sqlParser.runSingleQuery(catalogFile, query, false, useCsv);
                return 0;
            }
        } catch (org.apache.commons.cli.ParseException exp) {
	        System.err.println(exp.getMessage());
	        HelpFormatter formatter = new HelpFormatter();
	        formatter.printHelp("simpledb query", options);
	        return 1;
        }
	}

	private static int print(String[] args) throws DbException, TransactionAbortedException {
        Options options = new Options();
        options.addOption(Option.builder()
            .longOpt("table")
            .argName("name")
            .hasArg()
            .required()
            .type(String.class)
            .desc(String.format("the table to print"))
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
            String tableName = line.getOptionValue("table");
            File catalogFile = new File(line.getOptionValue("catalog"));

            Catalog catalog = Database.getCatalog();
            catalog.loadSchema(catalogFile.toString());
            DbFile table = catalog.getDatabaseFile(catalog.getTableId(tableName));

            TransactionId tid = new TransactionId();
            DbFileIterator it = table.iterator(tid);
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                System.out.println(t);
            }
            it.close();
            return 0;
        } catch (org.apache.commons.cli.ParseException exp) {
            System.err.println(exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("simpledb print", options);
            return 1;
        }
	}

	private static int load(String[] args) {
	    String nullStringsDefault = "#N/A,#NA,-1.#IND,-1.#QNAN,-NaN,-nan,1.#IND,1.#QNAN,N/A,NA,NULL,NaN,nan";

        Options options = new Options();
        options.addOption(Option.builder()
                .longOpt("null-strings")
                .argName("strings")
                .hasArg()
                .type(String.class)
                .desc(String.format("strings which will be treated as null values (default: %s)",
                        nullStringsDefault))
                .build());
        options.addOption(Option.builder()
                .longOpt("db")
                .argName("path")
                .hasArg()
                .required()
                .type(File.class)
                .desc("the directory to write the new database tables in")
                .build());

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine line = parser.parse(options, args);
            File outDir = new File(line.getOptionValue("db"));
            File catalogFile = new File(outDir.toString() + "/catalog.txt");

            Set<String> nullStrings = new HashSet<>();
            for (String s : line.getOptionValue("null-strings", nullStringsDefault).split(",")) {
                nullStrings.add(s);
            }
            String[] inputFiles = line.getArgs();

            if (outDir.exists()) {
                System.err.format("Database %s already exists. Remove? [y/n] ", outDir);
                if ((new Scanner(System.in)).nextLine().toLowerCase().startsWith("y")) {
                    Utility.removeDirectory(outDir);
                } else {
                    return 0;
                }
            }

            if (!outDir.mkdirs()) {
                System.err.format("Error: Failed to create database %s.\n", outDir);
                return 1;
            }

            Catalog catalog = new Catalog();

            for (String fileStr : inputFiles) {
                File inFile = new File(fileStr);

                if (!inFile.getPath().endsWith(".csv")) {
                    System.err.format("Error: Expected a CSV file: %s\n", inFile);
                    continue;
                }

                String tableName = Utility.stripSuffix(inFile.getName(), ".csv");
                File outFile = new File(outDir.toString() + "/" + tableName + ".dat");

                try (
                    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))
                ) {
                    TupleDesc desc = HeapFileEncoder.convert(inFile, out, BufferPool.getPageSize(), ',', nullStrings, true);
                    catalog.addTable(new HeapFile(outFile, desc), tableName);
                } catch (IOException e) {
                    System.err.format("Error: Processing file failed: %s\n", inFile);
                    e.printStackTrace(System.err);
                    continue;
                }
            }

            catalog.dumpSchema(catalogFile);
        } catch (org.apache.commons.cli.ParseException exp) {
            System.err.println(exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("simpledb load", options);
            return 1;
        }

        return 0;
	}

}
