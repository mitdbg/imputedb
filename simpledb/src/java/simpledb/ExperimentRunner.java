package simpledb;

import Zql.ParseException;
import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;


public class ExperimentRunner {
    private static final double ALPHA_EPSILON = 1e-10;
    private final boolean imputeAtBase;
    private final double minAlpha;
    private final double maxAlpha;
    private final double step;
    private final int iters;
    private final Path catalogPath;
    private final Path queriesPath;
    private final Path outputBaseDir;
    private final boolean planOnly;
    private final String imputationMethod;
    private FileWriter timesFileWriter;
    private FileWriter resultsFileWriter;
    private FileWriter plansFileWriter;
    private String[] queries;

    private ExperimentRunner(boolean imputeAtBase, double minAlpha, double
            maxAlpha, double step, int iters, String catalog, String queries,
            String outputBaseDir, boolean planOnly, String imputationMethod)
            throws IOException {
        this.imputeAtBase = imputeAtBase;
        this.minAlpha = minAlpha;
        this.maxAlpha = maxAlpha;
        this.step = step;
        this.iters = iters;
        this.catalogPath = Paths.get(catalog);
        this.queriesPath = Paths.get(queries);
        if (imputeAtBase){
            this.outputBaseDir = Paths.get(outputBaseDir, "base");
        } else {
            this.outputBaseDir = Paths.get(outputBaseDir);
        }
        this.planOnly = planOnly;
        this.imputationMethod = imputationMethod;
       	ImputeFactory.setImputationMethod(this.imputationMethod);
    }

    // constructor for invocation of experiments with --base
    public ExperimentRunner(int iters, String catalog, String queries, String
            outputDir, String imputationMethod)
            throws IOException {
        this(true, 0.0, 0.0, 1.0, iters, catalog, queries, outputDir, false,
                imputationMethod);
    }

    // constructor for other invocation of experiments
    public ExperimentRunner(double minAlpha, double maxAlpha, double step, int
            iters, String catalog, String queries, String outputDir, boolean
            planOnly, String imputationMethod)
            throws IOException {
        this(false, minAlpha, maxAlpha, step, iters, catalog, queries,
                outputDir, planOnly, imputationMethod);
    }

    private void init() throws IOException {
        Database.getCatalog().clear();
        Database.getCatalog().loadSchema(this.catalogPath.toAbsolutePath().toString());
        TableStats.computeStatistics();
    }

    private static DbIterator planQuery(String query, Function<Void,
            LogicalPlan> planFactory)
            throws ParseException, TransactionAbortedException, DbException,
                              IOException, ParsingException {
        ZqlParser p = new ZqlParser(new ByteArrayInputStream(query.getBytes("UTF-8")));
        ZStatement s = p.readStatement();
        Parser pp = new Parser(planFactory);
        return pp.handleQueryStatementSilent((ZQuery)s, new TransactionId()).getPhysicalPlan();
    }

    private void openTimeWriter(int q, double alpha) throws IOException {
    Path outputDir = outputBaseDir.resolve(String.format("q%02d/alpha%03.0f", q, alpha*1000));
    if (Files.notExists(outputDir)){
        Files.createDirectories(outputDir);
    }
    this.timesFileWriter = new FileWriter(outputDir.resolve("timing.csv").toFile());
        this.timesFileWriter.write("query,alpha,iter,plan_time,run_time,plan_hash\n");
    }

    private void closeTimeWriter() throws IOException {
        this.timesFileWriter.close();
    }

    private void openOtherWriters(int q, double alpha, int iter) throws IOException {
    Path outputDir = outputBaseDir.resolve(String.format("q%02d/alpha%03.0f/it%05d", q, alpha*1000, iter));
    if (Files.notExists(outputDir)){
        Files.createDirectories(outputDir);
    }
    this.resultsFileWriter = new FileWriter(outputDir.resolve("result.txt").toFile());
    this.plansFileWriter = new FileWriter(outputDir.resolve("plan.txt").toFile());
    }

    private void closeOtherWriters() throws IOException {
        this.resultsFileWriter.close();
        this.plansFileWriter.close();
    }

    private void writeTime(int q, double alpha, int iter, long planTime, long runTime, int planHash) throws IOException {
        this.timesFileWriter.write(String.format("%d,%f,%d,%d,%d,%s\n", q, alpha, iter, planTime, runTime, planHash));
    }

    private void writeResult(int q, double alpha, int iter, String results) throws IOException{
        this.resultsFileWriter.write(results);
        this.resultsFileWriter.write("\n");
    }

    private void writePlan(int q, double alpha, int iter, String plan) throws IOException{
        this.plansFileWriter.write(plan);
        this.plansFileWriter.write("\n");
    }

    private void run(int q, double alpha, int iter)
            throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {

        String query = queries[q] + ";";

        // time planning
        Instant planStart = Instant.now();
        DbIterator imputed  = planQuery(query, x -> new ImputedLogicalPlan(alpha, this.imputeAtBase));
        Instant planEnd = Instant.now();

        // time running
        StringBuilder results = new StringBuilder();
        Instant runStart = Instant.now();
        // some queries we only want to time the planning phase
        if (!planOnly) {
            imputed.open();
            while (imputed.hasNext()) {
                try {
                    results.append(imputed.next().toString());
                    results.append("\n");
                } catch (NoSuchElementException e) {
                    e.printStackTrace();
                }
            }
        }
        Instant runEnd = Instant.now();

        // get plan run
        QueryPlanVisualizer viz = new QueryPlanVisualizer();
        String plan = viz.getQueryPlanTree(imputed);

        // Write results
        writeTime(q, alpha, iter, Duration.between(planStart, planEnd).toMillis(),
            Duration.between(runStart, runEnd).toMillis(), plan.hashCode());
        writeResult(q, alpha, iter, results.toString());
        writePlan(q, alpha, iter, plan);
    }

    private String[] getQueries() throws IOException {
        List<String> lines = Files.readAllLines(this.queriesPath);
        StringBuilder sb = new StringBuilder();
        for(String line : lines) {
            sb.append(" "); // add blanks space to avoid combining terms across lines
            sb.append(line);
        }
        String text = sb.toString();
        String[] queries = text.split(";");
        System.out.println(Arrays.toString(queries));
        return queries;
    }

    public void runExperiments()
            throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
        init();
        this.queries = getQueries();
        for (int q = 0; q < queries.length; q++) {
            for (double alpha = this.minAlpha; alpha <= (this.maxAlpha + ALPHA_EPSILON); alpha += this.step) {
                openTimeWriter(q, alpha);
                for (int i = 0; i < this.iters; i++) {
                    openOtherWriters(q, alpha, i);
                    System.out.println("Running query " + q + ", alpha " + alpha
                            + ", iter " + i + " (impute " + this.imputationMethod + ")");
                    run(q, alpha, i);
                    closeOtherWriters();
                }
                closeTimeWriter();
            }
        }
    }
}
