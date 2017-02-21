package simpledb;

import Zql.ParseException;
import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;

import java.io.ByteArrayInputStream;
import java.io.File;
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
    private final boolean imputeAtBase;
    private final double minAlpha;
    private final double maxAlpha;
    private final double step;
    private final int iters;
    private final Path catalogPath;
    private final Path queriesPath;
    private final FileWriter timesFileWriter;
    private final FileWriter resultsFileWriter;
    private final FileWriter plansFileWriter;

    private ExperimentRunner(boolean imputeAtBase, double minAlpha, double maxAlpha, double step, int iters, String catalog, String queries, String outputDir)
            throws IOException {
        this.imputeAtBase = imputeAtBase;
        this.minAlpha = minAlpha;
        this.maxAlpha = maxAlpha;
        this.step = step;
        this.iters = iters;
        this.catalogPath = Paths.get(catalog);
        this.queriesPath = Paths.get(queries);
        this.timesFileWriter = new FileWriter(new File(outputDir, "times.csv"));
        this.resultsFileWriter = new FileWriter(new File(outputDir, "results.txt"));
        this.plansFileWriter = new FileWriter(new File(outputDir, "plans.txt"));
    }

    public ExperimentRunner(int iters, String catalog, String queries, String outputDir)
            throws IOException {
        this(true, -1.0, -1.0, 1.0, iters, catalog, queries, outputDir);
    }

    public ExperimentRunner(double minAlpha, double maxAlpha, double step, int iters, String catalog, String queries, String outputDir)
            throws IOException {
        this(false, minAlpha, maxAlpha, step, iters, catalog, queries, outputDir);
    }

    private void init() throws IOException {
        Database.getCatalog().clear();
        Database.getCatalog().loadSchema(this.catalogPath.toAbsolutePath().toString());
        TableStats.computeStatistics();

        this.timesFileWriter.write("query,alpha,iter,plan_time,run_time,plan_hash\n");
        this.resultsFileWriter.write("Query Results\n");
        this.plansFileWriter.write("Query Plans\n");
    }

    public void close() throws IOException {
        this.timesFileWriter.close();
        this.resultsFileWriter.close();
        this.plansFileWriter.close();
    }

    private static DbIterator planQuery(String query, Function<Void, LogicalPlan> planFactory)
            throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
        ZqlParser p = new ZqlParser(new ByteArrayInputStream(query.getBytes("UTF-8")));
        ZStatement s = p.readStatement();
        Parser pp = new Parser(planFactory);
        return pp.handleQueryStatementSilent((ZQuery)s, new TransactionId()).getPhysicalPlan();
    }

    private void writeTime(int id, double alpha, int iter, long planTime, long runTime, int planHash) throws IOException {
        this.timesFileWriter.write(String.format("%d,%f,%d,%d,%d,%s\n", id, alpha, iter, planTime, runTime, planHash));
    }

    private void writeResult(int id, double alpha, String results) throws IOException{
        this.resultsFileWriter.write("Query " + id + "\n");
        this.resultsFileWriter.write("Alpha: " + alpha + "\n");
        this.resultsFileWriter.write("Results: \n");
        this.resultsFileWriter.write(results);
        this.resultsFileWriter.write("\n\n\n");
    }

    private void writePlan(int id, double alpha, String plan) throws IOException{
        this.plansFileWriter.write("Query " + id + "\n");
        this.plansFileWriter.write("Alpha: " + alpha + "\n");
        this.plansFileWriter.write("Plan: \n");
        this.plansFileWriter.write(plan);
        this.plansFileWriter.write("\n\n\n");
    }

    private void run(String query, int id, double alpha, int iter)
            throws ParseException, TransactionAbortedException, DbException, IOException, ParsingException {
        // time planning
        Instant planStart = Instant.now();
        DbIterator imputed  = planQuery(query, x -> new ImputedLogicalPlan(alpha, this.imputeAtBase));
        Instant planEnd = Instant.now();

        StringBuilder results = new StringBuilder();
        // time running
        Instant runStart = Instant.now();
        imputed.open();
        while (imputed.hasNext()) {
            try {
                results.append(imputed.next().toString());
                results.append("\n");
            } catch(NoSuchElementException e) {
                e.printStackTrace();
            }
        }
        Instant runEnd = Instant.now();
        // getting plan run
        QueryPlanVisualizer viz = new QueryPlanVisualizer();
        String plan = viz.getQueryPlanTree(imputed);

        // write out times
        writeTime(id, alpha, iter, Duration.between(planStart, planEnd).toMillis(), Duration.between(runStart, runEnd).toMillis(), plan.hashCode());

        // if its the first time, go ahead and write out results and plan
        if (iter == 0) {
            writeResult(id, alpha, results.toString());
            writePlan(id, alpha, plan);
        }
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
        String[] queries = getQueries();
        for (double alpha = this.minAlpha; alpha <= this.maxAlpha; alpha += this.step) {
            for (int q = 0; q < queries.length; q++) {
                for (int i = 0; i < this.iters; i++) {
                    System.out.println("Running query " + q + " at alpha " + alpha + " iteration " + i);
                    run(queries[q] + ";", q, alpha, i);
                }
            }
        }
        close();
    }
}
