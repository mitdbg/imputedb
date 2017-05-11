package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Set;

public class ImputedPlanCacheFileWriter {
	private final PrintWriter writer;
	private final ImputedPlanCache cache;
	private final QueryPlanVisualizer viz = new QueryPlanVisualizer();
	
	public ImputedPlanCacheFileWriter(File log, ImputedPlanCache cache) throws FileNotFoundException {
		this.cache = cache;
		writer = new PrintWriter(new FileOutputStream(log, true));
	}
	
	private <T> void writeSet(Set<T> s) {
		int i = 0;
		writer.print("{");
		for (T e : s) {
			writer.print(" ");
			writer.print(e);
			if (i < s.size()) {
				writer.print(",");
			} else {
				writer.print(" ");
			}
			i++;
		}
		writer.print("}");
	}
	
	public void write(Set<String> tables) {
		for (AImputedPlanCache.Value val : cache.bestPlans(tables)) {
			writer.print("Tables: ");
			writeSet(tables);
			writer.print("\n");
			
			writer.print("Dirty: ");
			writeSet(val.plan.getDirtySet());
			writer.print("\n");
			
			writer.format("Loss: %f\n", val.plan.penalty().get());
			writer.format("Time: %f\n", val.plan.time());
			
			try {
				writer.print(viz.getQueryPlanTree(val.plan.getPlan()));
			} catch (ArrayIndexOutOfBoundsException e) {
			}
			writer.println();
			
			writer.flush();
		}
	}
}
