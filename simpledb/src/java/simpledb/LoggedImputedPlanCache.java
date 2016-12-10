package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Set;

public class LoggedImputedPlanCache extends ImputedPlanCache {
	private final PrintWriter writer;
	private final QueryPlanVisualizer viz = new QueryPlanVisualizer();
	
	public LoggedImputedPlanCache(File log) throws FileNotFoundException {
		super();
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
	
	@Override
	void addPlan(Set<String> ts, Set<QuantifiedName> dirtySet, ImputedPlan newPlan, double lossWeight) {
		super.addPlan(ts, dirtySet, newPlan, lossWeight);
		
		writer.print("Tables: ");
		writeSet(ts);
		writer.print("\n");
		
		writer.print("Dirty: ");
		writeSet(dirtySet);
		writer.print("\n");
		
		writer.format("Cost: %f\n", newPlan.cost(lossWeight));
		
		try {
			writer.print(viz.getQueryPlanTree(newPlan.getPlan()));
		} catch (ArrayIndexOutOfBoundsException e) {
		}
		writer.println();
		
		writer.flush();
	}
	
	@Override
	void addJoinPlan(Set<String> ts, Set<QuantifiedName> dirtySet, Set<LogicalJoinNode> joins, ImputedPlan newPlan, double lossWeight) {
		super.addJoinPlan(ts, dirtySet, joins, newPlan, lossWeight);
		
		writer.print("Tables: ");
		writeSet(ts);
		writer.print("\n");
		
		writer.print("Dirty: ");
		writeSet(dirtySet);
		writer.print("\n");
		
		writer.format("Cost: %f\n", newPlan.cost(lossWeight));
		
		try {
			writer.print(viz.getQueryPlanTree(newPlan.getPlan()));
		} catch (ArrayIndexOutOfBoundsException e) {
		}
		writer.println();
		
		writer.flush();
	}
}
