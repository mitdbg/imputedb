package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import simpledb.AImputedPlanCache.Value;

public class ImputedPlanCacheLogged implements ImputedPlanCache {
	private final PrintWriter writer;
	private final ImputedPlanCache cache;
	private final QueryPlanVisualizer viz = new QueryPlanVisualizer();
	
	public ImputedPlanCacheLogged(File log, ImputedPlanCache cache) throws FileNotFoundException {
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
	
	@Override
	public void addPlan(Set<String> ts, ImputedPlan newPlan) {
		addJoinPlan(ts, new HashSet<LogicalJoinNode>(), newPlan);
	}
	
	@Override
	public void addJoinPlan(Set<String> ts, Set<LogicalJoinNode> joins, ImputedPlan newPlan) {
		cache.addJoinPlan(ts, joins, newPlan);
		
		writer.print("Tables: ");
		writeSet(ts);
		writer.print("\n");
		
		writer.print("Dirty: ");
		writeSet(newPlan.getDirtySet());
		writer.print("\n");
		
		writer.format("Loss: %f\n", newPlan.loss().get());
		writer.format("Time: %f\n", newPlan.time());
		
		try {
			writer.print(viz.getQueryPlanTree(newPlan.getPlan()));
		} catch (ArrayIndexOutOfBoundsException e) {
		}
		writer.println();
		
		writer.flush();
	}

	@Override
	public Iterable<Value> bestPlans(Set<String> tables) {
		return cache.bestPlans(tables);
	}

	@Override
	public Iterable<Value> bestPlans(Set<String> tables, Set<QualifiedName> dirty) {
		return cache.bestPlans(tables, dirty);
	}
}
