package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import simpledb.AImputedPlanCache.Value;

public class ImputedPlanCacheDotted implements ImputedPlanCache {
	private final File outDir;
	private final ImputedPlanCache cache;
	
	public ImputedPlanCacheDotted(File outDir, ImputedPlanCache cache) throws FileNotFoundException {
		this.cache = cache;
		
		if (!outDir.exists()) {
			outDir.mkdir();
		} else if (!outDir.isDirectory()) {
			throw new IllegalArgumentException();
		}
		
		this.outDir = outDir;
	}
	
	private void writePlan(Set<QualifiedName> dirtySet, ImputedPlan plan) {
		StringBuilder prefix = new StringBuilder();
		prefix.append(plan.loss().get());
		prefix.append(" ");
		prefix.append(plan.time());
		prefix.append(" ");
		try {
			prefix.append(plan.cardinality());
		} catch (UnsupportedOperationException e) {
			prefix.append("??");
		}
		prefix.append(" ");
		
		try {
			File outFile = File.createTempFile(prefix.toString(), ".dot", outDir);
			QueryPlanDotter.print(plan.getPlan(), outFile.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public void addJoinPlan(Set<String> ts, Set<LogicalJoinNode> joins, ImputedPlan newPlan) {
		cache.addJoinPlan(ts, joins, newPlan);
		writePlan(newPlan.getDirtySet(), newPlan);
	}
	
	@Override
	public void addPlan(Set<String> ts, ImputedPlan newPlan) {
		cache.addPlan(ts, newPlan);
		writePlan(newPlan.getDirtySet(), newPlan);
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
