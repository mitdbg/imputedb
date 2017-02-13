package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class ImputedPlanCacheDotted extends ImputedPlanCache {
	private final File outDir;
	
	public ImputedPlanCacheDotted(File outDir) throws FileNotFoundException {
		super();
		if (!outDir.exists()) {
			outDir.mkdir();
		} else if (!outDir.isDirectory()) {
			throw new IllegalArgumentException();
		}
		
		this.outDir = outDir;
	}
	
	private void writePlan(Set<QualifiedName> dirtySet, ImputedPlan plan, double lossWeight) {
		StringBuilder prefix = new StringBuilder();
		prefix.append(plan.cost(lossWeight));
		prefix.append(" ");
		prefix.append(plan.cardinality());
		prefix.append(" ");
		
		try {
			File outFile = File.createTempFile(prefix.toString(), ".dot", outDir);
			QueryPlanDotter.print(plan.getPlan(), outFile.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	void addJoinPlan(Set<String> ts, Set<QualifiedName> dirtySet, Set<LogicalJoinNode> joins, ImputedPlan newPlan, double lossWeight) {
		super.addJoinPlan(ts, dirtySet, joins, newPlan, lossWeight);
		writePlan(dirtySet, newPlan, lossWeight);
	}
	
	@Override
	void addPlan(Set<String> ts, Set<QualifiedName> dirtySet, ImputedPlan newPlan, double lossWeight) {
		super.addPlan(ts, dirtySet, newPlan, lossWeight);
		writePlan(dirtySet, newPlan, lossWeight);
	}
}
