package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

public class ImputedPlanCacheDottedWriter implements ImputedPlanCacheWriter {
	private final File outDir;
	private final ImputedPlanCache cache;
	
	public ImputedPlanCacheDottedWriter(File outDir, ImputedPlanCache cache) throws FileNotFoundException {
		this.cache = cache;
		
		if (!outDir.exists()) {
			outDir.mkdir();
		} else if (!outDir.isDirectory()) {
			throw new IllegalArgumentException();
		}
		
		this.outDir = outDir;
	}
	
	/* (non-Javadoc)
	 * @see simpledb.ImputedPlanCacheWriter#write(java.util.Set)
	 */
	@Override
	public void write(Set<String> tables) {
		for (AImputedPlanCache.Value val : cache.bestPlans(tables)) {
			ImputedPlan plan = val.plan;
			StringBuilder prefix = new StringBuilder();
			prefix.append("queryplan_loss=");
			prefix.append(plan.penalty().get());
			prefix.append("_time=");
			prefix.append(plan.time());
			prefix.append("_card=");
			try {
				prefix.append(plan.cardinality());
			} catch (UnsupportedOperationException e) {
				prefix.append("??");
			}
			prefix.append("_");
			
			try {
				File outFile = File.createTempFile(prefix.toString(), ".dot", outDir);
				QueryPlanDotter.print(plan.getPlan(), outFile.getAbsolutePath());
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}
}
