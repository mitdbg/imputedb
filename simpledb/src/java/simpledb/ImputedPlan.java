package simpledb;

import java.util.Set;
import simpledb.Utility.AvgAgg;

/**
 * Generic type to extend for relations that have been imputed. We want to have a union of these types,
 * since imputation can happen at various steps along the tree, and this allows us to simplify how
 * we keep track.
 *
 * Each imputed plan must be able to report cost/cardinality, as these are directly influence by
 * what imputation was performed. Similarly, we need information on the dirty set. To get a physical implementation
 * we also have a getPlan, which saves us the trouble of replacing logical with physical nodes later on.
 *
 * Each ImputedPlan carries a copy of its table statistics. These are modified according to imputation/selection/joins.
 */
public abstract class ImputedPlan {
	protected abstract AvgAgg penalty();
	protected abstract double time();
	
	public abstract Set<QualifiedName> getDirtySet();
	public abstract DbIterator getPlan();
	public abstract double cardinality();
	public abstract TableStats getTableStats();

	public double cost(double lossWeight) {
		assert 0.0 <= lossWeight && lossWeight <= 1.0;
		return lossWeight * penalty().get() + (1 - lossWeight) * time();
	}

	public double getPenalty() {
		return penalty().get();
	}

	public double getTime() {
		return time();
	}
}
