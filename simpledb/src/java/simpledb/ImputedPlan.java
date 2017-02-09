package simpledb;

import java.util.Set;

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
  public abstract Set<QualifiedName> getDirtySet();
  public abstract DbIterator getPlan();
  public abstract double cost(double lossWeight);
  public abstract double cardinality();
  public abstract TableStats getTableStats();
}
