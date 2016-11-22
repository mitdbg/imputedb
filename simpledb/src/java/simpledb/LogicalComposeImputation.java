package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Adding imputation on top of a plan that already has imputed values along its tree.
 * We need this to add drop, impute min, impute max to plans that already include joins.
 */
public class LogicalComposeImputation extends ImputedPlan {
  private double cardinality;
  private DbIterator physicalPlan;
  private Set<QuantifiedName> dirtySet;
  private double loss;
  private double time;

  private static final double LOSS_FACTOR = 1.01;

  public LogicalComposeImputation(ImputedPlan subplan, ImputationType imp, Set<QuantifiedName> required, Map<String, Integer> tableMap) {
    // which ones do we actually need to impute
    Set<QuantifiedName> impute = new HashSet<>();
    impute.addAll(subplan.getDirtySet());
    impute.retainAll(required);

    dirtySet = new HashSet<>();
    dirtySet.addAll(subplan.getDirtySet());

    double totalData = subplan.cardinality() * subplan.getPlan().getTupleDesc().numFields();

    switch(imp) {
      case DROP:
        physicalPlan = new Drop(subplan.getPlan(), toNames(impute));
        dirtySet.removeAll(impute);
        loss = estimateNumNulls(impute, subplan.cardinality(), tableMap);
        time = subplan.cardinality();
        // TODO: this needs to be changed as well, need to estimate rows with any null values
        cardinality = subplan.cardinality();
        break;
      case MINIMAL:
        physicalPlan = new ImputeRandom(subplan.getPlan(), toNames(impute));
        dirtySet.removeAll(impute);
        loss = estimateNumNulls(impute, subplan.cardinality(), tableMap) * Math.pow(LOSS_FACTOR, -totalData);
        time = totalData;
        cardinality = subplan.cardinality();
        break;
      case MAXIMAL:
        physicalPlan = new ImputeRandom(subplan.getPlan(), toNames(subplan.getDirtySet()));
        dirtySet = new HashSet<>();
        loss = estimateNumNulls(subplan.getDirtySet(), subplan.cardinality(), tableMap) * Math.pow(LOSS_FACTOR, -totalData);
        time = totalData;
        cardinality = subplan.cardinality();
        break;
      case NONE:
        if (!impute.isEmpty()) {
          throw new IllegalArgumentException("Must impute all dirty attributes required.");
        }
    }
  }

  public double estimateNumNulls(Set<QuantifiedName> attrs, double card, Map<String, Integer> tableMap) {
    // TODO: need a good way to estimate number of nulls at this point..
    return 1.0;
  }


  public DbIterator getPlan() {
    return physicalPlan;
  }

  public Set<QuantifiedName> getDirtySet() {
    return dirtySet;
  }

  public double cost(double lossWeight) {
    return lossWeight * loss + (1 - lossWeight) * time;
  }

  public double cardinality() {
    return cardinality;
  }

  private Set<String> toNames(Set<QuantifiedName> attrs) {
    Set<String> names = new HashSet<>();
    for(QuantifiedName attr : attrs) {
      names.add(attr.toString());
    }
    return names;
  }

}
