package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class ImputedPlanCacheSingle extends AImputedPlanCache {
	private final double lossWeight;
	
	public ImputedPlanCacheSingle(double lossWeight) {
		this.lossWeight = lossWeight;
	}
	
	@Override
	public void addPlan(Set<String> ts, ImputedPlan newPlanIP) {
		addJoinPlan(ts, new HashSet<LogicalJoinNode>(), newPlanIP);
	}

	@Override
	public void addJoinPlan(Set<String> ts, Set<LogicalJoinNode> joins, ImputedPlan newPlanIP) {
		Key key = new Key(ts, newPlanIP.getDirtySet());
        Value newPlan = new Value(newPlanIP, joins);
        if (bestPlans.containsKey(key)) {
        	SortedSet<Value> plans = bestPlans.get(key);
        	assert plans.size() == 1;
        	
        	if (plans.first().plan.cost(lossWeight) > newPlan.plan.cost(lossWeight)) {
        		plans.clear();
        		plans.add(newPlan);
        	}
        } else {
        	// always insert if we don't have any info on this key combo
        	TreeSet<Value> plans = new TreeSet<>();
        	plans.add(newPlan);
            bestPlans.put(key, plans);
        }
	}

	public ImputedPlan getFinalPlan(double alpha, Set<String> tables) {
		throw new UnsupportedOperationException("This class should not be used, use pareto");
	}
}
