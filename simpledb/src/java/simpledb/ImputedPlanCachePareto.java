package simpledb;
import java.util.*;

/**
 * Keeps information on the best plan that includes a given set of tables and has a given set of dirty attributes
 * Insertions into the cache are handled directly to only overwrite if of lower cost.
 */
public class ImputedPlanCachePareto extends AImputedPlanCache {	
    public ImputedPlanCachePareto() {
        super();
    }

    /**
     * Add a plan if it is of lower cost for the appropriate (tables, dirty set) key
     * @param ts tables in the underlying plan
     * @param newPlan plan
     */
    public void addPlan(Set<String> ts, ImputedPlan newPlanIP) {
        addJoinPlan(ts, new HashSet<LogicalJoinNode>(), newPlanIP);
    }

    /**
     * Add a plan involving joins if it is of lower cost for the appropriate (tables, dirty set) key
     * @param ts tables in the underlying plan
     * @param joins join predicates involved in the plan
     * @param newPlan plan
     */
    public void addJoinPlan(Set<String> ts, Set<LogicalJoinNode> joins, ImputedPlan newPlanIP) {
    	Key key = new Key(ts, newPlanIP.getDirtySet());
        Value newPlan = new Value(newPlanIP, joins);
        if (bestPlans.containsKey(key)) {
        	SortedSet<Value> plans = bestPlans.get(key);
        	
        	// Find dominating plans.
        	for (Value plan : plans) {
        		// Found a dominating plan.
        		if (plan.dominates(newPlan)) { return; }
        	}
        	
        	// Remove dominated plans.
        	plans.removeIf(plan -> newPlan.dominates(plan));

        	plans.add(newPlan);
        } else {
        	// always insert if we don't have any info on this key combo
        	TreeSet<Value> plans = new TreeSet<>();
        	plans.add(newPlan);
            bestPlans.put(key, plans);
        }
    }
}
