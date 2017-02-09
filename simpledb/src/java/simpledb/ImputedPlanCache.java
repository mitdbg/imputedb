package simpledb;
import java.util.*;
import java.util.Map.Entry;

/**
 * Keeps information on the best plan that includes a given set of tables and has a given set of dirty attributes
 * Insertions into the cache are handled directly to only overwrite if of lower cost.
 */
public class ImputedPlanCache {	
    private class ImputeKey {
        public final Set<String> tables;
        public final Set<QualifiedName> dirtySet;
        
        public ImputeKey(Set<String> tables, Set<QualifiedName> dirtySet) {
            this.tables = tables;
            this.dirtySet = dirtySet;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (!(o instanceof ImputeKey)) {
                return false;
            }

            ImputeKey ok = (ImputeKey) o;
            return tables.equals(ok.tables) && dirtySet.equals(ok.dirtySet);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + tables.hashCode();
            result = prime * result + dirtySet.hashCode();
            return result;
        }

        @Override
        public String toString() {
            String tableStr = "Tables(" + Arrays.toString(this.tables.toArray()) + ")";
            String dirtyStr = "Dirty(" + Arrays.toString(this.dirtySet.toArray()) + ")";
            return tableStr + "," + dirtyStr;
        }
    }

    private final Map<ImputeKey, ImputedPlan> bestPlans;
    // joins necessary to get to this state
    private final Map<ImputeKey, Set<LogicalJoinNode>> necessaryJoins;
    
    public ImputedPlanCache() {
        bestPlans = new HashMap<ImputeKey, ImputedPlan>();
        necessaryJoins = new HashMap<ImputeKey, Set<LogicalJoinNode>>();
    }

    /**
     * Add a plan if it is of lower cost for the appropriate (tables, dirty set) key
     * @param ts tables in the underlying plan
     * @param dirtySet attributes that are still dirty in the underlying plan
     * @param newPlan plan
     * @param lossWeight weight loss used to estimate costs
     */
    void addPlan(Set<String> ts, Set<QualifiedName> dirtySet, ImputedPlan newPlan, double lossWeight) {
        ImputeKey key = new ImputeKey(ts, dirtySet);
        if (!bestPlans.containsKey(key)) {
            // always insert if we don't have any info on this key combo
            bestPlans.put(key, newPlan);
            necessaryJoins.put(key, new HashSet<>());
        } else {
            ImputedPlan currentBest = bestPlans.get(key);
            // otherwise insert if the new plan is less costly
            if (currentBest.cost(lossWeight) > newPlan.cost(lossWeight)) {
                bestPlans.put(key, newPlan);
                // do nothing for necessary joins
            }
        }
    }

    /**
     * Add a plan involving joins if it is of lower cost for the appropriate (tables, dirty set) key
     * @param ts tables in the underlying plan
     * @param dirtySet attributes that are still dirty in the underlying plan
     * @param joins join predicates involved in the plan
     * @param newPlan plan
     * @param lossWeight weight loss used to estimate costs
     */
    void addJoinPlan(Set<String> ts, Set<QualifiedName> dirtySet, Set<LogicalJoinNode> joins, ImputedPlan newPlan, double lossWeight) {
        ImputeKey key = new ImputeKey(ts, dirtySet);
        if (!bestPlans.containsKey(key)) {
            // always insert if we don't have any info on this key combo
            bestPlans.put(key, newPlan);
            necessaryJoins.put(key, joins);
        } else {
            ImputedPlan currentBest = bestPlans.get(key);
            // otherwise insert if the new plan is less costly
            if (currentBest.cost(lossWeight) > newPlan.cost(lossWeight)) {
                bestPlans.put(key, newPlan);
                necessaryJoins.put(key, joins);
            }
        }
    }

    /**
     * Return set of join predicates used in a plan
     * @param ts tables in plan
     * @param dirtySet dirty set of resulting plan
     * @return
     */
    Set<LogicalJoinNode> getNecessaryJoins(Set<String> ts, Set<QualifiedName> dirtySet) {
        Set<LogicalJoinNode> set = new HashSet<>();
        Set<LogicalJoinNode> found = necessaryJoins.get(new ImputeKey(ts, dirtySet));
        if (found != null) {
            set.addAll(found);
        }
        return set;
    }
    
    /** Find the best plan that contains a set of tables and ends up with a set of dirty attributes
        @param ts table names
        @param ds dirty attributes
        @return the best order for s in the cache
    */
    ImputedPlan getBestPlan(Set<String> ts, Set<QualifiedName> ds) {
        return bestPlans.get(new ImputeKey(ts, ds));
    }

    Map<Set<QualifiedName>, ImputedPlan> getBestPlans(Set<String> ts) {
        Map<Set<QualifiedName>, ImputedPlan> best = new HashMap<Set<QualifiedName>, ImputedPlan>();
        for (ImputeKey key : bestPlans.keySet()) {
            if (key.tables.equals(ts)) {
                best.put(key.dirtySet, bestPlans.get(key));
            }
        }
        return best;
    }
    
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("{ ");
    	for (Entry<ImputeKey, ImputedPlan> entry : bestPlans.entrySet()) {
    		sb.append(entry.getKey().toString() + " : " + entry.getValue().toString() + ", ");
    	}
    	sb.append("}");
    	return sb.toString();
    }
}
