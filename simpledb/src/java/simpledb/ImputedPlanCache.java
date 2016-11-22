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
        public final Set<QuantifiedName> dirtySet;
        
        public ImputeKey(Set<String> tables, Set<QuantifiedName> dirtySet) {
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
    }

    private final Map<ImputeKey, ImputedPlan> bestPlans;
    
    public ImputedPlanCache() {
        bestPlans = new HashMap<ImputeKey, ImputedPlan>();
    }

    /**
     * Add a plan if it is of lower cost for the appropriate (tables, dirty set) key
     * @param ts tables in the underlying plan
     * @param dirtySet attributes that are still dirty in the underlying plan
     * @param newPlan plan
     * @param lossWeight weight loss used to estimate costs
     */
    void addPlan(Set<String> ts, Set<QuantifiedName> dirtySet, ImputedPlan newPlan, double lossWeight) {
        ImputeKey key = new ImputeKey(ts, dirtySet);
        if (!bestPlans.containsKey(key)) {
            // always insert if we don't have any info on this key combo
            bestPlans.put(key, newPlan);
        } else {
            ImputedPlan currentBest = bestPlans.get(key);
            // otherwise insert if the new plan is less costly
            if (currentBest.cost(lossWeight) > newPlan.cost(lossWeight)) {
                bestPlans.put(key, newPlan);
            }
        }
    }

    /**
     * Add all entries in an existing ImputedPlanCache
     * @param pc existing cache
     * @param lossWeight
     */
    void addAll(ImputedPlanCache pc, double lossWeight) {
        for (Entry<ImputeKey, ImputedPlan> entry : pc.bestPlans.entrySet()) {
            Set<String> ts = entry.getKey().tables;
            Set<QuantifiedName> ds = entry.getKey().dirtySet;
            addPlan(ts, ds, entry.getValue(), lossWeight);
        }
    }
    
    /** Find the best plan that contains a set of tables and ends up with a set of dirty attributes
        @param ts table names
        @param ds dirty attributes
        @return the best order for s in the cache
    */
    ImputedPlan getBestPlan(Set<String> ts, Set<QuantifiedName> ds) {
        return bestPlans.get(new ImputeKey(ts, ds));
    }

    Map<Set<QuantifiedName>, ImputedPlan> getBestPlans(Set<String> ts) {
        Map<Set<QuantifiedName>, ImputedPlan> best = new HashMap<Set<QuantifiedName>, ImputedPlan>();
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
