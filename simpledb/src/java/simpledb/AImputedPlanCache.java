package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public abstract class AImputedPlanCache implements ImputedPlanCache {
	protected static class Key {
		public final Set<String> tables;
		public final Set<QualifiedName> dirtySet;

		public Key(Set<String> tables, Set<QualifiedName> dirtySet) {
			this.tables = tables;
			this.dirtySet = dirtySet;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null) {
				return false;
			}
			if (!(o instanceof Key)) {
				return false;
			}

			Key ok = (Key) o;
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

	public static class Value implements Comparable<Value> {
		public final ImputedPlan plan;
		public final Set<LogicalJoinNode> joins;

		public Value(ImputedPlan plan, Set<LogicalJoinNode> joins) {
			this.plan = plan;
			this.joins = joins;
		}

		public Value(ImputedPlan plan) {
			this(plan, new HashSet<LogicalJoinNode>());
		}

		public boolean dominates(Value o) {
			return plan.loss().get() <= o.plan.loss().get() && plan.time() <= o.plan.time();
		}

		@Override
		public int compareTo(Value o) {
			int lcmp = Double.compare(plan.loss().get(), o.plan.loss().get());
			if (lcmp != 0) { return lcmp; }
			return Double.compare(plan.time(), o.plan.time());
		}
	}

	protected final HashMap<Key, TreeSet<Value>> bestPlans;

	public AImputedPlanCache() {
		bestPlans = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see simpledb.IImputedPlanCache#bestPlans(java.util.Set)
	 */
	@Override
	public Iterable<Value> bestPlans(Set<String> tables) {
		Value[] vals = bestPlans.keySet().stream()
				.filter(key -> key.tables.equals(tables))
				.flatMap(key -> bestPlans.get(key).stream())
				.toArray(n -> new Value[n]);
		return () -> Arrays.stream(vals).iterator();
	}

	/* (non-Javadoc)
	 * @see simpledb.IImputedPlanCache#bestPlans(java.util.Set, java.util.Set)
	 */
	@Override
	public Iterable<Value> bestPlans(Set<String> tables, Set<QualifiedName> dirty) {
		Value[] vals = bestPlans.get(new Key(tables, dirty)).toArray(new Value[] {});
		return () -> Arrays.stream(vals).iterator();
	}
	
	/* (non-Javadoc)
	 * @see simpledb.IImputedPlanCache#addPlan(java.util.Set, simpledb.ImputedPlan)
	 */
	@Override
	public abstract void addPlan(Set<String> ts, ImputedPlan newPlanIP);
	/* (non-Javadoc)
	 * @see simpledb.IImputedPlanCache#addJoinPlan(java.util.Set, java.util.Set, simpledb.ImputedPlan)
	 */
	@Override
	public abstract void addJoinPlan(Set<String> ts, Set<LogicalJoinNode> joins, ImputedPlan newPlanIP);

	@Override
	public String toString() {
		/*StringBuilder sb = new StringBuilder();
		sb.append("{ ");
		for (Entry<Key, ImputedPlan> entry : bestPlans.entrySet()) {
			sb.append(entry.getKey().toString() + " : " + entry.getValue().toString() + ", ");
		}
		sb.append("}");
		return sb.toString();*/
		throw new RuntimeException();
	}

}