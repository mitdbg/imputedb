package simpledb;

import java.util.Set;

import simpledb.AImputedPlanCache.Value;

public interface ImputedPlanCache {
	Iterable<Value> bestPlans(Set<String> tables);

	Iterable<Value> bestPlans(Set<String> tables, Set<QualifiedName> dirty);

	void addPlan(Set<String> ts, ImputedPlan newPlanIP);

	void addJoinPlan(Set<String> ts, Set<LogicalJoinNode> joins, ImputedPlan newPlanIP);

	ImputedPlan getFinalPlan(double alpha, Set<String> tables);

	void setApproximate();
}