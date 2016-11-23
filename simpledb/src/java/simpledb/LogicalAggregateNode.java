package simpledb;

import java.util.*;

public class LogicalAggregateNode extends ImputedPlan {
	private final ImputedPlan plan;
	private final DbIterator physicalPlan;
	
	public LogicalAggregateNode(ImputedPlan subplan, ImputationType imp, QuantifiedName groupByField,
			Aggregator.Op aggOp, QuantifiedName aggField, Map<String, Integer> tableMap) {
		HashSet<QuantifiedName> required = new HashSet<>();
		required.add(groupByField);
		required.add(aggField);

		plan = LogicalComposeImputation.create(subplan, imp, required, tableMap);
		TupleDesc schema = plan.getPlan().getTupleDesc();
		physicalPlan = new Aggregate(plan.getPlan(), schema.fieldNameToIndex(aggField), schema.fieldNameToIndex(groupByField), aggOp);
	}

	@Override
	public Set<QuantifiedName> getDirtySet() {
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public DbIterator getPlan() {
		return physicalPlan;
	}

	@Override
	public double cost(double lossWeight) {
		// TODO: Add cost of aggregation?
		return plan.cost(lossWeight);
	}

	@Override
	public double cardinality() {
		throw new RuntimeException("Not implemented.");
	}
}