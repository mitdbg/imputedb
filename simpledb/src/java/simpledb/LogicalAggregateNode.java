package simpledb;

import java.util.*;

public class LogicalAggregateNode extends ImputedPlan {
	private final ImputedPlan plan;
	private final DbIterator physicalPlan;
	
	public LogicalAggregateNode(ImputedPlan subplan, ImputationType imp, QuantifiedName groupByField,
			Aggregator.Op aggOp, QuantifiedName aggField, Map<String, Integer> tableMap) throws BadImputation {
		HashSet<QuantifiedName> required = new HashSet<>();

		// group-by keys needs only to be imputed when its actually used
		// e.g. select avg(c1) from t doesn't have a group-by key to impute
		if (groupByField != null) {
			required.add(groupByField);
		}
		required.add(aggField);

		plan = LogicalComposeImputation.create(subplan, imp, required, tableMap);
		TupleDesc schema = plan.getPlan().getTupleDesc();
		// indices
		int groupByKeyIndex = Aggregator.NO_GROUPING;
		int aggFieldIndex = schema.fieldNameToIndex(aggField);

		if (groupByField != null) {
			groupByKeyIndex = schema.fieldNameToIndex(groupByField);
		}
		physicalPlan = new Aggregate(plan.getPlan(), aggFieldIndex, groupByKeyIndex, aggOp);
	}

	public TableStats getTableStats() {
		throw new UnsupportedOperationException("table stats not supported on aggregated results");
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
		// TODO FIX: Add cost of aggregation?
		return plan.cost(lossWeight);
	}

	@Override
	public double cardinality() {
		throw new RuntimeException("Not implemented.");
	}
}