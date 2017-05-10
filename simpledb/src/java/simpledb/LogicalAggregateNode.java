package simpledb;

import java.util.*;

import simpledb.Utility.AvgAgg;

public class LogicalAggregateNode extends ImputedPlan {
	private final ImputedPlan plan;
	private final DbIterator physicalPlan;
	
	public LogicalAggregateNode(ImputedPlan subplan, QualifiedName groupByField, Aggregator.Op aggOp, QualifiedName aggField) {
		plan = subplan;
		TupleDesc schema = subplan.getPlan().getTupleDesc();
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
	public Set<QualifiedName> getDirtySet() {
		return Collections.emptySet();
	}

	@Override
	public DbIterator getPlan() {
		return physicalPlan;
	}

	@Override
	protected AvgAgg penalty() {
		return plan.penalty();
	}
	
	@Override
	protected double time() {
		return plan.time();
	}

	@Override
	public double cardinality() {
		throw new UnsupportedOperationException();
	}
}