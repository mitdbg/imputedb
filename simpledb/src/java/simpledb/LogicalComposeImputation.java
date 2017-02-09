package simpledb;

import java.util.*;

import static simpledb.ImputationType.DROP;
import static simpledb.ImputationType.MAXIMAL;
import static simpledb.ImputationType.MINIMAL;

/**
 * Adding imputation on top of a plan that already has imputed values along its
 * tree. We need this to add drop, impute min, impute max to plans that already
 * include joins.
 */
public class LogicalComposeImputation extends ImputedPlan {
	private final DbIterator physicalPlan;
	private final ImputedPlan subplan;
	private final Set<QuantifiedName> dirtySet;
	private final double loss;
	private final double time;
	private TableStats tableStats;

	/**
	 * Constructor intended only for internal construction of updated plan
	 * @param tableStats
	 * @param physicalPlan
	 * @param dirtySet
	 * @param loss
	 * @param time
	 */
	private LogicalComposeImputation(TableStats tableStats, DbIterator physicalPlan, ImputedPlan subplan, Set<QuantifiedName> dirtySet,
			double loss, double time) {
		super();
		this.physicalPlan = physicalPlan;
		this.dirtySet = dirtySet;
		this.loss = loss;
		this.time = time;
		this.tableStats = tableStats;
		this.subplan = subplan;
	}

	public TableStats getTableStats() {
		return this.tableStats;
	}

	public static ImputedPlan create(ImputedPlan subplan, ImputationType imp, Set<QuantifiedName> required,
			Map<String, Integer> tableMap) throws BadImputation {
		// don't change anything if there are no dirty columns
		if (subplan.getDirtySet().isEmpty()) {
			return subplan;
		}

		if (Collections.disjoint(subplan.getDirtySet(), required) && (imp == MINIMAL || imp == DROP)) {
			return subplan;
		}

		// which ones do we actually need to impute
		Set<QuantifiedName> impute = new HashSet<>();
		impute.addAll(subplan.getDirtySet());
		impute.retainAll(required);

		final Set<QuantifiedName> dirtySet = new HashSet<>();
		dirtySet.addAll(subplan.getDirtySet());

		TupleDesc schema = subplan.getPlan().getTupleDesc();
		double totalData = subplan.cardinality() * schema.numFields();

		DbIterator physicalPlan;
		double loss, time;
		// table stats for subplan
		TableStats subplanTableStats = subplan.getTableStats();
		// holds tablestats adjusted for imputation
		TableStats adjustedTableStats;

		switch (imp) {
		case DROP: {
			Collection<Integer> imputeIndices = schema.fieldNamesToIndices(impute);
			physicalPlan = new Drop(toNames(impute), subplan.getPlan());
			dirtySet.removeAll(impute);
			loss = estimateNumNulls(subplan, imputeIndices);
			time = subplan.cardinality();
			adjustedTableStats = subplanTableStats.adjustForImpute(DROP, imputeIndices);
			return new LogicalComposeImputation(adjustedTableStats, physicalPlan, subplan, dirtySet, loss, time);
		}
		case MINIMAL: {
			Collection<Integer> imputeIndices = schema.fieldNamesToIndices(impute);
			Impute imputeOp = new ImputeRegressionTree(toNames(impute), subplan.getPlan());
			physicalPlan = imputeOp;
			dirtySet.removeAll(impute);
			loss = estimateNumNulls(subplan, imputeIndices) * (1 / Math.sqrt(totalData));
			int numComplete = schema.numFields() - dirtySet.size();
			time = imputeOp.getEstimatedCost(imputeIndices.size(), numComplete, (int) subplan.cardinality());
			adjustedTableStats = subplanTableStats.adjustForImpute(MINIMAL, imputeIndices);
			return new LogicalComposeImputation(adjustedTableStats, physicalPlan, subplan, dirtySet, loss, time);
		}
		case MAXIMAL: {
			Set<QuantifiedName> i = subplan.getDirtySet();
			Collection<Integer> imputeIndices = schema.fieldNamesToIndices(i);
			Impute imputeOp = new ImputeRegressionTree(toNames(i), subplan.getPlan());
			physicalPlan = imputeOp;
			dirtySet.clear();
			loss = estimateNumNulls(subplan, imputeIndices) * (1 / Math.sqrt(totalData));
			int numComplete = schema.numFields() - dirtySet.size();
			time = imputeOp.getEstimatedCost(imputeIndices.size(), numComplete, (int) subplan.cardinality());
			adjustedTableStats = subplanTableStats.adjustForImpute(MAXIMAL, imputeIndices);
			return new LogicalComposeImputation(adjustedTableStats, physicalPlan, subplan, dirtySet, loss, time);
		}
		case NONE:
			if (impute.isEmpty()) {
				return subplan;
			}
			throw new BadImputation();
		default:
			throw new RuntimeException("Unexpected ImputationType.");
		}
	}

	public static double estimateNumNulls(ImputedPlan subplan, Collection<Integer> indices) {
		return subplan.getTableStats().estimateTotalNull(indices);
	}

	public DbIterator getPlan() {
		return physicalPlan;
	}

	public Set<QuantifiedName> getDirtySet() {
		return dirtySet;
	}

	public double cost(double lossWeight) {
		return lossWeight * loss + (1 - lossWeight) * time + subplan.cost(lossWeight);
	}

	public double cardinality() {
		return tableStats.totalTuples();
	}

	private static Set<String> toNames(Set<QuantifiedName> attrs) {
		Set<String> names = new HashSet<>();
		for (QuantifiedName attr : attrs) {
			names.add(attr.toString());
		}
		return names;
	}

}
