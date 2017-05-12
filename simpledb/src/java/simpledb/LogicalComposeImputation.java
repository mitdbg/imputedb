package simpledb;

import java.util.*;

import simpledb.Utility.AvgAgg;

import static simpledb.ImputationType.DROP;
import static simpledb.ImputationType.MAXIMAL;

/**
 * Adding imputation on top of a plan that already has imputed values along its
 * tree. We need this to add drop, impute min, impute max to plans that already
 * include joins.
 */
public class LogicalComposeImputation extends ImputedPlan {
	private final DbIterator physicalPlan;
	private final ImputedPlan subplan;
	private final Set<QualifiedName> dirtySet;
	private final double penalty;
	private final double time;
	private TableStats tableStats;

	/**
	 * Constructor intended only for internal construction of updated plan
	 * @param tableStats
	 * @param physicalPlan
	 * @param dirtySet
	 * @param penalty
	 * @param time
	 */
	private LogicalComposeImputation(TableStats tableStats, DbIterator physicalPlan, ImputedPlan subplan, Set<QualifiedName> dirtySet,
			double penalty, double time) {
		super();
		this.physicalPlan = physicalPlan;
		this.dirtySet = dirtySet;
		this.penalty = penalty;
		this.time = time;
		this.tableStats = tableStats;
		this.subplan = subplan;
	}

	public TableStats getTableStats() {
		return this.tableStats;
	}

	public static ImputedPlan create(ImputedPlan subplan, ImputationType imp, Set<QualifiedName> impute, Map<String, Integer> tableMap) {
		assert !Collections.disjoint(subplan.getDirtySet(), impute); // No-op imputations aren't allowed.

		final Set<QualifiedName> dirtySet = new HashSet<>(subplan.getDirtySet()); // compute new dirty set
		dirtySet.removeAll(impute);

		final TupleDesc schema = subplan.getPlan().getTupleDesc();
		final Collection<Integer> imputeIndices = schema.fieldNamesToIndices(impute);

		TableStats subplanTableStats = subplan.getTableStats(); // table stats for subplan

		switch (imp) {
		case DROP: {
			Impute dropOp = new Drop(toNames(impute), subplan.getPlan());
			final double penalty = dropOp.getEstimatedPenalty(subplan);
			final double time = dropOp.getEstimatedTime(subplan);
			final TableStats adjustedTableStats = subplanTableStats.adjustForImpute(DROP, imputeIndices);
			return new LogicalComposeImputation(adjustedTableStats, dropOp, subplan, dirtySet, penalty, time);
		}
		case MAXIMAL:
		case MINIMAL: 
			Impute imputeOp = ImputeFactory.newImpute(toNames(impute), subplan.getPlan());
			// Impute imputeOp = new ImputeRegressionTree(toNames(impute), subplan.getPlan());
			final double penalty = imputeOp.getEstimatedPenalty(subplan);
			final double time = imputeOp.getEstimatedTime(subplan);
			final TableStats adjustedTableStats = subplanTableStats.adjustForImpute(MAXIMAL, imputeIndices);
			return new LogicalComposeImputation(adjustedTableStats, imputeOp, subplan, dirtySet, penalty, time);
		case NONE:
			throw new RuntimeException("NONE is no longer a valid ImputationType.");
		default:
			throw new RuntimeException("Unexpected ImputationType.");
		}
	}

	public DbIterator getPlan() {
		return physicalPlan;
	}

	public Set<QualifiedName> getDirtySet() {
		return dirtySet;
	}
	
	@Override
	protected AvgAgg penalty() {
		return subplan.penalty().add(penalty);
	}
	
	@Override
	protected double time() {
		return subplan.time() + time;
	}

	public double cardinality() {
		return tableStats.totalTuples();
	}

	private static Set<String> toNames(Set<QualifiedName> attrs) {
		Set<String> names = new HashSet<>();
		for (QualifiedName attr : attrs) {
			names.add(attr.toString());
		}
		return names;
	}

}
