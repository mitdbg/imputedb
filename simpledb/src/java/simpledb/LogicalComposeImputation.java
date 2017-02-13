package simpledb;

import java.util.*;

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
	private LogicalComposeImputation(TableStats tableStats, DbIterator physicalPlan, ImputedPlan subplan, Set<QualifiedName> dirtySet,
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

	public static ImputedPlan create(ImputedPlan subplan, ImputationType imp, Set<QualifiedName> impute, Map<String, Integer> tableMap) {
		assert !Collections.disjoint(subplan.getDirtySet(), impute); // No-op imputations aren't allowed.

		final Set<QualifiedName> dirtySet = new HashSet<>(subplan.getDirtySet()); // compute new dirty set
		dirtySet.removeAll(impute);

		final TupleDesc schema = subplan.getPlan().getTupleDesc();
		final double totalData = subplan.cardinality() * schema.numFields();
		final Collection<Integer> imputeIndices = schema.fieldNamesToIndices(impute);

		TableStats subplanTableStats = subplan.getTableStats(); // table stats for subplan

		switch (imp) {
		case DROP: {
			final DbIterator physicalPlan = new Drop(toNames(impute), subplan.getPlan());
			final double loss = 1.0;
			final double time = 0.0;
			final TableStats adjustedTableStats = subplanTableStats.adjustForImpute(DROP, imputeIndices);
			return new LogicalComposeImputation(adjustedTableStats, physicalPlan, subplan, dirtySet, loss, time);
		}
		case MAXIMAL:
		case MINIMAL: 
			Impute imputeOp = new ImputeRegressionTree(toNames(impute), subplan.getPlan());
			final DbIterator physicalPlan = imputeOp;
			final double loss = (1 / Math.sqrt(totalData));
			final int numComplete = schema.numFields() - dirtySet.size();
			final double time = imputeOp.getEstimatedCost(imputeIndices.size(), numComplete, (int) subplan.cardinality());
			final TableStats adjustedTableStats = subplanTableStats.adjustForImpute(MAXIMAL, imputeIndices);
			return new LogicalComposeImputation(adjustedTableStats, physicalPlan, subplan, dirtySet, loss, time);
		case NONE:
			throw new RuntimeException("NONE is no longer a valid ImputationType.");
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

	public Set<QualifiedName> getDirtySet() {
		return dirtySet;
	}
	
	@Override
	protected double loss() {
		return subplan.loss() + loss;
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
