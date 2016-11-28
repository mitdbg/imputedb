package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Adding imputation on top of a plan that already has imputed values along its
 * tree. We need this to add drop, impute min, impute max to plans that already
 * include joins.
 */
public class LogicalComposeImputation extends ImputedPlan {
	private static final double LOSS_FACTOR = 1.01;

	private final double cardinality;
	private final DbIterator physicalPlan;
	private final Set<QuantifiedName> dirtySet;
	private final double loss;
	private final double time;

	private LogicalComposeImputation(double cardinality, DbIterator physicalPlan, Set<QuantifiedName> dirtySet,
			double loss, double time) {
		super();
		this.cardinality = cardinality;
		this.physicalPlan = physicalPlan;
		this.dirtySet = dirtySet;
		this.loss = loss;
		this.time = time;
	}

	public static ImputedPlan create(ImputedPlan subplan, ImputationType imp, Set<QuantifiedName> required,
			Map<String, Integer> tableMap) throws BadImputation {
		// don't add anything if there are no dirty columns
		if (subplan.getDirtySet().isEmpty()) {
			return subplan;
		}

		// which ones do we actually need to impute
		Set<QuantifiedName> impute = new HashSet<>();
		impute.addAll(subplan.getDirtySet());
		impute.retainAll(required);

		final Set<QuantifiedName> dirtySet = new HashSet<>();
		dirtySet.addAll(subplan.getDirtySet());

		double totalData = subplan.cardinality() * subplan.getPlan().getTupleDesc().numFields();

		DbIterator physicalPlan;
		double loss, time, cardinality;
		switch (imp) {
		case DROP:
			physicalPlan = new Drop(subplan.getPlan(), toNames(impute));
			dirtySet.removeAll(impute);
			loss = estimateNumNulls(impute, subplan.cardinality(), tableMap);
			time = subplan.cardinality();
			// TODO FIX: this needs to be changed as well, need to estimate rows
			// with any null values
			cardinality = subplan.cardinality();
			return new LogicalComposeImputation(cardinality, physicalPlan, dirtySet, loss, time);
		case MINIMAL:
			physicalPlan = new ImputeRandom(subplan.getPlan(), toNames(impute));
			dirtySet.removeAll(impute);
			loss = estimateNumNulls(impute, subplan.cardinality(), tableMap) * Math.pow(LOSS_FACTOR, -totalData);
			time = totalData;
			cardinality = subplan.cardinality();
			return new LogicalComposeImputation(cardinality, physicalPlan, dirtySet, loss, time);
		case MAXIMAL:
			physicalPlan = new ImputeRandom(subplan.getPlan(), toNames(subplan.getDirtySet()));
			dirtySet.clear();
			loss = estimateNumNulls(subplan.getDirtySet(), subplan.cardinality(), tableMap)
					* Math.pow(LOSS_FACTOR, -totalData);
			time = totalData;
			cardinality = subplan.cardinality();
			return new LogicalComposeImputation(cardinality, physicalPlan, dirtySet, loss, time);
		case NONE:
			if (impute.isEmpty()) {
				return subplan;
			}
			throw new BadImputation();
		default:
			throw new RuntimeException("Unexpected ImputationType.");
		}
	}

	public static double estimateNumNulls(Set<QuantifiedName> attrs, double card, Map<String, Integer> tableMap) {
		// TODO FIX: need a good way to estimate number of nulls at this point..
		return 1.0;
	}

	public DbIterator getPlan() {
		return physicalPlan;
	}

	public Set<QuantifiedName> getDirtySet() {
		return dirtySet;
	}

	public double cost(double lossWeight) {
		return lossWeight * loss + (1 - lossWeight) * time;
	}

	public double cardinality() {
		return cardinality;
	}

	private static Set<String> toNames(Set<QuantifiedName> attrs) {
		Set<String> names = new HashSet<>();
		for (QuantifiedName attr : attrs) {
			names.add(attr.toString());
		}
		return names;
	}

}
