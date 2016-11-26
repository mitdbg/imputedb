package simpledb;

import java.util.*;
import java.util.Map.Entry;

/**
 * The JoinOptimizer class for the extend imputation functionality. This not
 * only picks order of joins, but also the imputation operators. Functionality
 * here is much more complicated than in the original simpledb, so we can no
 * longer just return a vector of joins to perform in a given order. Joins may
 * require imputation to take place. So the result of the optimizer is now a map
 * from possible sets of remaining dirty columns to the plan with the lowest
 * cost. This plan can retrieve the physical plan as necessary.
 */
public class ImputedLogicalPlan extends LogicalPlan {
	private final double lossWeight;

	/**
	 * Constructor
	 *
	 * @param p
	 *            the logical plan being optimized
	 * @param joins
	 *            the list of joins being performed
	 */
	public ImputedLogicalPlan(double lossWeight) {
		super();
		this.lossWeight = lossWeight;
	}

	/**
	 * Helper method to enumerate all of the subsets of a given size of a
	 * specified vector.
	 * 
	 * @param numElems
	 *            The size of the vector whose subsets are desired
	 * @param size
	 *            The size of the subsets of interest
	 * @return a set of all subsets of the specified size
	 */
	public static Iterable<BitSet> enumerateSubsets(int numElems, int size) {
		// Implemented as per Knuth, 7.2.1.3 pg. 4.
		int[] c = new int[size + 3];
		for (int j = 1; j <= size; j++) {
			c[j] = j - 1;
		}
		c[size + 1] = numElems;
		c[size + 2] = 0;

		return new Iterable<BitSet>() {
			boolean hasNext = true;

			@Override
			public Iterator<BitSet> iterator() {
				return new Iterator<BitSet>() {
					@Override
					public boolean hasNext() {
						return hasNext;
					}

					@Override
					public BitSet next() {
						if (!hasNext) {
							throw new NoSuchElementException();
						}

						BitSet ret = new BitSet(numElems);
						for (int j = 1; j <= size; j++) {
							ret.set(c[j]);
						}

						int j = 1;
						while (c[j] + 1 == c[j + 1]) {
							c[j] = j - 1;
							j++;
						}

						if (j > size) {
							hasNext = false;
						} else {
							c[j]++;
						}

						return ret;
					}
				};
			}
		};
	}
	
	private void optimizeFilters(TransactionId tid, ImputedPlanCache cache) throws ParsingException {			
		HashMap<String, Set<LogicalFilterNode>> filterMap = new HashMap<>();
		for (LogicalFilterNode filter : filters) {
			Set<LogicalFilterNode> accFilters = filterMap.get(filter.tableAlias);
			if (accFilters == null) {
				accFilters = new HashSet<LogicalFilterNode>();
			}
			accFilters.add(filter);
			filterMap.put(filter.tableAlias, accFilters);
		}

		for (LogicalScanNode scan : tables) {
			String tableAlias = scan.alias;
			Set<LogicalFilterNode> filters = filterMap.get(tableAlias);

			// TODO FIX: if table has no dirty columns, better to skip and just add None? as stands can end up with Drop(t, 0) etc
			ArrayList<LogicalAccessNode> candidates = new ArrayList<LogicalAccessNode>();
			candidates.add(new LogicalAccessNode(tid, scan, ImputationType.DROP, filters));
			candidates.add(new LogicalAccessNode(tid, scan, ImputationType.MINIMAL, filters));
			candidates.add(new LogicalAccessNode(tid, scan, ImputationType.MAXIMAL, filters));
			try {
				candidates.add(new LogicalAccessNode(tid, scan, ImputationType.NONE, filters));
			} catch (IllegalArgumentException e) {
			}

			/* Select the best candidate for each distinct dirty set. */
			for (LogicalAccessNode node : candidates) {
				HashSet<String> tables = new HashSet<>();
				tables.add(tableAlias);
				cache.addPlan(tables, node.getDirtySet(), node, lossWeight);
			}
		}
	}

	/**
	 * Compute best plans for each set of dirty columns once all joins have been
	 * performed. The plans already contain all joins in the order they need to
	 * be performed, and it suffices to get the physical plan directly from one
	 * of them.
	 * 
	 * @param imputedFilters
	 *            cache with best plans for each dirty set for filter
	 *            applications
	 * @return A Vector<LogicalJoinNode> that stores joins in the left-deep
	 *         order in which they should be executed.
	 * @throws ParsingException
	 *             when stats or filter selectivities is missing a table in the
	 *             join, or or when another internal error occurs
	 */
	private void optimizeJoins(TransactionId tid, ImputedPlanCache cache) throws ParsingException {
		if (joins.isEmpty()) {
			return;
		}

		// dynamic programming algo from selinger
		for (int i = 1; i <= joins.size(); i++) {
			for (BitSet s : enumerateSubsets(joins.size(), i)) {
				int j = s.nextSetBit(0);
				while (j != -1) {
					computePlan(tid, j, s, cache);
					j = s.nextSetBit(j + 1);
				}
			}
		}
	}

	/**
	 * This helper computes the set of best plans for joining `joinToRemove`
	 * from an existing set of joins. It adds imputation operators to each side
	 * of the join as necessary. To do so, it considers all imputations and
	 * resulting dirty sets.
	 * 
	 * @param joinToRemove
	 *            the join to remove from joinSet
	 * @param joinSet
	 *            the set of joins being considered
	 * @param pc
	 *            ImputedPlanCache, should have subplans for all joins of size
	 *            joinSet.size()-1
	 * @throws ParsingException
	 *             when stats, filterSelectivities, or pc object is missing
	 *             tables involved in join
	 */
	private void computePlan(TransactionId tid, int joinToRemove, BitSet joinSet, ImputedPlanCache pc) throws ParsingException {
		// join node from standard simpledb planner, not imputed, just used for
		// convenience
		LogicalJoinNode j = joins.get(joinToRemove);

		// check that the user provided aliases, as needed for all joins
		if (getTableId(j.t1Alias) == null)
			throw new ParsingException("Unknown table " + j.t1Alias);
		if (getTableId(j.t2Alias) == null)
			throw new ParsingException("Unknown table " + j.t2Alias);

		String table1Alias = j.t1Alias;
		String table2Alias = j.t2Alias;

		BitSet news = (BitSet) joinSet.clone();
		news.clear(joinToRemove);

		// possible plans to arrive at left and right sides of the join
		Map<Set<QuantifiedName>, ImputedPlan> leftPlans;
		Map<Set<QuantifiedName>, ImputedPlan> rightPlans;

		// attributes that must be imputed for join
		Set<QuantifiedName> required = new HashSet<>();
		required.add(new QuantifiedName(j.t1Alias, j.f1PureName));
		required.add(new QuantifiedName(j.t2Alias, j.f2PureName));

		// names of table in already performed join
		Set<String> leftTables = new HashSet<>();
		// alias for table to join in
		Set<String> rightTables = new HashSet<>();
		Set<String> allTables = new HashSet<>();

		// just joining existing base relations
		boolean isSimpleJoin;

		if ((isSimpleJoin = news.isEmpty())) {
			// base case -- both are base relations
			leftTables.add(table1Alias);
			rightTables.add(table2Alias);
			// this retrieves results from FilterOptimizer
			leftPlans = pc.getBestPlans(leftTables);
			rightPlans = pc.getBestPlans(rightTables);
		} else {
			// news is not empty -- figure best way to join j to news
			leftTables = getTableAliases(news);
			if (!leftTables.contains(table1Alias) && !leftTables.contains(table2Alias)) {
				// there is no join possible if existing tables joined and new
				// join don't share any data
				return;
			}

			if (leftTables.contains(table1Alias)) {
				rightTables.add(table2Alias);
			} else {
				rightTables.add(table1Alias);
			}

			leftPlans = pc.getBestPlans(leftTables);
			rightPlans = pc.getBestPlans(rightTables);
		}

		// possible plans with imputations for the left-hand side
		for (ImputedPlan lplan : leftPlans.values()) {
			// extended with any imputations necessary for join
			for (ImputedPlan lplanPrime : addImputes(lplan, required)) {
				// possible plans with imputations for right-hand side
				for (ImputedPlan rplan : rightPlans.values()) {
					// extended with any imputations necessary for join
					for (ImputedPlan rplanPrime : addImputes(rplan, required)) {
						// create new join
						LogicalImputedJoinNode joined = new LogicalImputedJoinNode(tid, table1Alias, table2Alias, lplanPrime,
								rplanPrime, j.f1QuantifiedName, j.f2QuantifiedName, j.p, tableMap);
						// add to cache as appropriate
						pc.addPlan(allTables, joined.getDirtySet(), joined, lossWeight);

						// if it was a simple join, consider swapping order
						if (isSimpleJoin) {
							pc.addPlan(allTables, joined.getDirtySet(), joined.swapInnerOuter(), lossWeight);
						}
					}
				}
			}
		}

		// TODO FIX: do we need to consider swapping left/right nodes to join when
		// not base relations? don't think so if just doing left-deep
		// TODO FIX: this doesn't currently consider cross products...
	}

	/**
	 * Create list of possible imputations on a given plan, provided a required
	 * set of attributes that must not be dirty.
	 * 
	 * @param plan
	 *            existing plan (may have some columns already imputed)
	 * @param required
	 *            attributes that must not be dirty upon return
	 * @return
	 */
	private List<ImputedPlan> addImputes(ImputedPlan plan, Set<QuantifiedName> required) {
		List<ImputedPlan> plans = new ArrayList<>();
		if (plan.getDirtySet().isEmpty()) {
			// nothing dirty, don't need to do anything
			plans.add(plan);
		} else {
			// we consider all possible imputation strategies and compose them
			// with the existing plan
			for (ImputationType imp : ImputationType.values()) {
				try {
					plans.add(LogicalComposeImputation.create(plan, imp, required, tableMap));
				} catch (IllegalArgumentException e) {
					// pass
				}
			}
		}
		return plans;
	}
	
	@Override
	public DbIterator physicalPlan(TransactionId tid, Map<String, TableStats> baseTableStats, boolean explain)
			throws ParsingException {
		final ImputedPlanCache cache = new ImputedPlanCache();
		optimizeFilters(tid, cache);
		optimizeJoins(tid, cache);
		
		final Set<String> allTables = new HashSet<>();
		for (LogicalScanNode scan : tables) {
			allTables.add(scan.alias);
		}
		final Map<Set<QuantifiedName>, ImputedPlan> bestPlans = cache.getBestPlans(allTables);

		// The a tuple description for the output of the join. (all the plans
		// should
		// have the same schema, so it doesn't matter which we use)
		if (bestPlans.size() == 0) {
			throw new RuntimeException("BUG: No plans available that cover all tables.");
		}
		TupleDesc td = bestPlans.values().iterator().next().getPlan().getTupleDesc();

		// walk the select list, to determine order in which to project output
		// fields
		ArrayList<Integer> outFields = new ArrayList<Integer>();
		ArrayList<Type> outTypes = new ArrayList<Type>();
		for (LogicalSelectListNode si : selectList) {
			if (si.aggOp != null) {
				outFields.add(groupByField != null ? 1 : 0);
				try {
					td.fieldNameToIndex(si.fname);
				} catch (NoSuchElementException e) {
					throw new ParsingException("Unknown field " + si.fname + " in SELECT list");
				}
				outTypes.add(Type.INT_TYPE); // the type of all aggregate
												// functions is INT
			} else if (aggField != null) {
				if (groupByField == null) {
					throw new ParsingException("Field " + si.fname + " does not appear in GROUP BY list");
				}
				outFields.add(0);
				int id;
				try {
					id = td.fieldNameToIndex(groupByField);
				} catch (NoSuchElementException e) {
					throw new ParsingException("Unknown field " + groupByField + " in GROUP BY statement");
				}
				outTypes.add(td.getFieldType(id));
			} else if (si.fname.equals("null.*")) {
				for (int i = 0; i < td.numFields(); i++) {
					outFields.add(i);
					outTypes.add(td.getFieldType(i));
				}
			} else {
				int id;
				try {
					id = td.fieldNameToIndex(si.fname);
				} catch (NoSuchElementException e) {
					throw new ParsingException("Unknown field " + si.fname + " in SELECT list");
				}
				outFields.add(id);
				outTypes.add(td.getFieldType(id));

			}
		}

		// Add an aggregation node if this plan has an aggregation.
		ImputedPlan bestPlan = null;
		if (aggField != null) {
			for (Entry<Set<QuantifiedName>, ImputedPlan> entry : bestPlans.entrySet()) {
				for (ImputationType imp : ImputationType.values()) {
					LogicalAggregateNode plan = new LogicalAggregateNode(entry.getValue(), imp, groupByField, aggOp, aggField, tableMap);
					if (bestPlan == null || plan.cost(lossWeight) < bestPlan.cost(lossWeight)) {
						bestPlan = plan;
					}
				}
			}
		} 
		
		// Otherwise, select the plan of lowest cost.
		else {
			for (Entry<Set<QuantifiedName>, ImputedPlan> entry : bestPlans.entrySet()) {
				ImputedPlan plan = entry.getValue();
				if (bestPlan == null || plan.cost(lossWeight) < bestPlan.cost(lossWeight)) {
					bestPlan = plan;
				}
			}
		}

		DbIterator physicalPlan = bestPlan.getPlan();
		if (oByField != null) {
			physicalPlan = new OrderBy(physicalPlan.getTupleDesc().fieldNameToIndex(oByField), oByAsc, physicalPlan);
		}

		return new Project(outFields, outTypes, physicalPlan);
	}

	/**
	 * Retrieve table alias names given a bit set for joins being considered
	 * 
	 * @param ts
	 *            bit set for joins from which to extract left and right aliases
	 * @return
	 */
	private Set<String> getTableAliases(BitSet ts) {
		Set<String> aliases = new HashSet<>();
		for (int i = 0; i < ts.size(); i++) {
			if (ts.get(i)) {
				LogicalJoinNode j = joins.get(i);
				aliases.add(j.t1Alias);
				aliases.add(j.t2Alias);
			}
		}
		return aliases;
	}
}
