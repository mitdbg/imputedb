package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

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
	private final boolean imputeAtBaseTable;

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
		this.imputeAtBaseTable = false;
	}

	public ImputedLogicalPlan(double lossWeight, boolean imputeAtBaseTable) {
		super();
		this.lossWeight = lossWeight;
		this.imputeAtBaseTable = imputeAtBaseTable;
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

	private ImputedPlan addImputeAtBase(ImputedPlan scanPlan) {
		HashSet<QualifiedName> allDirty = new HashSet<QualifiedName>();
		allDirty.addAll(scanPlan.getDirtySet());
		return LogicalComposeImputation.create(scanPlan, ImputationType.MAXIMAL, allDirty, tableMap);
	}

	private void optimizeFilters(TransactionId tid, ImputedPlanCache cache, Set<QualifiedName> globalRequired) throws ParsingException {
		// Accumulate all filters on each table.
		HashMap<String, Set<LogicalFilterNode>> filterMap = new HashMap<>();
		for (LogicalFilterNode filter : filters) {
			Set<LogicalFilterNode> accFilters = filterMap.get(filter.tableAlias);
			if (accFilters == null) {
				accFilters = new HashSet<LogicalFilterNode>();
			}
			accFilters.add(filter);
			filterMap.put(filter.tableAlias, accFilters);
		}

		// Create an access node for each table to be scanned/filtered.
		for (LogicalScanNode scan : tables) {
			ImputedPlan scanPlan = new LogicalImputedScanNode(tid, scan);
			
			HashSet<String> tablesInPlan = new HashSet<>();
			tablesInPlan.add(scan.alias);

			Set<LogicalFilterNode> filters = filterMap.get(scan.alias);

			if (this.imputeAtBaseTable) {
				// if we're imputing at the base table
				// we should just add the one main impute, which takes care of everything
				scanPlan = addImputeAtBase(scanPlan);
			}
			
			// If there's no filters, add the scan plan as is.
			if (filters == null) { 
				cache.addPlan(tablesInPlan, scanPlan);
			} 
			
			// Otherwise, impute as needed and add on the required filters.
			else {
				/* Get the required set of the predicate. */
				HashSet<QualifiedName> localRequired = new HashSet<QualifiedName>();
				for(LogicalFilterNode filter : filters) {
					localRequired.add(new QualifiedName(filter.tableAlias, filter.fieldPureName));
				}
				
				List<ImputedPlan> imputedScans = addImputes(scanPlan, localRequired, globalRequired);
				
				for (ImputedPlan imputedScan : imputedScans) {
					ImputedPlan filterPlan = new LogicalImputedFilterNode(tid, imputedScan, filters);
					cache.addPlan(tablesInPlan, filterPlan);
				}
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
	private void optimizeJoins(TransactionId tid, ImputedPlanCache cache, Set<QualifiedName> globalRequired) throws ParsingException {
		if (joins.isEmpty()) {
			return;
		}

		// dynamic programming algo from selinger
		for (int i = 1; i <= joins.size(); i++) {
			for (BitSet s : enumerateSubsets(joins.size(), i)) {
				int j = s.nextSetBit(0);
				while (j != -1) {
					computePlan(tid, j, s, cache, globalRequired);
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
	private void computePlan(TransactionId tid, int joinToRemove, BitSet joinSet, ImputedPlanCache pc, Set<QualifiedName> globalRequired) throws ParsingException {
		// join node from standard simpledb planner, not imputed, just used for
		// convenience
		LogicalJoinNode j = joins.get(joinToRemove);

		// check that the user provided aliases, as needed for all joins
		if (getTableId(j.t1Alias) == null)
			throw new ParsingException("Unknown table " + j.t1Alias);
		if (getTableId(j.t2Alias) == null)
			throw new ParsingException("Unknown table " + j.t2Alias);

		BitSet news = (BitSet) joinSet.clone();
		news.clear(joinToRemove);

		// possible plans to arrive at left and right sides of the join
		Iterable<ImputedPlanCachePareto.Value> leftPlans, rightPlans;

		// attributes that must be imputed for join
		Set<QualifiedName> required = new HashSet<>();
		required.add(new QualifiedName(j.t1Alias, j.f1PureName));
		required.add(new QualifiedName(j.t2Alias, j.f2PureName));

		// names of table in already performed join
		final Set<String> leftTables = new HashSet<>();
		// alias for table to join in
		final Set<String> rightTables = new HashSet<>();
		Set<String> allTables = new HashSet<>();
		// just joining existing base relations
		boolean isSimpleJoin;

		if ((isSimpleJoin = news.isEmpty())) {
			// base case -- both are base relations
			leftTables.add(j.t1Alias);
			rightTables.add(j.t2Alias);
			// this retrieves results from FilterOptimizer
			leftPlans = pc.bestPlans(leftTables);
			rightPlans = pc.bestPlans(rightTables);
		} else {
			// news is not empty -- figure best way to join j to news
			leftTables.addAll(getTableAliases(news));

			if (!leftTables.contains(j.t1Alias) && !leftTables.contains(j.t2Alias)) {
				// there is no join possible if existing tables joined and new
				// join don't share any data
				return;
			}

			if (leftTables.contains(j.t1Alias)) {
				// this is already a left-deep plan, so no need to change anything
				rightTables.add(j.t2Alias);
			} else {
				rightTables.add(j.t1Alias);
				// we need to swap the join predicate information to perform this as
				// a left-deep join
				j = j.swapInnerOuter();
			}

			leftPlans = pc.bestPlans(leftTables);
			rightPlans = pc.bestPlans(rightTables);
		}

		// add the top-level of tables after this join
		allTables.addAll(leftTables);
		allTables.addAll(rightTables);

		// possible plans with imputations for the left-hand side
		for (ImputedPlanCachePareto.Value lplan : leftPlans) {
			// joins necessary to obtain state of LHS
			Set<LogicalJoinNode> necessaryJoins = new HashSet<LogicalJoinNode>(lplan.joins);
			if(!necessaryJoins.contains(j) && !necessaryJoins.contains(j.swapInnerOuter())) {
				// we can only explore removing a given join, if the plan on the LHS doesn't use it
				// extend information with new join
				necessaryJoins.add(j);
				// extended with any imputations necessary for join
				for (ImputedPlan lplanPrime : addImputes(lplan.plan, required, globalRequired)) {
					// possible plans with imputations for right-hand side
					for (ImputedPlanCachePareto.Value rplan : rightPlans) {
						// extended with any imputations necessary for join
						for (ImputedPlan rplanPrime : addImputes(rplan.plan, required, globalRequired)) {
							// create new join
							LogicalImputedJoinNode joined = new LogicalImputedJoinNode(tid, j.t1Alias, j.t2Alias, lplanPrime,
									rplanPrime, j.f1QuantifiedName, j.f2QuantifiedName, j.p, tableMap);
							// add to cache as appropriate
							pc.addJoinPlan(allTables, necessaryJoins, joined);

							// if it was a simple join, consider swapping order
							if (isSimpleJoin) {
								pc.addJoinPlan(allTables, necessaryJoins, joined.swapInnerOuter());
							}
						}
					}
				}
			}
		}
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
	 * @throws BadImputation 
	 */
	private List<ImputedPlan> addImputes(ImputedPlan plan, Set<QualifiedName> local, Set<QualifiedName> global) {
		// must = Dirty(plan) & local
		// all local requirements that are currently dirty
		Set<QualifiedName> must = new HashSet<>(local);
		must.retainAll(plan.getDirtySet());
		
		// may = must | (Dirty(plan) & global)
		// all forced attributes + all global requirements that are currently dirty
		Set<QualifiedName> may = new HashSet<>(global);
		may.retainAll(plan.getDirtySet());
		may.addAll(must);
		
		List<ImputedPlan> plans = new ArrayList<>();
		if (must.isEmpty()) {
			plans.add(plan);
		} else {
			plans.add(LogicalComposeImputation.create(plan, ImputationType.MINIMAL, must, tableMap));
			plans.add(LogicalComposeImputation.create(plan, ImputationType.DROP, must, tableMap));
		}
		
		if (!may.isEmpty()) {
			plans.add(LogicalComposeImputation.create(plan, ImputationType.DROP, may, tableMap));
			plans.add(LogicalComposeImputation.create(plan, ImputationType.MAXIMAL, may, tableMap));
		}

		return plans;
	}
	
	private ImputedPlanCache makeCache() {
		return new ImputedPlanCachePareto();
	}
	
	@Override
	public DbIterator physicalPlan(TransactionId tid, Map<String, TableStats> baseTableStats, boolean explain)
			throws ParsingException {
		// Determine the global imputation requirements.
		Set<QualifiedName> globalRequired = new HashSet<QualifiedName>();
		for (LogicalFilterNode filter : filters) {
			globalRequired.add(filter.fieldName);
		}
		for (LogicalJoinNode join : joins) {
			globalRequired.add(join.f1Name);
			globalRequired.add(join.f2Name);
		}
		
		// TODO: Check that this is precise.
		for (LogicalSelectListNode select : selectList) {
			if (select.fname.equals("null.*")) {
				for (LogicalScanNode table : tables) {
					TupleDesc td = Database.getCatalog().getTupleDesc(table.t);
					for (TDItem ti : td) {
						QualifiedName name = new QualifiedName(table.alias, ti.fieldName);
						globalRequired.add(name);
					}
				}
			} else {
				String[] aliasAndAttr = select.fname.split("\\.");
				globalRequired.add(new QualifiedName(aliasAndAttr[0], aliasAndAttr[1]));
			}
			
			if (select.aggOp != null) {
				globalRequired.add(aggField);
				if (groupByField != null) {
					globalRequired.add(groupByField);
				}
			}
		}
		
		// Construct an empty plan cache.
		ImputedPlanCache cache = makeCache();
		
		optimizeFilters(tid, cache, globalRequired);

		// if the number of tables used in attributes is large
		// planning times can blow up, so check set of tables and switch to approximate caches
		// if it is above 6 (empirically decided)
		// this set could be refined further to only consider dirty attributes in the base tables
		// but this requires getting base table states from qualified names that use table alias
		// rather than table name, which is what is found in the table stats map...
		Set<String> tablesImputed = new HashSet<String>();
		for(QualifiedName col : globalRequired) {
			tablesImputed.add(col.tableAlias);
		}

		if (tablesImputed.size() >= 7) {
			// use approximate pareto set for large number of joins
			// with imputation scatter across the tables
			System.err.println("Using approximate pareto sets");
			cache.setApproximate();
		}

		optimizeJoins(tid, cache, globalRequired);

		final Set<String> allTables = new HashSet<>();
		for (LogicalScanNode scan : tables) {
			allTables.add(scan.alias);
		}
		
		Iterable<ImputedPlanCachePareto.Value> bestPlans = cache.bestPlans(allTables);

		// The a tuple description for the output of the join. (all the plans
		// should
		// have the same schema, so it doesn't matter which we use)
		TupleDesc td;
		if (bestPlans.iterator().hasNext()) {
			td = bestPlans.iterator().next().plan.getPlan().getTupleDesc();
		} else {
			throw new RuntimeException("BUG: No plans available that cover all tables.");
		}

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
		ImputedPlanCache aggPlans = makeCache();
		if (aggField != null) {
			for (ImputedPlanCachePareto.Value val : bestPlans) {
				for (ImputedPlan plan : addImputes(val.plan, globalRequired, globalRequired)) {
					plan = new LogicalAggregateNode(plan, groupByField, aggOp, aggField);
					aggPlans.addPlan(allTables, plan);
				}
			}
		}
		else {
			// Otherwise impute other outfields and select lowest cost plan
			for (ImputedPlanCachePareto.Value val : bestPlans) {
				for (ImputedPlan plan : addImputes(val.plan, globalRequired, globalRequired)) {
					aggPlans.addPlan(allTables, plan);
				}
			}
		}

		DbIterator physicalPlan = null;
		ImputedPlan chosenPlan = aggPlans.getFinalPlan(lossWeight, allTables);

		if (chosenPlan != null) {
			physicalPlan = chosenPlan.getPlan();
		}

		if (physicalPlan == null) {
			throw new RuntimeException("BUG: No top-level plans available.");
		}
		
		// order-by can only be used on one of the projected fields, so no need to impute again
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
