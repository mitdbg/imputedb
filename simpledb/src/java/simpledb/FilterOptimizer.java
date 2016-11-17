package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;

public class FilterOptimizer {
	public static enum ImputationType {
		MINIMAL, MAXIMAL, DROP, NONE
	}

	private final TransactionId tid;
	private final HashMap<String, LogicalScanNode> scans;
	private final HashMap<String, LogicalFilterNode> filters;
	private final double lossWeight;

	public FilterOptimizer(TransactionId tid, Collection<LogicalScanNode> scanNodes,
			Collection<LogicalFilterNode> filterNodes, double lossWeight) {
		scans = new HashMap<String, LogicalScanNode>(scanNodes.size());
		for (LogicalScanNode scan : scanNodes) {
			scans.put(scan.alias, scan);
		}
		filters = new HashMap<String, LogicalFilterNode>(filterNodes.size());
		for (LogicalFilterNode filter : filterNodes) {
			filters.put(filter.tableAlias, filter);
		}
		this.tid = tid;
		this.lossWeight = lossWeight;
	}

	public HashMap<HashSet<QuantifiedName>, LogicalAccessNode> getBest(String tableAlias) throws ParsingException {
		LogicalScanNode scan = scans.get(tableAlias);
		LogicalFilterNode filter = filters.get(tableAlias);
		if (scan == null) {
			throw new NoSuchElementException();
		}

		ArrayList<LogicalAccessNode> candidates = new ArrayList<LogicalAccessNode>();
		candidates.add(new LogicalAccessNode(tid, scan, ImputationType.DROP, filter));
		candidates.add(new LogicalAccessNode(tid, scan, ImputationType.MINIMAL, filter));
		candidates.add(new LogicalAccessNode(tid, scan, ImputationType.MAXIMAL, filter));
		try {
			candidates.add(new LogicalAccessNode(tid, scan, ImputationType.NONE, filter));
		} catch (IllegalArgumentException e) {
		}

		/* Select the best candidate for each distinct dirty set. */
		HashMap<HashSet<QuantifiedName>, LogicalAccessNode> best = new HashMap<HashSet<QuantifiedName>, LogicalAccessNode>();
		for (LogicalAccessNode node : candidates) {
			HashSet<QuantifiedName> ds = node.getDirtySet();
			if (!best.containsKey(ds) || best.get(ds).cost(lossWeight) > node.cost(lossWeight)) {
				best.put(ds, node);
			}
		}
		return best;
	}
}
