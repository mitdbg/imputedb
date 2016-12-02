package simpledb;

import java.util.HashSet;
import java.util.Set;

public class DirtySet {
	private DirtySet() {}
	
	public static final HashSet<QuantifiedName> ofBaseTable(int tableId, String alias) {
		String tableName = Database.getCatalog().getTableName(tableId);
		Set<String> dirtyAttrs = TableStats.getTableStats(tableName).dirtyAttrs();
		HashSet<QuantifiedName> dirty = new HashSet<QuantifiedName>();
		for (String attr : dirtyAttrs) {
			dirty.add(new QuantifiedName(alias, attr));
		}
		return dirty;
	}
	
	public static final HashSet<String> toAttrs(HashSet<QuantifiedName> dirty) {
		HashSet<String> ret = new HashSet<String>();
		for (QuantifiedName n : dirty) {
			ret.add(n.toString());
		}
		return ret;
	}
}