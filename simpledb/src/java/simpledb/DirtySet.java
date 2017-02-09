package simpledb;

import java.util.HashSet;
import java.util.Set;

public class DirtySet {
	private DirtySet() {}
	
	public static final HashSet<QualifiedName> ofBaseTable(int tableId, String alias) {
		String tableName = Database.getCatalog().getTableName(tableId);
		Set<String> dirtyAttrs = TableStats.getTableStats(tableName).dirtyAttrs();
		HashSet<QualifiedName> dirty = new HashSet<QualifiedName>();
		for (String attr : dirtyAttrs) {
			dirty.add(new QualifiedName(alias, attr));
		}
		return dirty;
	}
	
	public static final HashSet<String> toAttrs(HashSet<QualifiedName> dirty) {
		HashSet<String> ret = new HashSet<String>();
		for (QualifiedName n : dirty) {
			ret.add(n.toString());
		}
		return ret;
	}
}