package simpledb;

import java.util.*;

public class QualifiedName implements Comparable<QualifiedName> {
	public final String tableAlias;
	public final String attrName;

	public QualifiedName(String tableAlias, String attrName) {
		this.tableAlias = tableAlias;
		this.attrName = attrName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((attrName == null) ? 0 : attrName.hashCode());
		result = prime * result + ((tableAlias == null) ? 0 : tableAlias.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		QualifiedName other = (QualifiedName) obj;
		if (attrName == null) {
			if (other.attrName != null) {
				return false;
			}
		} else if (!attrName.equals(other.attrName)) {
			return false;
		}
		if (tableAlias == null) {
			if (other.tableAlias != null) {
				return false;
			}
		} else if (!tableAlias.equals(other.tableAlias)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return tableAlias + "." + attrName;
	}

	public static Collection<String> toNames(Collection<QualifiedName> attrs) {
		ArrayList<String> names = new ArrayList<>();
		for(QualifiedName attr : attrs) {
			names.add(attr.toString());
		}
		return names;
	}

	@Override
	public int compareTo(QualifiedName o) {
		int c1 = tableAlias.compareTo(o.tableAlias);
		if (c1 != 0) {
			return c1;
		}
		return attrName.compareTo(o.attrName);
	}
}