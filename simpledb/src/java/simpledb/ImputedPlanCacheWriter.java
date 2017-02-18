package simpledb;

import java.util.Set;

public interface ImputedPlanCacheWriter {

	void write(Set<String> tables);

}