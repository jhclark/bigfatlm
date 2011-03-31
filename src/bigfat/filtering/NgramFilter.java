package bigfat.filtering;

import bigfat.datastructs.FastIntArrayList;

public interface NgramFilter {
	public boolean isRequired(FastIntArrayList context, boolean isReversed);
}
