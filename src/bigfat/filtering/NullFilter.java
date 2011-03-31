package bigfat.filtering;

import bigfat.datastructs.FastIntArrayList;

public class NullFilter implements NgramFilter {
	public boolean isRequired(FastIntArrayList ngram, boolean isReversed) {
		return true;
	}
}
