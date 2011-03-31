package bigfat.util;

import bigfat.datastructs.FastIntArrayList;

public class NgramUtils {

	public static int hashNgram(FastIntArrayList arr, boolean includeLastToken) {
	
		int hashCode = 0;
		int end = includeLastToken ? arr.size() : arr.size() - 1;
		for (int i = 0; i < end; i++) {
			// TODO: Better hash function for hashing first 2 integer ID's?
			// we shift since high-frequency items get low
			// ID's and we're likely to just end up with 0 most of the
			// time otherwise
			if (i == 0) {
				hashCode = Hashing.smear(arr.get(0));
			} else {
				hashCode ^= Hashing.smear(arr.get(i) << (8 * i));
			}
		}
		return hashCode;
	}

}
