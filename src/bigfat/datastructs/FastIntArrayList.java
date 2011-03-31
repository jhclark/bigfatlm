package bigfat.datastructs;

import java.util.concurrent.atomic.AtomicInteger;

import bigfat.BigFatLM;

/**
 * A few bits taken from the colt library for fast expandable int array lists.
 * This class DOES sacrifice some state safety for speed. For example, writing
 * to the original IntArrayList object when other subarrays still exist will
 * cause the subarrays to be overwritten. We try to detect this at runtime, but
 * make no guarantees.
 * 
 * @author jon
 */
public class FastIntArrayList {

	// sacrifice some time efficiency for sanity checking
	public boolean PARANOID = true;

	int[] data;

	int start = 0;
	int end = 0;

	// sanity checking
	AtomicInteger globalRevision;
	int myRevision;
	boolean mutable;

	public FastIntArrayList(int capacity) {
		data = new int[capacity];

		if (PARANOID) {
			// quickly generate a fairly unique integer
			// (even if all aren't unique, it should detect when we're doing
			// horrible things)
			myRevision = System.identityHashCode(data);
			globalRevision = new AtomicInteger(myRevision);
			mutable = true;
		}
	}

	private FastIntArrayList() {
	}

	public void add(int value) {
		if (PARANOID && !mutable) {
			throw new RuntimeException("Array is not mutable!");
		}
		if (end >= data.length) {
			expand(end + 1);
		}
		data[end] = value;
		end++;
	}

	public int get(int index) {
		if (PARANOID && myRevision != globalRevision.get()) {
			throw new IllegalStateException(
					"Parent object was changed, corrupting subarray.");
		}
		int i = index + start;
		if (i >= end)
			throw new IndexOutOfBoundsException("requested index = " + index
					+ "; actual index = " + i + "; start = " + start
					+ "; end  = " + end);
		return data[i];
	}

	/**
	 * Gets an immutable subarray
	 * 
	 * @param i
	 * @param j
	 * @return
	 */
	public FastIntArrayList subarray(int i, int j) {
		FastIntArrayList sub = new FastIntArrayList();
		if (PARANOID) {
			if (j < 0) {
				throw new IllegalArgumentException(
						"Index must be non-negative: " + j);
			}
			sub.myRevision = this.myRevision;
			sub.globalRevision = this.globalRevision;
			sub.mutable = false;
		}
		// TODO: Check bounds?
		sub.data = this.data;
		sub.start = this.start + i;
		sub.end = this.start + j;
		return sub;
	}

	public void clear() {
		start = 0;
		end = 0;
		if (PARANOID) {
			myRevision = globalRevision.incrementAndGet();
		}
	}

	// ZEROTON_ID is assumed to come first
	// TODO: Move this compareTo() to more specific location
	public int compareTo(FastIntArrayList other) {
		for (int i = 0; i < this.size() || i < other.size(); i++) {
			if (i >= this.size()) {
				return -1;
			} else if (i >= other.size()) {
				return 1;
			} else {
				int myId = this.get(i);
				int otherId = other.get(i);
				if (myId != otherId) {
					if(myId == BigFatLM.ZEROTON_ID) {
						return -1;
					} else if(otherId == BigFatLM.ZEROTON_ID) {
						return 1;
					} else if (myId > otherId) {
						return 1;
					} else {
						return -1;
					}
				}
			}
		}
		return 0;
	}

	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj instanceof FastIntArrayList) {
			FastIntArrayList other = (FastIntArrayList) obj;
			if (this.size() != other.size()) {
				return false;
			} else {
				// since this class is intended for use in MapReduce,
				// take advantage of sorted order: last integer
				// is most likely to differ
				for (int i = this.size() - 1; i >= 0; i--) {
					if (this.get(i) != other.get(i)) {
						return false;
					}
				}
				return true;
			}
		} else {
			return false;
		}
	}

	public int size() {
		return end - start;
	}

	private void expand(int max) {
		while (data.length < max) {
			final int len = data.length;
			int[] newArray = new int[len * 2];
			System.arraycopy(data, 0, newArray, 0, len);
			this.data = newArray;
		}
	}

	/**
	 * Makes a safe copy that is not subject to the parent array changing.
	 * 
	 * @return
	 */
	public FastIntArrayList copy() {
		FastIntArrayList copy = new FastIntArrayList(this.data.length);
		if (this.data.length > 0) {
			System.arraycopy(this.data, this.start, copy.data, 0, this.size());
		}
		copy.end = this.size();
		return copy;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder("[");
		for(int i=0; i<this.size(); i++) {
			builder.append(this.get(i) + ", ");
		}
		builder.append("]");
		return builder.toString();
	}

	public void addAll(FastIntArrayList other) {
		for (int i = 0; i < other.size(); i++) {
			this.add(other.get(i));
		}
	}

	// WARNING: Unsafe operation that could affect other lingering copies!
	public void set(int i, int value) {
		if (PARANOID) {
			myRevision = globalRevision.incrementAndGet();
		}
		this.data[i + this.start] = value;
	}

	public void setParanoid(boolean b) {
		this.PARANOID = b;
	}
}
