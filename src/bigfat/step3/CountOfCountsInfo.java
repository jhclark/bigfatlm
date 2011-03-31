package bigfat.step3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * For count of counts keys. Uses compareTo(), hashCode(),
 * and equals() for sorting and grouping instead of a custom comparator since
 * this MapReduce iteration is very fast anyway.
 * 
 * @author jhclark
 * 
 */
public class CountOfCountsInfo implements WritableComparable<CountOfCountsInfo> {

	private int order;
	private int count;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(order);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.order = in.readInt();
		this.count = in.readInt();
	}

	public void setInfo(int order, int count) {
		this.order = order;
		this.count = count;
	}

	public int getOrder() {
		return this.order;
	}

	public int getCountBeingCounted() {
		return this.count;
	}

	@Override
	public int compareTo(CountOfCountsInfo oth) {
		if (this.order < oth.order) {
			return -1;
		} else if (this.order > oth.order) {
			return 1;
		} else {
			if (this.count < oth.count) {
				return -1;
			} else if (this.count > oth.count) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof CountOfCountsInfo) {
			return compareTo((CountOfCountsInfo) other) == 0;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return order ^ count;
	}

	@Override
	public String toString() {
		return order + " " + count;
	}
}
