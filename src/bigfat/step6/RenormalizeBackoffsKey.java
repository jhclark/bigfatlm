package bigfat.step6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.datastructs.SerializationUtils;
import bigfat.step7.DArpaMapper;

public class RenormalizeBackoffsKey implements
		WritableComparable<RenormalizeBackoffsKey> {

	public static final int DEFAULT_SIZE = 5;
	private int targetOrder;
	private FastIntArrayList list = new FastIntArrayList(DEFAULT_SIZE);
	private boolean last;

	public int getTargetOrder() {
		return targetOrder;
	}

	public boolean shouldBeLastInSort() {
		return last;
	}

	public void setInfo(int targetOrder, FastIntArrayList ids,
			boolean lastInSortOrder, boolean reverse) {

		this.targetOrder = targetOrder;
		this.last = lastInSortOrder;
		if (!reverse) {
			this.list = ids;
		} else {
			list.clear();
			for (int i = ids.size() - 1; i >= 0; i--) {
				list.add(ids.get(i));
			}
		}
	}

	public void getAsIds(FastIntArrayList ids) {
		getAsIds(ids, false);
	}

	public void getAsIds(FastIntArrayList ids, boolean reverse) {
		ids.clear();

		// TODO: Optimize
		if (!reverse) {
			ids.addAll(list);
		} else {
			for (int i = list.size(); i >= 0; i--) {
				ids.add(list.get(i));
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, targetOrder);
		SerializationUtils.writeList(out, list);
		out.writeByte(last ? 1 : 0);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		targetOrder = WritableUtils.readVInt(in);
		SerializationUtils.readList(in, list);
		last = in.readByte() == 1;
	}

	@Override
	public int compareTo(RenormalizeBackoffsKey o) {
		throw new RuntimeException("Use custom SortComaparator instead.");
	}

	@Override
	public String toString() {
		if (Ngram.DEBUG_VOCAB == null) {
			return targetOrder + " " + list.toString() + " "
					+ Boolean.toString(last);
		} else {
			StringBuilder builder = new StringBuilder();
			DArpaMapper.stringify(list, Ngram.DEBUG_VOCAB, builder);
			return targetOrder + " '" + builder.toString() + "' "
					+ Boolean.toString(last);
		}
	}
}
