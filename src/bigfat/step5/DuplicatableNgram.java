package bigfat.step5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;

public class DuplicatableNgram extends Ngram {
	
	private int destReducer;
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		WritableUtils.writeVInt(out, destReducer);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		destReducer = WritableUtils.readVInt(in);
	}

	public int getDestinationReducer() {
		return destReducer;
	}

	public void setFromIds(int iReducer, FastIntArrayList ids,
			boolean reverse) {
		destReducer = iReducer;
		super.setFromIds(ids, reverse);
	}	
	
	@Override
	public void setFromIds(FastIntArrayList ids, boolean reverse) {
		throw new RuntimeException("Not valid for DuplicatableNgram");
	}
}
