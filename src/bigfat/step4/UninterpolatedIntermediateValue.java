package bigfat.step4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.SerializationUtils;

/**
 * Stores the count and w as in P(w|c).
 * 
 * @author jhclark
 */
public class UninterpolatedIntermediateValue implements Writable, Serializable {

	private static final long serialVersionUID = 9132284232330979707L;
	
	// this values will be recovered from the string
	private FastIntArrayList word = new FastIntArrayList(1);
	private long count;
	
	public UninterpolatedIntermediateValue() {
	}
	
	private UninterpolatedIntermediateValue(FastIntArrayList copy, long count) {
		this.count = count;
		this.word = copy;
	}
	
	// custom java Serialization just defers to custom Hadoop serialization
	private void writeObject(ObjectOutputStream out) throws IOException {
		write(out);
	}
	
	// custom java Serialization just defers to custom Hadoop serialization
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		SerializationUtils.writeList(out, word);
		WritableUtils.writeVLong(out, count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if(word == null) {
			word = new FastIntArrayList(1);
		}
		SerializationUtils.readList(in, word);
		count = WritableUtils.readVLong(in);
	}

	public void getWord(FastIntArrayList wordOut) {
		
		// TODO: Optimize
		wordOut.clear();
		if(word.size() > 0) {
			wordOut.add(word.get(0));
		}
	}

	public long getAdjustedCount() {
		return count;
	}

	public void setInfo(FastIntArrayList word, long count) {
		this.word = word;
		this.count = count;
	}

	public UninterpolatedIntermediateValue copy() {
		return new UninterpolatedIntermediateValue(word.copy(), count);
	}
}
