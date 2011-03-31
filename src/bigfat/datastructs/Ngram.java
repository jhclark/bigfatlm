package bigfat.datastructs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import bigfat.BigFatLM;
import bigfat.step1.Vocabulary;
import bigfat.step7.DArpaMapper;

public class Ngram implements WritableComparable<Ngram> {
	
	public static Vocabulary DEBUG_VOCAB = null;
	public static void setVocabForDebug(Vocabulary vocab) {
		DEBUG_VOCAB = vocab;
	}

	public static final int DEFAULT_SIZE = 5;
	private FastIntArrayList list = new FastIntArrayList(DEFAULT_SIZE);

	public int getOrder() {
		if(list.size() == 1 && list.get(0) == BigFatLM.ZEROTON_ID) {
			return 0;
		} else {
			return list.size();
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		SerializationUtils.writeList(out, list);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		SerializationUtils.readList(in, list);
	}

	@Override
	public int compareTo(Ngram other) {
		throw new RuntimeException("Use raw comparator instead.");
	}

	static {
		// register this comparator
		WritableComparator.define(Ngram.class, new NgramComparator());
	}

	public void setFromIds(FastIntArrayList ids, boolean reverse) {
		if (!reverse) {
			this.list = ids;
		} else {
			this.list.clear();
			for (int i = ids.size() - 1; i >= 0; i--) {
				this.list.add(ids.get(i));
			}
		}
	}

	public void getAsIds(FastIntArrayList ids) {
		getAsIds(ids, false);
	}

	public void getAsIds(FastIntArrayList ids, boolean reverse) {
		ids.clear();
		if (!reverse) {
			ids.addAll(list);
		} else {
			for (int i = list.size() - 1; i >= 0; i--) {
				ids.add(list.get(i));
			}
		}
	}
	
	public String toString() {
		if(DEBUG_VOCAB == null) {
			return list.toString();
		} else {
			StringBuilder builder = new StringBuilder();
			DArpaMapper.stringify(list, DEBUG_VOCAB, builder);
			return builder.toString();
		}
	}
}
