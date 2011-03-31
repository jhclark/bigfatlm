package bigfat.step6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import bigfat.datastructs.Ngram;
import bigfat.util.SerializationPrecision;

/**
 *  
 * @author jhclark
 * 
 */
public class RenormalizeBackoffsInfo implements Writable, Serializable {

	private static final long serialVersionUID = 2846558896011479855L;
	private int word;
	private double prob;

	private RenormalizeBackoffsInfo(RenormalizeBackoffsInfo other) {
		this.prob = other.prob;
		this.word =  other.word;
	}
	
	public RenormalizeBackoffsInfo() {
	}
	
	public RenormalizeBackoffsInfo copy() {
		return new RenormalizeBackoffsInfo(this);
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
		WritableUtils.writeVInt(out, word);
		SerializationPrecision.writeReal(out, prob);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word = WritableUtils.readVInt(in);
		this.prob = SerializationPrecision.readReal(in);
	}

	public double getProb() {
		return prob;
	}

	public int getWord() {
		return word;
	}

	public void setInfo(int wordId, double p) {
		this.prob = p;
		this.word = wordId;
	}
	
	@Override
	public String toString() {
		if(Ngram.DEBUG_VOCAB == null) {
			return word + " " + prob;
		} else {
			return Ngram.DEBUG_VOCAB.toWord(word) + " " + prob;
		}
	}
}
