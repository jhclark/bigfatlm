package bigfat.step4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import bigfat.util.SerializationPrecision;

/**
 * Stores a word w (as in P(w|h), it's interpolation weight, and its
 * uninterpolated probability.
 * 
 * @author jhclark
 * 
 */
public class UninterpolatedInfo implements Writable {

	private int wordId;
	private double prob;
	private double interpWeight;

	public double getUninterpolatedProb() {
		return prob;
	}

	public double getInterpolationWeight() {
		return interpWeight;
	}

	public int getWord() {
		return wordId;
	}

	public void setValues(int wordId, double p, double bo) {
		this.prob = p;
		this.interpWeight = bo;
		this.wordId = wordId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, wordId);
		SerializationPrecision.writeReal(out, prob);
		SerializationPrecision.writeReal(out, interpWeight);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		wordId = WritableUtils.readVInt(in);
		prob = SerializationPrecision.readReal(in);
		interpWeight = SerializationPrecision.readReal(in);
	}
	
	public String toString() {
		return wordId + " " + prob + " " + interpWeight;
	}
}
