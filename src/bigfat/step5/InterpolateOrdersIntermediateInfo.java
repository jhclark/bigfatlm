package bigfat.step5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import bigfat.util.SerializationPrecision;

/**
 * A convenience wrapper for count of counts keys
 * 
 * @author jon
 * 
 */
public class InterpolateOrdersIntermediateInfo implements Writable {

	private double prob;
	private double interpWeight;

	public double getUninterpolatedProb() {
		return prob;
	}

	public double getInterpolationWeight() {
		return interpWeight;
	}

	public void setValues(double p, double interpWeight) {
		this.prob = p;
		this.interpWeight = interpWeight;
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		SerializationPrecision.writeReal(out, prob);
		SerializationPrecision.writeReal(out, interpWeight);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		prob = SerializationPrecision.readReal(in);
		interpWeight = SerializationPrecision.readReal(in);
	}
}
