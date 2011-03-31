package bigfat.step5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import bigfat.util.SerializationPrecision;


/**
 * @author jhclark
 */
public class InterpolateOrdersInfo implements Writable {

	private double p;
	private double bo;

	public double getInterpolatedProb() {
		return p;
	}

	public double getBackoffWeight() {
		return bo;
	}

	public void setInfo(double p, double bo) {
		this.bo = bo;
		this.p = p;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		SerializationPrecision.writeReal(out, p);
		SerializationPrecision.writeReal(out, bo);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		p = SerializationPrecision.readReal(in);
		bo = SerializationPrecision.readReal(in);
	}
	
	public String toString() {
		return p +" " + bo;
	}
}
