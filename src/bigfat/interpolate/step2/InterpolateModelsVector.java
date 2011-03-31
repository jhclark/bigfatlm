package bigfat.interpolate.step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

import bigfat.datastructs.SerializationUtils;
import bigfat.util.SerializationPrecision;


/**
 * @author jhclark
 */
public class InterpolateModelsVector implements Writable {

	private double[] p;
	private double[] bo;
	
	public double[] getProbs() {
		return p;
	}

	public double[] getBackoffWeights() {
		return bo;
	}

	public void setInfo(double[] p, double[] bo) {
		this.bo = bo;
		this.p = p;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		SerializationPrecision.writeRealArray(out, p);
		SerializationPrecision.writeRealArray(out, bo);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// creates an array iff the passed in array is null
		p = SerializationPrecision.readRealArray(in, p);
		bo = SerializationPrecision.readRealArray(in, bo);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(p) + " " + Arrays.toString(bo);
	}
}
