package bigfat.interpolate.step1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import bigfat.util.SerializationPrecision;


/**
 * @author jhclark
 */
public class InterpolateModelsInfo implements Writable {

	private int modelNumber;
	private double p;
	private double bo;
	
	public boolean isMasterLM() {
		return modelNumber == 0;
	}
	
	public boolean hasProb() {
		return p != Float.NEGATIVE_INFINITY;
	}
	
	public boolean hasBackoff() {
		return bo != Float.NEGATIVE_INFINITY;
	}

	public double getInterpolatedProb() {
		return p;
	}

	public double getBackoffWeight() {
		return bo;
	}
	
	public int getModelNumber() {
		return modelNumber;
	}

	public void setAllInfo(int modelNumber, double p, double bo) {
		this.modelNumber = modelNumber;
		this.bo = bo;
		this.p = p;
	}
	
	public void setProbInfo(int modelNumber, double p) {
		this.modelNumber = modelNumber;
		this.bo = Double.NEGATIVE_INFINITY;
		this.p = p;
	}
	
	public void setBackoffInfo(int modelNumber, double bo) {
		this.modelNumber = modelNumber;
		this.bo = bo;
		this.p = Double.NEGATIVE_INFINITY;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, modelNumber);
		SerializationPrecision.writeReal(out, p);
		SerializationPrecision.writeReal(out, bo);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		modelNumber = WritableUtils.readVInt(in);
		p = SerializationPrecision.readReal(in);
		bo = SerializationPrecision.readReal(in);
	}
}
