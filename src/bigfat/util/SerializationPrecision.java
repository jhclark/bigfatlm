package bigfat.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class SerializationPrecision {
	public static boolean DOUBLE_PRECISION = false;

	public static void writeReal(DataOutput out, double prob) throws IOException {
		if(DOUBLE_PRECISION) {
			out.writeDouble(prob);
		} else {
			out.writeFloat((float)prob);
		}
	}

	public static double readReal(DataInput in) throws IOException {
		if(DOUBLE_PRECISION) {
			return in.readDouble();
		} else {
			return in.readFloat();
		}
	}
	
	public static void writeRealArray(DataOutput out, double[] arr) throws IOException {
		WritableUtils.writeVInt(out, arr.length);
		for(int i=0; i<arr.length; i++) {
			writeReal(out, arr[i]);
		}
	}

	public static double[] readRealArray(DataInput in, double[] arr) throws IOException {
		int len = WritableUtils.readVInt(in);
		double[] result;
		if(arr == null || len != arr.length) {
			result = new double[len];
		} else {
			result = arr;
		}
		for(int i=0; i<len; i++) {
			result[i] = readReal(in);
		}
		return result;
	}
}
