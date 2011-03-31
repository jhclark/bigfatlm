package bigfat.datastructs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class SerializationUtils {

	public static void writeList(DataOutput out, FastIntArrayList list) throws IOException {
		WritableUtils.writeVInt(out, list.size());
		for (int i = 0; i < list.size(); i++) {
			WritableUtils.writeVInt(out, list.get(i));
		}
	}

	public static void readList(DataInput in, FastIntArrayList list) throws IOException {
		list.clear();
		int size = WritableUtils.readVInt(in);
		for (int i = 0; i < size; i++) {
			list.add(WritableUtils.readVInt(in));
		}
	}

	public static void writeArray(DataOutput out, float[] arr) throws IOException {
		WritableUtils.writeVInt(out, arr.length);
		for(int i=0; i<arr.length; i++) {
			out.writeFloat(arr[i]);
		}
	}

	public static float[] readArray(DataInput in, float[] arr) throws IOException {
		int len = WritableUtils.readVInt(in);
		float[] result;
		if(arr == null || len != arr.length) {
			result = new float[len];
		} else {
			result = arr;
		}
		for(int i=0; i<len; i++) {
			result[i] = in.readFloat();
		}
		return result;
	}

}
