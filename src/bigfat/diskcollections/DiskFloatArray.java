package bigfat.diskcollections;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DiskFloatArray {
	
	private RandomAccessFile ra;
	private File file;
	private static final int BYTES_PER_FLOAT = 4;
	private long size = 0;

	public DiskFloatArray(File file) throws IOException {
		this.file = file;
		this.ra = new RandomAccessFile(file, "rw");
	}
	
	public void set(int i, float f) throws IOException {
		if(i >= size) {
			size = i+1;
			this.ra.setLength(size*BYTES_PER_FLOAT);
		}
		
		int offset = i * BYTES_PER_FLOAT;
		ra.seek(offset);
		ra.writeFloat(f);
	}
	
	// returns 0.0 if i has never been set
	public float get(int i) throws IOException {
		if(i >= size) {
			return 0.0f;
		} else {
			int offset = i * BYTES_PER_FLOAT;
			ra.seek(offset);
			return ra.readFloat();
		}
	}
	
	public void dispose() throws IOException {
		ra.close();
		file.delete();
	}
}
