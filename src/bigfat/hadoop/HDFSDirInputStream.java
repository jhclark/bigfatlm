package bigfat.hadoop;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import bigfat.util.MultiMemberGZIPInputStream;

/**
 * This file is taken from the Chaski project under the Apache license.
 * 
 * @author Qin Gao
 */
public class HDFSDirInputStream extends PipeInputStream {

	private FileSystem fs;

	@Override
	protected InputStream getNextStream(String file) {
		try {
			FSDataInputStream stream = fs.open(new Path(file));
			if (file.endsWith(".gz")) {
				return new MultiMemberGZIPInputStream(stream);
			} else {
				return stream;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create a input stream that will read through all the files in one
	 * directory note that the file will be sorted by name, using the default
	 * string comparator.
	 * 
	 * @param fs
	 * @param dir
	 * @throws IOException
	 */
	public HDFSDirInputStream(FileSystem fs, String dir) throws IOException {
		this(fs, dir, null);
	}

	public HDFSDirInputStream(String dir) throws IOException {
		this(FileSystem.get(new Configuration()), dir, null);
	}

	/**
	 * Create a input stream that will read through all the files in one
	 * directory note that the file will be sorted by name, using the
	 * comparator.
	 * 
	 * @param fs
	 * @param dir
	 * @param comp
	 * @throws IOException
	 */
	public HDFSDirInputStream(FileSystem fs, String dir, Comparator<String> comp)
			throws IOException {
		this.fs = fs;
		Path p = new Path(dir);
		FileStatus fstate = fs.getFileStatus(p);
		if (fstate.isDir()) {
			FileStatus[] child = fs.globStatus(new Path(dir + "/*"));
			LinkedList<String> s = new LinkedList<String>();
			Map<String, Path> map = new HashMap<String, Path>();
			for (FileStatus c : child) {
				if (c.isDir())
					continue;
				map.put(c.getPath().getName(), c.getPath());
				s.add(c.getPath().getName());
			}
			if (comp != null)
				Collections.sort(s, comp);
			else
				Collections.sort(s);
			Iterator<String> it = s.iterator();
			while (it.hasNext()) {
				String n = it.next();
				Path pr = map.get(n);
				this.appendFile(pr.toString());
			}
		} else {
			this.appendFile(dir);
		}
	}

	/**
	 * Test case, input int dir wants to read and the file to output
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String args[]) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		HDFSDirInputStream inp = new HDFSDirInputStream(fs, args[0]);
		FileOutputStream ops = new FileOutputStream(args[1]);

		int r;
		while ((r = inp.read()) != -1) {
			ops.write(r);
		}
		ops.close();
	}

}
