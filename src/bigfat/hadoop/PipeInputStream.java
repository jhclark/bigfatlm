package bigfat.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;

/**
 * This file is taken from the Chaski project under the Apache license.
 * 
 * @author Qin Gao
 *
 */
public abstract class PipeInputStream extends InputStream {
	
	private Queue<String> files = new LinkedList<String>();
	private InputStream current = null;

	@Override
	public int read() throws IOException {
		int ret;
		while(true){
			if(current==null || (ret = current.read())==-1){
				String str = files.poll();
				if(str == null){
					return -1; // EOF
				}else{
					current = getNextStream(str);
					continue;
				}
			}else{
				return ret;
			}		
		}
	}
	
	/**
	 * Append a new file to the end of the pipe manually. Note that
	 * this method should not be mixed with using the file finder,
	 * when file finder is specified
	 * @param file
	 */
	public void appendFile(String file){
		files.add(file);
	}

	/**
	 * Get the input stream of next file, if the file cannot be opened, return
	 * null.
	 * @param file
	 * @return
	 */
	protected abstract InputStream getNextStream(String file);
	
}
