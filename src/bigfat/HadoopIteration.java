package bigfat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public interface HadoopIteration {
	public void run(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException;
	
	public Iterable<Class<?>> getConfigurables();
}
