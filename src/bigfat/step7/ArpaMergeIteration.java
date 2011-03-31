package bigfat.step7;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;
import jannopts.validators.PathCheck;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import bigfat.HadoopIteration;

import com.google.common.collect.ImmutableList;

public class ArpaMergeIteration implements HadoopIteration {

	@Option(shortName = "m", longName = "dArpaIn", usage = "HDFS input path to dARPA model")
	@HdfsPathCheck(exists = true)
	String modelIn;
	
	@Option(shortName = "c", longName = "countsByOrder", usage = "local input path to counts of ngrams for each order")
	@PathCheck(exists = true)
	File countsByOrder;

	@Option(shortName = "a", longName = "arpaFileOut", usage = "local out path to final ARPA file")
	@PathCheck(exists = false)
	File arpaFileOut;

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of();
	}
	
	@Override
	public void run(Configuration conf) throws IOException, InterruptedException,
			ClassNotFoundException {
		
		System.out.println("Merging dARPA shards into single ARPA file...");
		ArpaMerger.merge(conf, new Path(modelIn), countsByOrder, arpaFileOut);
	}
}
