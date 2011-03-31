package bigfat.step7;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;
import jannopts.validators.PathCheck;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigfat.BigFatLM;
import bigfat.HadoopIteration;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step5.InterpolateOrdersInfo;

import com.google.common.collect.ImmutableList;

public class ArpaFilterIteration implements HadoopIteration {

	@Option(shortName = "m", longName = "modelIn", usage = "HDFS input path to unformatted model")
	@HdfsPathCheck(exists = true)
	String modelIn;

	@Option(shortName = "h", longName = "hdfsTmpDir", usage = "HDFS temporary output path for intermediate files")
	@HdfsPathCheck(exists = false)
	String hdfsTmp;
	
	@Option(shortName = "v", longName = "vocabFile", usage = "HDFS file mapping integer vocabulary indices to voacbulary IDs")
	private String vocabFile;

	@Option(shortName = "a", longName = "arpaFileOut", usage = "local out path to final ARPA file")
	@PathCheck(exists = false)
	File arpaFileOut;

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return (Iterable) ImmutableList.of();
	}

	@Override
	public void run(Configuration conf) throws IOException, InterruptedException,
			ClassNotFoundException {
		System.err.println("UNIMPLEMENTED. Please use hadoop streaming instead.");
		System.exit(1);
	}

}
