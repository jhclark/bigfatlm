package bigfat.step4;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import bigfat.BigFatLM;
import bigfat.HadoopIteration;
import bigfat.datastructs.Ngram;
import bigfat.datastructs.NgramComparator;
import bigfat.hadoop.HadoopUtils;

import com.google.common.collect.ImmutableList;

public class UninterpolatedIteration implements HadoopIteration {

	@Option(shortName = "c", longName = "adjustedCountsIn", usage = "HDFS input path of adjusted counts")
	@HdfsPathCheck(exists = true)
	String adjustedCountsIn;
	
	@Option(shortName = "d", longName = "discountFile", usage = "HDFS file containing discounts for each order and count category")
	@HdfsPathCheck(exists = true)
	private String discountFile;

	@Option(shortName = "s", longName = "statsOut", usage = "HDFS Output for sufficient statistics for interpolation")
	@HdfsPathCheck(exists = false)
	String suffStatsOut;

	public void run(Configuration conf) throws IOException, InterruptedException,
			ClassNotFoundException {

		String jobName =
				BigFatLM.PROGRAM_NAME
						+ " -- Calculate Uninterpolated Probabilities and Backoff Weights";
		Job job = new Job(conf, jobName);
		System.out.println("Starting: " + jobName);

		DistributedCache.addCacheFile(new Path(discountFile).toUri(), job.getConfiguration());
		Path inPath = new Path(adjustedCountsIn);
		Path outPath = new Path(suffStatsOut);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setJarByClass(UninterpolatedMapper.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(UninterpolatedMapper.class);
		job.setMapOutputKeyClass(Ngram.class);
		job.setMapOutputValueClass(UninterpolatedIntermediateValue.class);

		job.setPartitionerClass(UninterpolatedPartitioner.class);
		job.setSortComparatorClass(NgramComparator.class);
		job.setGroupingComparatorClass(NgramComparator.class);
		job.setReducerClass(UninterpolatedReducer.class);

		job.setOutputKeyClass(Ngram.class);
		job.setOutputValueClass(UninterpolatedInfo.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		HadoopUtils.runJob(job);
	}

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(UninterpolatedMapper.class, UninterpolatedReducer.class);
	}
}
