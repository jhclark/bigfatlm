package bigfat.step6; 

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import bigfat.BigFatLM;
import bigfat.HadoopIteration;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step5.InterpolateOrdersInfo;

import com.google.common.collect.ImmutableList;

public class RenormalizeBackoffsIteration implements HadoopIteration {

	@Option(shortName = "i", longName = "modelIn", usage = "HDFS input path to model with correct probabilities and arbitrary backoffs")
	@HdfsPathCheck(exists = true)
	String modelIn;

	@Option(shortName = "o", longName = "modelOut", usage = "HDFS output path for model with backoff weights corrected to match probabilities")
	@HdfsPathCheck(exists = false)
	String modelOut;

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(RenormalizeBackoffsMapper.class,
				RenormalizeBackoffsReducer.class);
	}

	@Override
	public void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		String jobName = BigFatLM.PROGRAM_NAME
				+ " -- Renormalize Backoff Weights";
		Job job = new Job(conf, jobName);
		System.out.println("Starting: " + jobName);

		Path inPath = new Path(modelIn);
		Path outTmpPath = new Path(modelOut);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outTmpPath);

		job.setJarByClass(RenormalizeBackoffsMapper.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(RenormalizeBackoffsMapper.class);
		//job.setCombinerClass(RenormalizeBackoffsReducer.class);
		job.setPartitionerClass(RenormalizeBackoffsPartitioner.class);

		job.setMapOutputKeyClass(RenormalizeBackoffsKey.class);
		job.setMapOutputValueClass(RenormalizeBackoffsInfo.class);

		job.setSortComparatorClass(RenormalizeBackoffsSortComparator.class);
		// TODO: speed up grouping by comparing bytes directly
		// ByteWriable.Comparator and our own byte implementation seem to make this
		// fail...
		job.setGroupingComparatorClass(RenormalizeBackoffsSortComparator.class);
		job.setReducerClass(RenormalizeBackoffsReducer.class);
		job.setOutputKeyClass(Ngram.class);
		job.setOutputValueClass(InterpolateOrdersInfo.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		HadoopUtils.runJob(job);

		System.out.println("Completed: " + jobName);
	}

}
