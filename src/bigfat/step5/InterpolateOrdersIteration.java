package bigfat.step5;

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
import bigfat.datastructs.NgramComparator;
import bigfat.hadoop.HadoopUtils;

import com.google.common.collect.ImmutableList;

public class InterpolateOrdersIteration implements HadoopIteration {

	@Option(shortName = "i", longName = "uninterpolatedIn", usage = "HDFS input path of uninterpolated model")
	@HdfsPathCheck(exists = true)
	String uninterpolatedIn;

	@Option(shortName = "o", longName = "interpolatedOut", usage = "HDFS output path interpolated model")
	@HdfsPathCheck(exists = false)
	String interpolatedOut;

	public void run(Configuration conf) throws IOException, InterruptedException,
			ClassNotFoundException {

		String jobName =
				BigFatLM.PROGRAM_NAME
						+ " -- Interpolate N-gram Orders";
		Job job = new Job(conf, jobName);
		System.out.println("Starting: " + jobName);

		Path inPath = new Path(uninterpolatedIn);
		Path outPath = new Path(interpolatedOut);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setJarByClass(InterpolateOrdersMapper.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapperClass(InterpolateOrdersMapper.class);

		job.setMapOutputKeyClass(DuplicatableNgram.class);
		job.setMapOutputValueClass(InterpolateOrdersIntermediateInfo.class);

		job.setSortComparatorClass(NgramComparator.class);
		job.setGroupingComparatorClass(NgramComparator.class);
		job.setPartitionerClass(InterpolateOrdersPartitioner.class);
		job.setReducerClass(InterpolateOrdersReducer.class);
		
		job.setOutputKeyClass(Ngram.class);
		job.setOutputValueClass(InterpolateOrdersInfo.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		HadoopUtils.runJob(job);
	}

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(InterpolateOrdersMapper.class, InterpolateOrdersReducer.class);
	}
}
