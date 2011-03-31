package bigfat.interpolate.step1;

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
import bigfat.interpolate.step2.InterpolateModelsVector;

import com.google.common.collect.ImmutableList;

public class AttachBackoffsIteration implements HadoopIteration {

	@Option(shortName = "i", longName = "modelsIn", usage = "HDFS input paths (semicolon-delimited) to models with correct uninterpolated (with regard to models) probabilities and arbitrary backoffs", arrayDelim = ";")
	@HdfsPathCheck(exists = true)
	String[] modelsIn;

	@Option(shortName = "o", longName = "modelOut", usage = "HDFS output path for model with backoff weights corrected to match probabilities")
	@HdfsPathCheck(exists = false)
	String modelOut;

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(AttachBackoffsMapper.class,
				AttachBackoffsReducer.class);
	}

	@Override
	public void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		String jobName = BigFatLM.PROGRAM_NAME
				+ " -- Expand Models by Attaching Prob and Model Vectors to Master LM";
		Job job = new Job(conf, jobName);
		System.out.println("Starting: " + jobName);

		for (int i = 0; i < modelsIn.length; i++) {
			// TODO: Check length of weight vector (currently only accessible in
			// mapper) against number of models here?
			if (modelsIn[i].trim().length() > 0) {
				System.err.println("Adding input path: " + modelsIn[i]);
				Path inPath = new Path(modelsIn[i]);
				FileInputFormat.addInputPath(job, inPath);
			}
		}

		Path outTmpPath = new Path(modelOut);
		FileOutputFormat.setOutputPath(job, outTmpPath);

		job.setJarByClass(AttachBackoffsMapper.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(AttachBackoffsMapper.class);
		job.setPartitionerClass(AttachBackoffsPartitioner.class);

		job.setMapOutputKeyClass(Ngram.class);
		job.setMapOutputValueClass(InterpolateModelsInfo.class);

		// job.setSortComparatorClass(RenormalizeBackoffsSortComparator.class);
		// speed up grouping by comparing bytes directly
		// job.setGroupingComparatorClass(BytesWritable.Comparator.class);
		job.setReducerClass(AttachBackoffsReducer.class);

		job.setOutputKeyClass(Ngram.class);
		job.setOutputValueClass(InterpolateModelsVector.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		HadoopUtils.runJob(job);

		System.out.println("Completed: " + jobName);
	}

}
