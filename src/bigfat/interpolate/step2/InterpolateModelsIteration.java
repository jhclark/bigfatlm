package bigfat.interpolate.step2;

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
import bigfat.step5.DuplicatableNgram;
import bigfat.step5.InterpolateOrdersInfo;
import bigfat.step5.InterpolateOrdersPartitioner;

import com.google.common.collect.ImmutableList;

public class InterpolateModelsIteration implements HadoopIteration {

	@Option(shortName = "i", longName = "modelsIn", usage = "HDFS input path to expanded LM with vector of probabilities and backoffs for each ngram in the master LM")
	@HdfsPathCheck(exists = true)
	String modelsIn;

	@Option(shortName = "o", longName = "modelOut", usage = "HDFS output path for model with backoff weights corrected to match probabilities")
	@HdfsPathCheck(exists = false)
	String modelOut;

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(
				InterpolateModelsMapper.class,
				InterpolateModelsReducer.class);
	}

	@Override
	public void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		String jobName = BigFatLM.PROGRAM_NAME
				+ " -- Interpolate Models";
		Job job = new Job(conf, jobName);
		System.out.println("Starting: " + jobName);
		
		Path inPath = new Path(modelsIn);
		FileInputFormat.addInputPath(job, inPath);

		Path outPath = new Path(modelOut);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setJarByClass(InterpolateModelsMapper.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(InterpolateModelsMapper.class);
		job.setPartitionerClass(InterpolateOrdersPartitioner.class);

		job.setMapOutputKeyClass(DuplicatableNgram.class);
		job.setMapOutputValueClass(InterpolateModelsVector.class);

		job.setSortComparatorClass(NgramComparator.class);
		// speed up grouping by comparing bytes directly
		job.setGroupingComparatorClass(NgramComparator.class);
		job.setReducerClass(InterpolateModelsReducer.class);

		job.setOutputKeyClass(Ngram.class);
		job.setOutputValueClass(InterpolateOrdersInfo.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		HadoopUtils.runJob(job);

		System.out.println("Completed: " + jobName);
	}

}
