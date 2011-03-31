package bigfat.step2;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;
import jannopts.validators.PathCheck;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import bigfat.BigFatLM;
import bigfat.HadoopIteration;
import bigfat.datastructs.Ngram;
import bigfat.datastructs.NgramComparator;
import bigfat.hadoop.HadoopUtils;

import com.google.common.collect.ImmutableList;

public class ExtractIteration implements HadoopIteration {

	@Option(shortName = "C", longName = "corpusIn", usage = "HDFS path to corpus to be processed")
	@HdfsPathCheck(exists = true)
	String hdfsCorpusIn;

	@Option(shortName = "v", longName = "vocabIds", usage = "HDFS path to vocabulary ID file")
	String vocabFile;

	@Option(shortName = "O", longName = "countsOut", usage = "Counts file out")
	@PathCheck(exists = false)
	String hdfsCountsOut;

	public void run(Configuration conf) throws IOException, InterruptedException,
			ClassNotFoundException {

		String jobName = BigFatLM.PROGRAM_NAME + " -- Extract and Count N-Grams";
		Job job = new Job(conf, jobName);
		System.out.println("Starting: " + jobName);

		DistributedCache.addCacheFile(new Path(vocabFile).toUri(), job.getConfiguration());
		System.err.println("Cache files now " + job.getConfiguration().get("mapred.cache.files"));

		Path inPath = new Path(hdfsCorpusIn);
		Path outPath = new Path(hdfsCountsOut);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setJarByClass(ExtractMapper.class);
		
		job.setMapperClass(ExtractMapper.class);
		job.setCombinerClass(CountReducer.class);
		job.setMapOutputKeyClass(Ngram.class);
		job.setMapOutputValueClass(VLongWritable.class);
		
		job.setSortComparatorClass(NgramComparator.class);
		job.setGroupingComparatorClass(NgramComparator.class);
		
		job.setPartitionerClass(AdjustedCountPartitioner.class);
		job.setReducerClass(AdjustedCountReducer.class);
		
		job.setOutputKeyClass(Ngram.class);
		job.setOutputValueClass(VLongWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		HadoopUtils.runJob(job);
	}

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(ExtractMapper.class, AdjustedCountReducer.class);
	}
}
