package bigfat.step1;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;
import jannopts.validators.PathCheck;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigfat.BigFatLM;
import bigfat.HadoopIteration;
import bigfat.hadoop.HadoopUtils;
import bigfat.hadoop.SortUtils;

import com.google.common.collect.ImmutableList;

public class VocabIteration implements HadoopIteration {

	@Option(shortName = "c", longName = "corpusIn", usage = "HDFS path to corpus to be processed")
	@HdfsPathCheck(exists = true)
	String hdfsCorpusIn;

	@Option(shortName = "v", longName = "vocabIdsOut", usage = "HDFS path to final vocab IDs (most frequent words get lowest IDs)")
	@HdfsPathCheck(exists = false)
	String hdfsVocabOut;

	public void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		String jobName = BigFatLM.PROGRAM_NAME + " -- Make Vocabulary ID's";
		Job job = new Job(conf, jobName);
		System.err.println("Starting: " + jobName);

		Path inPath = new Path(hdfsCorpusIn);
		Path outPath = HadoopUtils.makeTempDir(conf, BigFatLM.PROGRAM_NAME);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setJarByClass(VocabMapper.class);
		job.setMapperClass(VocabMapper.class);
		job.setCombinerClass(VocabReducer.class);
		job.setReducerClass(VocabReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		HadoopUtils.runJob(job);

		System.err.println("Merging unigram count files: " + jobName);

		File tmpFile1 = File.createTempFile(BigFatLM.PROGRAM_NAME, ".vocab1");
		boolean gunzip = false;
		System.err.println("Copying HDFS file " + outPath + " to " + tmpFile1.getAbsolutePath());
		HadoopUtils.copyHdfsDirToDiskFile(outPath, tmpFile1, gunzip);
		SortUtils.sortInPlace(tmpFile1, 2, true);

		System.err.println("Assigning vocab IDs for: " + jobName);
		File tmpFile2 = File.createTempFile(BigFatLM.PROGRAM_NAME, ".vocab2");
		BufferedReader in = new BufferedReader(new FileReader(tmpFile1));
		PrintWriter out = new PrintWriter(tmpFile2);
		out.println(BigFatLM.UNK + "\t" + BigFatLM.UNK_ID);
		out.println(BigFatLM.BOS + "\t" + BigFatLM.BOS_ID);
		out.println(BigFatLM.EOS + "\t" + BigFatLM.EOS_ID);
		String line;
		long wordId = 1;
		while ((line = in.readLine()) != null) {
			String word = line.substring(0, line.indexOf("\t"));
			out.println(word + "\t" + wordId);
			wordId++;
		}
		out.close();
		in.close();
		tmpFile1.delete();

		System.err.println("Copying final vocabulary from " + tmpFile2.getAbsolutePath() + " to HDFS " + hdfsVocabOut);
		FileSystem.get(job.getConfiguration()).copyFromLocalFile(
				new Path(tmpFile2.getAbsolutePath()), new Path(hdfsVocabOut));

		System.err.println("Done: " + jobName);
	}

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(VocabMapper.class, VocabReducer.class);
	}
}
