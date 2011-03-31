package bigfat.step7;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;
import jannopts.validators.PathCheck;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigfat.BigFatLM;
import bigfat.HadoopIteration;
import bigfat.hadoop.HadoopUtils;

import com.google.common.collect.ImmutableList;

public class DArpaIteration implements HadoopIteration {

	@Option(shortName = "m", longName = "modelIn", usage = "HDFS input path to unformatted model")
	@HdfsPathCheck(exists = true)
	String modelIn;
	
	@Option(shortName = "v", longName = "vocabFile", usage = "HDFS file mapping integer vocabulary indices to voacbulary IDs")
	private String vocabFile;

	@Option(shortName = "a", longName = "dArpaOut", usage = "HDFS out path to dARPA format model")
	@PathCheck(exists = false)
	String arpaFileOut;

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return (Iterable) ImmutableList.of(DArpaMapper.class);
	}

	@Override
	public void run(Configuration conf) throws IOException, InterruptedException,
			ClassNotFoundException {

		String jobName = BigFatLM.PROGRAM_NAME + " -- Format dARPA Model";
		Job job = new Job(conf, jobName);
		System.out.println("Starting: " + jobName);

		DistributedCache.addCacheFile(new Path(vocabFile).toUri(), job.getConfiguration());
		Path inPath = new Path(modelIn);
		Path outTmpPath = new Path(arpaFileOut);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outTmpPath);

		job.setJarByClass(DArpaMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(DArpaMapper.class);
		job.setNumReduceTasks(0);

		// we need to use hadoop streaming next for filtering
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		HadoopUtils.runJob(job);
	}

}
