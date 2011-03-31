package bigfat.step3;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;
import jannopts.validators.PathCheck;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigfat.BigFatLM;
import bigfat.HadoopIteration;
import bigfat.hadoop.HadoopUtils;
import bigfat.util.IOUtils;
import bigfat.util.StringUtils;

import com.google.common.collect.ImmutableList;

public class DiscountsIteration implements HadoopIteration {

	@Option(shortName = "c", longName = "countsIn", usage = "HDFS path to (adjusted) counts")
	@HdfsPathCheck(exists = true)
	String hdfsCorpusIn;

	@Option(shortName = "C", longName = "countOfCountsTmpOut", usage = "Local path for temporary count of counts file")
	String countCountsTmpOut;

	@Option(shortName = "d", longName = "discountsOut", usage = "HDFS file where discounts will be written")
	@HdfsPathCheck(exists = false)
	String hdfsDiscountsOut;

	@Option(shortName = "m", longName = "maxOrder", usage = "order of the language model")
	@PathCheck(exists = false)
	int maxOrder;

	public void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		String jobName = BigFatLM.PROGRAM_NAME
				+ " -- Calculate Discounts via Counts of (Adjusted) Counts";
		Job job = new Job(conf, jobName);
		System.err.println("Starting: " + jobName);

		Path inPath = new Path(hdfsCorpusIn);
		Path outPath = HadoopUtils.makeTempDir(conf, BigFatLM.PROGRAM_NAME);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setJarByClass(CountCountsMapper.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(CountCountsMapper.class);
		job.setCombinerClass(CountCountsReducer.class);
		job.setMapOutputKeyClass(CountOfCountsInfo.class);
		job.setMapOutputValueClass(VLongWritable.class);

		job.setReducerClass(CountCountsReducer.class);
		job.setOutputKeyClass(CountOfCountsInfo.class);
		job.setOutputValueClass(LongWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		HadoopUtils.runJob(job);

		File tmpFile1 = new File(countCountsTmpOut);
		System.err.println("Copying counts of counts temporarily from "
				+ outPath.toString() + " to " + tmpFile1.getAbsolutePath()
				+ ": " + jobName);
		boolean gunzip = false;
		HadoopUtils.copyHdfsDirToDiskFile(outPath, tmpFile1, gunzip);

		System.err.println("Calculating discounts: " + jobName);
		long[][] countCounts = loadCountCounts(tmpFile1);
		checkCountCountsSanity(countCounts, maxOrder,
				CountCountsMapper.MAX_MOD_KN_SPECIAL_COUNT);
		// tmpFile.delete();

		File tmpFile2 = File
				.createTempFile(BigFatLM.PROGRAM_NAME, ".discounts");
		PrintWriter out = new PrintWriter(tmpFile2);

		for (int order = 1; order <= maxOrder; order++) {
			for (int countCateg = 1; countCateg <= CountCountsMapper.MAX_MOD_KN_SPECIAL_COUNT; countCateg++) {
				double disc = knDiscount(countCounts, order, countCateg);
				out.println(order + "\t" + countCateg + "\t" + disc);
			}
		}

		out.close();

		FileSystem.get(job.getConfiguration()).copyFromLocalFile(
				new Path(tmpFile2.getAbsolutePath()),
				new Path(hdfsDiscountsOut));

		System.err.println("Done: " + jobName);
	}

	private static void checkCountCountsSanity(long[][] countCounts,
			int maxOrder, int maxSpecialCount) {
		
		boolean error = false;
		for (int order = 0; order < maxOrder; order++) {
			for (int count = 0; count < maxSpecialCount + 1; count++) {
				if (countCounts[order][count] == 0) {
					System.err
							.println("ERROR: Count of (adjusted) counts for order "
									+ (order + 1)
									+ ", count category "
									+ (count + 1) + " is zero.");
					error = true;
				}
			}
		}
		if (error) {
			System.err
					.println("FATAL: Count of counts are not valid. Did you give me artificial or very small data?");
			System.exit(1);
		}
	}

	private static double knDiscount(long[][] countCounts, int order,
			int countCat) {

		// Mod-Kneser Ney specializes the count categories 1, 2, and 3+ by
		// definition
		// count corresponds to a
		// countCat corresponds to b
		// countCat = min(count, 3)

		// notice that countCounts is indexed by [order][countCat-1]
		// so the normally one-based countCat now becomes zero-based when
		// addressing the array
		double Y = countCounts[order - 1][0]
				/ (double) (countCounts[order - 1][0] + 2 * countCounts[order - 1][1]);
		double disc = (countCat - (countCat + 1) * Y
				* countCounts[order - 1][countCat]
				/ (double) countCounts[order - 1][countCat - 1]);
		// TODO: Make this an option in verbose mode
		// System.err.printf("eval %d - %d * %f * %d / %d = %f\n", countCat,
		// countCat + 1, Y,
		// countCounts[order - 1][countCat], countCounts[order - 1][countCat -
		// 1], disc);
		return disc;
	}

	private long[][] loadCountCounts(File tmpFile)
			throws FileNotFoundException, IOException {

		BufferedReader in = IOUtils.getBufferedReader(tmpFile);
		String line;
		long[][] countCounts = new long[maxOrder][CountCountsMapper.MAX_MOD_KN_SPECIAL_COUNT + 1];
		while ((line = in.readLine()) != null) {
			String[] split = StringUtils.tokenize(line, " \t");
			int order = Integer.parseInt(split[0]);
			int count = Integer.parseInt(split[1]);
			long countCount = Long.parseLong(split[2]);
			countCounts[order - 1][count - 1] = countCount;
		}
		in.close();
		return countCounts;
	}

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of(CountCountsMapper.class,
				CountCountsReducer.class);
	}
}
