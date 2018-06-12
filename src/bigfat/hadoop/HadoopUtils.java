package bigfat.hadoop;

import jannopts.ConfigurationException;
import jannopts.Configurator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import bigfat.BigFatLM;
import bigfat.util.MultiMemberGZIPInputStream;
import bigfat.util.MultiMemberGZIPInputStream;

import com.google.common.collect.Lists;

public class HadoopUtils {

	public static Path makeTempDir(Configuration conf, String prefix)
			throws IOException {
		String tmpDir = FileSystem.get(conf).getHomeDirectory() + "/tmp/"
				+ prefix + new Random().nextLong();
		Path tmp = new Path(tmpDir);
		return tmp;
	}

	public static void copyHdfsDirToDiskFile(Path hdfsDir, File diskFile,
			boolean gunzip) throws IOException {

		InputStream is = new HDFSDirInputStream(hdfsDir.toString());
		if (gunzip) {
			is = new MultiMemberGZIPInputStream(is);
		}
		BufferedReader in = new BufferedReader(new InputStreamReader(is));
		PrintWriter out = new PrintWriter(diskFile);

		String line;
		while ((line = in.readLine()) != null) {
			out.println(line);
		}
		in.close();
		out.close();
	}

	public static void runJob(Job job) throws IOException,
			InterruptedException, ClassNotFoundException {

		job.waitForCompletion(true);
		// job.submit();
		// int i = 0;
		// while (!job.isComplete()) {
		// System.err.println("M" + job.mapProgress() + " R" +
		// job.reduceProgress());
		// if (i++ % 10 == 0) {
		// printCounters(job);
		// }
		// Thread.sleep(1000);
		// }

		printCounters(job);

		if (!job.isSuccessful()) {
			System.err.println("JOB FAILED");
			System.exit(1);
		} else {
			System.err.println("Finished: " + job.getJobName());
		}
	}

	private static void printCounters(Job job) throws IOException {
		Iterable<String> groups = job.getCounters().getGroupNames();
		for (String group : groups) {
			if (!BigFatLM.PROGRAM_NAME.equals(group)) {
				continue;
			}

			CounterGroup counterGroup = job.getCounters().getGroup(group);
			List<Counter> counters = Lists
					.newArrayList(counterGroup.iterator());
			Collections.sort(counters, new Comparator<Counter>() {
				@Override
				public int compare(Counter o1, Counter o2) {
					return o1.getDisplayName().compareTo(o2.getDisplayName());
				}
			});

			for (Counter counter : counters) {
				String key = counterGroup.getDisplayName().trim() + "."
						+ counter.getDisplayName().trim();
				key = key.replace(" ", "_");
				String value = counter.getValue() + "";
				System.err.println(key + "\t" + value);
			}
		}
	}

	public static void configure(Configuration hadoopConfig, Object configurable) {
		Configurator config = new Configurator().ignoreUnrecognized(true)
				.withOptions(configurable.getClass()).readFrom(hadoopConfig);

		try {
			config.configure(configurable);
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
	}

	public static Path[] getLocalCacheFiles(Configuration conf)
			throws IOException {
		// hack for bug in local runner
		if (DistributedCache.getCacheFiles(conf) != null
				&& DistributedCache.getLocalCacheFiles(conf) == null) {
			DistributedCache
					.setLocalFiles(conf, conf.get("mapred.cache.files"));
		}
		return DistributedCache.getLocalCacheFiles(conf);
	}

	public static int getMapperId(Mapper<?, ?, ?, ?>.Context context) {
		return context.getTaskAttemptID().getTaskID().getId();
	}

	public static int getReducerId(Reducer<?, ?, ?, ?>.Context context) {
		return context.getTaskAttemptID().getTaskID().getId();
	}
}
