package bigfat.step7;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;

import bigfat.BigFatLM;
import bigfat.util.MultiMemberGZIPInputStream;
import bigfat.BigFatLM.CompressionType;
import bigfat.util.StringUtils;
import bigfat.util.UncheckedLineIterable.UncheckedLineIterator;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;

public class ArpaMerger {

	public static boolean debug = false;

	public static void merge(Configuration conf, Path hdfsDir, File countsByOrder, File arpaFile)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(hdfsDir);

		// keep files sorted by order, but nothing else
		Ordering<DArpaLineIterator> shardComparator = new Ordering<DArpaLineIterator>() {
			@Override
			public int compare(DArpaLineIterator o1, DArpaLineIterator o2) {
				// discard iterators that should be closed early
				if (debug)
					System.err.print(o1.getName() + " " + o1.getOrder()
							+ " vs " + o2.getName() + " " + o2.getOrder()
							+ "\t");

				if (!o1.hasNext()) {
					if (debug)
						System.err.println("O1 !hasNext()");
					return -1;
				} else if (!o2.hasNext()) {
					if (debug)
						System.err.println("O2 !hasNext()");
					return 1;
				} else {
					if (o1.getOrder() == o2.getOrder()) {
						if (debug)
							System.err.println("Ord1 == Ord2");
						// don't pay attention to actual n-gram
						return 0;
					} else if (o1.getOrder() < o2.getOrder()) {
						if (debug)
							System.err.println("Ord1 < Ord2");
						return -1; // flip
					} else {
						if (debug)
							System.err.println("Ord1 > Ord2");
						return 1;
					}
				}
			}
		};
		
		// read countsByOrder
		Map<Integer, Long> orderCounts = new HashMap<Integer, Long>();
		BufferedReader countsIn = new BufferedReader(new FileReader(countsByOrder));
		String line;
		int nLine = 0;
		while((line = countsIn.readLine()) != null) {
			nLine++;
			Iterator<String> toks = Splitter.on(' ').split(line).iterator();
			try {
				int order = Integer.parseInt(toks.next());
				long count = Long.parseLong(toks.next());
				orderCounts.put(order, count);
			} catch(NoSuchElementException e) {
				throw new RuntimeException("Invalid line at " + countsByOrder.getAbsolutePath() + ":" 
						+ nLine + ": " + line);
			}
		}

		// write ARPA header
		PrintWriter arpaOut = new PrintWriter(new BufferedOutputStream(
				new FileOutputStream(arpaFile)));

		arpaOut.println();
		arpaOut.println("\\data\\");
		for (int order : orderCounts.keySet()) {
			long count = orderCounts.get(order);
			arpaOut.printf("ngram %d=%d\n", order, count);
		}

		PriorityQueue<DArpaLineIterator> shardHeap = new PriorityQueue<DArpaLineIterator>(
				100, shardComparator);

		CompressionType compression = BigFatLM.getCompressionType(
				new ArpaMergeIteration(), conf);

		for (FileStatus stat : files) {
			FSDataInputStream is = fs.open(stat.getPath());
			InputStream compressedInputStream;
			if(compression == CompressionType.GZIP) {
				compressedInputStream = new MultiMemberGZIPInputStream(is);
			} else if(compression == CompressionType.BZIP) {
				compressedInputStream = new BZip2Codec().createInputStream(is);
			} else {
				throw new RuntimeException("Unrecognized compression type: " + compression);
			}
			DArpaLineIterator shardReader = new DArpaLineIterator(
					compressedInputStream, Charset.forName("UTF-8"), stat
							.getPath().getName());
			shardHeap.add(shardReader);
			if (debug) {
				System.err.println("Heapified.");
				int j = 0;
				for (DArpaLineIterator i : shardHeap) {
					System.err.println(i.getName() + " -- " + i.getOrder());
					if (++j > 10)
						break;
				}
			}

		}

		System.out.printf("Opened %d shards...\n", shardHeap.size());

		int errors = 0;
		int prevOrder = 0;
		String prevFile = null;
		long n = 0;
		long totalN = 0;
		long expectedN = 0;
		
		long totalExpectedN = 0;
		for (long count : orderCounts.values()) {
			totalExpectedN += count;
		}

		while (true) {
			DArpaLineIterator shardWithNextLine = shardHeap.poll();
			if (shardWithNextLine == null) {
				break;
			}
			System.out.println("Reading shard " + shardWithNextLine.getName());
			int shardOrder = shardWithNextLine.getOrder();
			while (shardWithNextLine.hasNext()
					&& shardWithNextLine.getOrder() == shardOrder) {

				line = shardWithNextLine.next();
				String[] columns = StringUtils.tokenize(line, "\t");
				if (columns.length == 1) {
					System.err.println("Skipping zeroton: " + line);
				} else {
					String ngram = columns[0];
					String logProb = columns[1];
					// NOTE: if there's a 3rd column, it's the log backoff

					// switch column order to ARPA from dARPA
					columns[0] = logProb;
					columns[1] = ngram;
					int order = getOrder(ngram);

					if (order < prevOrder) {
						arpaOut.flush();
						throw new RuntimeException(
								"Order must always increase. Just wrote "
										+ prevOrder + " from " + prevFile
										+ " and now we're going to write "
										+ order + " from "
										+ shardWithNextLine.getName() + "?");
					} else if (order > prevOrder) {
						
						System.out.printf("Merged %d %d-grams...\n", n, (order-1));
						if(n != expectedN) {
							throw new RuntimeException(String.format("The specified orderByCounts file told us to expect %d %d-grams," +
									"but instead we found %d. This will lead to a malformed ARPA file.", expectedN, (order-1), n));
						}
						
						// move on to the next order
						System.out.printf("Merging %d-grams...\n", order);
						expectedN = orderCounts.get(order);
						n = 0;
						arpaOut.println();
						arpaOut.printf("\\%d-grams:\n", order);
					}

					n++;
					totalN++;

					arpaOut.print(columns[0]);
					arpaOut.print('\t');
					arpaOut.print(columns[1]);
					if (columns.length == 3) {
						arpaOut.print('\t');
						arpaOut.print(columns[2]);
					}
					arpaOut.print('\n');
					prevFile = shardWithNextLine.getName();

					int unit = 1000000;
					if (n % unit == 0) {
						float percent = (float) totalN / (float) totalExpectedN * 100;
						System.out.printf("Merged %,d/%,d %d-grams so far; %,d/%,d overall (%.2f percent complete)...\n",
								n, expectedN, order, totalN, totalExpectedN, percent);
					}
					prevOrder = order;
				}
			}

			// we read all the n-grams of the current order -- put the shard
			// back in the heap and sort by its next line
			if (shardWithNextLine.hasNext()) {
				shardHeap.add(shardWithNextLine);
			}
		}
		
		System.out.printf("Merged %d %d-grams...\n", n, prevOrder);
		if(n != expectedN) {
			throw new RuntimeException(String.format("The specified orderByCounts file told us to expect %d %d-grams," +
					"but instead we found %d. This will lead to a malformed ARPA file.", expectedN, prevOrder, n));
		}

		// write ARPA footer
		arpaOut.println();
		arpaOut.println("\\end\\");
		arpaOut.close();

		for (UncheckedLineIterator shard : shardHeap) {
			shard.close();
		}

		if (errors > 0) {
			System.err.println("Encountered " + errors
					+ " errors while merging shards");
			System.exit(1);
		}
	}

	private static int getOrder(String ngram) {
		int order = StringUtils.count(ngram, ' ') + 1;
		return order;
	}
}
