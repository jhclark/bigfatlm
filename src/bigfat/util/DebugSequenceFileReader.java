package bigfat.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HDFSDirInputStream;
import bigfat.step1.Vocabulary;

public class DebugSequenceFileReader {
	public static void main(String[] args) throws IOException,
			InstantiationException, IllegalAccessException {

		if (args.length != 4) {
			System.err
					.println("Usage: program <format> <vocabIdFile> <inFile> <outFile>");
			System.exit(1);
		}

		String format = args[0];
		String vocabIdFile = args[1];
		String inFile = args[2];
		String outFile = args[3];

		Vocabulary vocab = Vocabulary.load(new Path(vocabIdFile),
				FileSystem.get(new Job().getConfiguration()));

		Configuration config = new Configuration();
		// FileSystem fs = FileSystem.get(config);
		Path path = new Path(inFile);
		// SequenceFile.Reader in = new SequenceFile.Reader(fs, path, config);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				new HDFSDirInputStream(path.toString())));
		// RecordReader<Ngram, LongWritable> reader;
		PrintWriter out = new PrintWriter(outFile);

		// final WritableComparable<?> key;
		// final Writable value;
		// if (format.equals("adjusted-counts")) {
		// key = (WritableComparable<?>) reader.getKeyClass()
		// .newInstance();
		// value = (Writable) reader.getValueClass().newInstance();
		// } else {
		// throw new RuntimeException("Unrecognized data format: " + format);
		// }
		//
		// while (reader.next(key, value)) {
		// out.println(key.toString() + "\t" + value.toString());
		// }
		// reader.close();

		StringBuilder builder = new StringBuilder();
		Ngram key = new Ngram();
		LongWritable value = new LongWritable();
		FastIntArrayList ids = new FastIntArrayList(5);
		String line;
		while ((line = in.readLine()) != null) {
			
			if(true) throw new Error("TODO: Implement binary parsing here.");
			
			key.getAsIds(ids);
			// convert IDs to strings
			builder.setLength(0);
			for (int i = 0; i < ids.size(); i++) {
				String tok = vocab.toWord(ids.get(i));
				builder.append(tok);
				if (i < ids.size() - 1) {
					builder.append(' ');
				}
			}
			out.println(builder.toString() + "\t" + value);
		}
		out.close();
		in.close();
	}
}
