package bigfat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import bigfat.datastructs.Ngram;
import bigfat.step1.Vocabulary;

public class DataViewer {
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		Path dataPath = new Path(args[0]);
		FileSystem fs = dataPath.getFileSystem(conf);
		SequenceFile.Reader read = new SequenceFile.Reader(fs, dataPath, conf);
		
		if(args.length == 2) {
			Path vocabPath = new Path(args[1]);
			Vocabulary vocab = Vocabulary.load(vocabPath, fs);
			Ngram.setVocabForDebug(vocab);
		}
		
		Object key = null;
		Object value = null;
		while( (key = read.next(key)) != null ) {
			value = read.getCurrentValue(value);
			System.out.println(key.toString() + "\t" + value.toString());
		}
	}
}
