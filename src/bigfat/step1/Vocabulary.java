package bigfat.step1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import bigfat.BigFatLM;
import bigfat.util.StringUtils;

import com.google.common.io.Files;

public class Vocabulary {

	private final String[] vocab;

	private Vocabulary(int size) {
		this.vocab = new String[size];
	}

	public String toWord(int i) {
		if(i == BigFatLM.ZEROTON_ID) {
			return BigFatLM.UNK;
		} else if (i == BigFatLM.UNK_ID) {
			return BigFatLM.UNK;
		} else if (i == BigFatLM.BOS_ID) {
			return BigFatLM.BOS;
		} else if (i == BigFatLM.EOS_ID) {
			return BigFatLM.EOS;
		} else {
			return vocab[i-1];
		}
	}

	public static Vocabulary load(Path vocabFile, FileSystem fs) throws IOException {

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(vocabFile)));
		int size = 0;
		String line;
		while ((line = in.readLine()) != null) {
			String[] columns = StringUtils.tokenize(line, "\t");
			int n = Integer.parseInt(columns[1]);
			if (n > 0) {
				size++;
			}
		}
		in.close();

		in = new BufferedReader(new InputStreamReader(fs.open(vocabFile)));
		Vocabulary v = new Vocabulary(size);
		while ((line = in.readLine()) != null) {
			String[] columns = StringUtils.tokenize(line, "\t");
			String tok = columns[0];
			int n = Integer.parseInt(columns[1]);
			if (n > 0) {
				v.vocab[n-1] = tok;
			}
		}
		in.close();
		return v;
	}
}
