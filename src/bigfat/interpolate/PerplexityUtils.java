package bigfat.interpolate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import bigfat.BigFatLM;
import bigfat.util.StringUtils;

import com.google.common.base.Joiner;

@Deprecated
public class PerplexityUtils {
	
	public static final boolean DEBUG = false;

	public static class ProbabilityStream implements Iterable<Double> {
		private List<List<String>> sents;
		private ARPALanguageModel lm;

		public ProbabilityStream(List<List<String>> sents, ARPALanguageModel lm) {
			this.sents = sents;
			this.lm = lm;
		}

		@Override
		public Iterator<Double> iterator() {
			return new Iterator<Double>() {

				private int iSent = 0;
				private int iTok = 2;

				@Override
				public boolean hasNext() {
					return (iSent + 1) < sents.size()
							|| (iSent < sents.size() && iTok <= sents.get(iSent).size());
				}

				@Override
				public Double next() {
					assert lm.order() > 1;

					// score the current token given its history
					List<String> curSent = sents.get(iSent);
					int start = Math.max(0, iTok - lm.order());
					int end = Math.max(0, iTok);
					List<String> ngram = curSent.subList(start, end);
					double score = lm.getProb(ngram);
					if (DEBUG) {
						System.err.println("P(" + Joiner.on(' ').join(ngram) + ") = " + score);
					}

					// move forward
					iTok++;
					if (iTok > sents.get(iSent).size()) {
						iTok = 2;
						iSent++;
					}

					return score;
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
	}

	public static List<List<String>> loadCorpus(File f) throws IOException {
		List<List<String>> corpus = new ArrayList<List<String>>();
		BufferedReader in = new BufferedReader(new FileReader(f));
		String line;
		while ((line = in.readLine()) != null) {
			if (line.trim().isEmpty()) {
				continue;
			}

			List<String> toks = StringUtils.tokenizeList(line, " ");
			List<String> sent = new ArrayList<String>(toks.size() + 2);
			sent.add(BigFatLM.BOS);
			sent.addAll(toks);
			sent.add(BigFatLM.EOS);
			corpus.add(sent);
		}
		in.close();
		return corpus;
	}

	public static double getPerplexity(Iterator<Double> probStream) {
		double sum = 0.0f;

		return sum;
	}
}
