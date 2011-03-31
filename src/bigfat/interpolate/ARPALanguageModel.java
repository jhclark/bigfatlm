package bigfat.interpolate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bigfat.util.StringUtils;

/**
 * A naive Hash-based LM table for performing LM inference and loading
 * <b>small</a> ARPA-formatted language models. This is useful in cases such as
 * calculating perplexity on a small tuning set, as is done for interpolation.
 * For other use cases, you should probably use a C++ library unless you want to
 * fill up RAM.
 */
public class ARPALanguageModel {

	private static class LMEntry {
		public final double logProb;
		public final double logBackoff;

		public LMEntry(double logProb, double logBackoff) {
			this.logProb = logProb;
			this.logBackoff = logBackoff;
		}
	}

	List<Map<List<String>, LMEntry>> table;
	private int order;
	private double unkLogProb;

	public void load(File f, boolean readRawFormat) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(f));

		if (readRawFormat) {
			readRawFormat(in);
		} else {
			readArpa(in);
		}

		if (table == null) {
			throw new RuntimeException("No entries for LM "
					+ f.getAbsolutePath());
		}

		LMEntry unkEntry = table.get(0).get(Collections.singletonList("<unk>"));
		if (unkEntry == null) {
			throw new RuntimeException("<unk> entry not found.");
		}
		this.unkLogProb = unkEntry.logProb;
	}

	private void readRawFormat(BufferedReader in) throws NumberFormatException,
			IOException {

		table = new ArrayList<Map<List<String>, LMEntry>>();

		String line;
		while ((line = in.readLine()) != null) {
			String[] columns = StringUtils.tokenize(line, "\t");
			double logProb = Double.parseDouble(columns[1]);
			List<String> ngram = StringUtils.tokenizeList(columns[0], " ");
			double logBackoff = 0.0;
			if (columns.length == 3) {
				logBackoff = Double.parseDouble(columns[2]);
			}

			int order = ngram.size();
			this.order = Math.max(this.order, order);
			while (table.size() < order) {
				table.add(new HashMap<List<String>, ARPALanguageModel.LMEntry>(
						5000));
			}

			table.get(order - 1).put(ngram, new LMEntry(logProb, logBackoff));
		}
	}

	private void readArpa(BufferedReader in) throws IOException {
		List<Integer> header = new ArrayList<Integer>();

		boolean inHeader = false;
		boolean inData = false;
		int curOrder = 0;

		String line;
		while ((line = in.readLine()) != null) {
			if (line.startsWith("\\data\\")) {
				inHeader = true;
			} else if (inHeader) {
				if (line.trim().isEmpty()) {
					inHeader = false;
					inData = true;

					// done reading header
					order = header.size();
					table = new ArrayList<Map<List<String>, LMEntry>>(order);
					for (int iOrder = 0; iOrder < header.size(); iOrder++) {
						int numEntries = header.get(iOrder);
						table.add(new HashMap<List<String>, ARPALanguageModel.LMEntry>(
								numEntries));
					}

				} else if (line.startsWith("ngram ")) {
					String after = StringUtils
							.getSubstringAfter(line, "ngram ");
					String[] toks = StringUtils.tokenize(after, "=");
					int forOrder = Integer.parseInt(toks[0]);
					int count = Integer.parseInt(toks[1]);
					assert header.size() + 1 == forOrder;
					header.add(count);
				}
			} else if (inData) {
				// parse when we hit new orders
				// parse n-grams
				if (line.trim().isEmpty()) {
					continue;
				} else if (line.trim().equals("\\end\\")) {
					break;
				} else if (line.startsWith("\\") && line.endsWith("-grams:")) {
					String ord = line.substring(1, line.indexOf('-'));
					curOrder = Integer.parseInt(ord);
				} else {
					String[] columns = StringUtils.tokenize(line, "\t");
					double logProb = Double.parseDouble(columns[0]);
					List<String> ngram = StringUtils.tokenizeList(columns[1],
							" ");
					double logBackoff = 0.0;
					if (columns.length == 3) {
						logBackoff = Double.parseDouble(columns[2]);
					}
					table.get(ngram.size() - 1).put(ngram,
							new LMEntry(logProb, logBackoff));
				}
			}
		}
	}

	public double getProb(List<String> ngram) {
		assert ngram.size() > 0 : "zero length n-gram";

		double log10Prob = getLogProb(ngram, 0.0f);
		return Math.pow(10, log10Prob);
	}

	private double getLogProb(List<String> ngram, double prevBackoff) {
		assert ngram.size() > 0 : "zero length n-gram";

		LMEntry entry = table.get(ngram.size() - 1).get(ngram);
		if (entry != null) {
			return entry.logProb + prevBackoff;
		} else {
			if (ngram.size() == 1) {
				return unkLogProb + prevBackoff;
			} else {
				List<String> shorterNgram = ngram.subList(1, ngram.size());

				List<String> backoffContext = ngram
						.subList(0, ngram.size() - 1);
				LMEntry backoffEntry = table.get(backoffContext.size() - 1)
						.get(backoffContext);
				double myBackoff = 0.0f;
				if (backoffEntry != null) {
					myBackoff = backoffEntry.logBackoff;
				}

				return getLogProb(shorterNgram, myBackoff + prevBackoff);
			}
		}
	}

	public int order() {
		return order;
	}
}
