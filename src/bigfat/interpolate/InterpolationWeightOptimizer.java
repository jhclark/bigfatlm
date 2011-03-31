package bigfat.interpolate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class InterpolationWeightOptimizer {

	public static double[] findInterpolationWeights(double[][] probs,
			double epsilon) {

		int numLMs = probs.length;
		int numWords = probs[0].length;
		double[] weights = new double[numLMs];

		// initialize weights to uniform
		for (int i = 0; i < weights.length; i++) {
			weights[i] = 1.0f / numLMs;
		}

		// TODO: Calculate LL and perplexity of each component LM
		for (int i = 0; i < numLMs; i++) {
			double ll = calcLL(probs[i]);
			double pp = calcPerplexity(probs[i]);
			System.err.println("LM " + (i + 1) + ": LL=" + ll + "; PP=" + pp);
		}

		// run degenerate EM to find weights
		boolean converged = false;
		int i = 0;
		double prevLogProb = calcLL(probs, weights);
		double prevPerplexity = calcPerplexity(probs, weights);
		System.err.println("Iteration " + i + " LL=" + prevLogProb + "; PP="
				+ prevPerplexity + "; Weights = " + Arrays.toString(weights));

		while (!converged) {

			// TODO: Check sanity of LM inference code
			// TODO: Move these calculations into log space?
			double[] newWeights = new double[weights.length];
			for (int jWeight = 0; jWeight < weights.length; jWeight++) {
				double sumOverCorpus = 0.0f;
				for (int iWord = 0; iWord < numWords; iWord++) {
					double denomSumOverLMs = 0.0f;
					for (int kLM = 0; kLM < numLMs; kLM++) {
						denomSumOverLMs += weights[kLM] * probs[kLM][iWord];
					}
					sumOverCorpus += weights[jWeight] * probs[jWeight][iWord]
							/ denomSumOverLMs;
				}
				newWeights[jWeight] += sumOverCorpus / numWords;
			}

			double logProb = calcLL(probs, newWeights);
			double perplexity = calcPerplexity(probs, newWeights);
			double deltaPP = prevPerplexity - perplexity;

			// log prob will always decrease...

			i++;
			System.err.println("Iteration " + i + " LL=" + logProb + "; PP="
					+ perplexity + "; DeltaPP = " + deltaPP);
			System.err.println("Weights " + Arrays.toString(weights));

			if (deltaPP < epsilon) {
				System.err.println("Change less than " + epsilon
						+ ". Converged.");
				converged = true;
				weights = newWeights;
			} else {
				prevLogProb = logProb;
				prevPerplexity = perplexity;
				weights = newWeights;
			}
		}

		return weights;
	}

	// calculate perplexity for a single LM
	private static double calcLL(double[] probs) {
		return calcLL(new double[][] { probs }, new double[] { 1.0f });
	}

	private static double calcPerplexity(double[] probs) {
		return calcPerplexity(new double[][] { probs }, new double[] { 1.0f });
	}

	private static double calcLL(double[][] probs, double[] weights) {

		int numLMs = probs.length;
		int numWords = probs[0].length;

		double avgLogProbOverCorpus = 0.0f;
		for (int iWord = 0; iWord < numWords; iWord++) {
			double sumOverLMs = 0.0f;
			for (int kLM = 0; kLM < numLMs; kLM++) {
				sumOverLMs += weights[kLM] * probs[kLM][iWord];
			}
			avgLogProbOverCorpus += Math.log(sumOverLMs) / numWords;
		}
		return avgLogProbOverCorpus;
	}

	private static double calcPerplexity(double[][] probs, double[] weights) {

		int numLMs = probs.length;
		int numWords = probs[0].length;

		double overCorpus = 0.0f;
		for (int iWord = 0; iWord < numWords; iWord++) {
			double sumOverLMs = 0.0f;
			for (int kLM = 0; kLM < numLMs; kLM++) {
				sumOverLMs += weights[kLM] * probs[kLM][iWord];
			}
			overCorpus += log2(sumOverLMs);
		}

		double entropy = overCorpus / numWords;
		return Math.pow(2, -entropy);
	}

	// log base 2
	private static double log2(double x) {
		return Math.log(x) / Math.log(2);
	}

	public static double[][] loadLMs(int numWords, List<List<String>> corpus,
			List<String> lmNames, boolean readRawFormat) throws IOException {
		
		int numLMs = lmNames.size();
		double[][] probs = new double[numLMs][numWords];
		for (int i = 0; i < numLMs; i++) {
			ARPALanguageModel lm = new ARPALanguageModel();
			File lmFile = new File(lmNames.get(i));
			System.err.println("Loading LM " + lmFile + "...");
			lm.load(lmFile, readRawFormat);

			System.err.println("Scoring corpus with " + lmFile);
			// TODO: Check for same # of words
			Iterator<Double> stream = new PerplexityUtils.ProbabilityStream(
					corpus, lm).iterator();
			for (int j = 0; j < numWords; j++) {
				if (!stream.hasNext()) {
					throw new RuntimeException(
							"Probability stream did not have as many probabilities as words in the corpus: "
									+ j + "/" + numWords);
				}
				probs[i][j] = stream.next();
			}
			if (stream.hasNext()) {
				throw new RuntimeException(
						"Probability stream had more probabilities than words in the corpus");
			}
		}
		return probs;
	}
	
	public static void interpolate(File corpusFile, List<String> lmNames, boolean readRawFormat) throws IOException {
		
		float EPSILON = 1e-4f;
		
		System.err.println("Loading corpus...");
		List<List<String>> corpus = PerplexityUtils
				.loadCorpus(corpusFile);
		int numWords = 0;
		int numSents = corpus.size();
		for (List<String> sent : corpus) {
			numWords += sent.size();
		}
		numWords -= numSents; // don't include <s>

		double[][] probs = loadLMs(numWords, corpus, lmNames, readRawFormat);
		double[] weights = findInterpolationWeights(probs, EPSILON);
		
		for (int i = 0; i < weights.length; i++) {
			System.out.println(lmNames.get(i) + "\t" + weights[i]);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: program corpus lmFiles...");
			System.exit(1);
		}

		File corpusFile = new File(args[0]);
		List<String> lmNames = new ArrayList<String>();
		for (int i = 1; i < args.length; i++) {
			lmNames.add(args[i]);
		}
		
		boolean readRawFormat = false;
		interpolate(corpusFile, lmNames, readRawFormat);
		System.exit(0);
	}
}
