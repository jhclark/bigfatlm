package bigfat.util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class StringUtils {

	public static String[] tokenize(String str, String delims) {
		StringTokenizer tok = new StringTokenizer(str, delims);
		String[] toks = new String[tok.countTokens()];
		for (int i = 0; i < toks.length; i++) {
			toks[i] = tok.nextToken();
		}
		return toks;
	}

	public static int count(String ngram, char c) {
		int n = 0;
		for (int i = 0; i < ngram.length(); i++) {
			if (ngram.charAt(i) == c) {
				n++;
			}
		}
		return n;
	}
	
	public static String getSubstringBefore(String str, char c) {
		int i = str.indexOf(c);
		if(i == -1) {
			throw new RuntimeException(c + " not found in " + str);
		}
		return str.substring(0, i);
	}

	public static String getSubstringAfterLast(String str, char c) {
		int i = str.lastIndexOf(c);
		if(i == -1) {
			throw new RuntimeException(c + " not found in " + str);
		}
		return str.substring(i+1);
	}

	public static String getSubstringAfter(String str, String c) {
		int i = str.indexOf(c);
		if(i == -1) {
			throw new RuntimeException(c + " not found in " + str);
		}
		return str.substring(i + c.length());
	}

	public static List<String> tokenizeList(String str, String delims) {
		StringTokenizer tok = new StringTokenizer(str, delims);
		int numToks = tok.countTokens();
		List<String> toks = new ArrayList<String>(numToks);
		for (int i = 0; i < numToks; i++) {
			toks.add(tok.nextToken());
		}
		return toks;
	}
}
