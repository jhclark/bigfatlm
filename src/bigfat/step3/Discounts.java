package bigfat.step3;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bigfat.util.StringUtils;

public class Discounts {

	double[][] discountTable;

	public double getDiscount(int order, int count) {
		if (count == 0 || order == 0) {
			return 0;
		} else {
			return discountTable[order - 1][count - 1];
		}
	}

	public static Discounts load(BufferedReader in) throws IOException {
		Discounts d = new Discounts();
		int maxOrder = 0;
		int maxCount = 0;
		List<String> lines = new ArrayList<String>(100);
		{
			String line;
			while ((line = in.readLine()) != null) {
				String[] toks = StringUtils.tokenize(line, "\t");
				int order = Integer.parseInt(toks[0]);
				int countCateg = Integer.parseInt(toks[1]);
				maxOrder = Math.max(order, maxOrder);
				maxCount = Math.max(countCateg, maxCount);
				lines.add(line);
			}
			in.close();
		}

		d.discountTable = new double[maxOrder][maxCount];
		for (String line : lines) {
			String[] toks = StringUtils.tokenize(line, "\t");
			int order = Integer.parseInt(toks[0]);
			int countCateg = Integer.parseInt(toks[1]);
			double discount = Double.parseDouble(toks[2]);
			d.discountTable[order - 1][countCateg - 1] = discount;
		}

		return d;
	}
}
