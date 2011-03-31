package bigfat.interpolate;

import jannopts.Option;
import jannopts.validators.PathCheck;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import bigfat.HadoopIteration;

import com.google.common.collect.ImmutableList;

public class OptimizeInterpolateModelsIteration implements HadoopIteration {

	@Option(shortName = "c", longName = "corpusIn", usage = "Local input path to corpus on which weights will be tuned")
	@PathCheck(exists = true)
	String corpusIn;

	@Option(shortName = "m", longName = "modelsIn", usage = "Local input paths (semicolon-delimited) to models with correct uninterpolated (with regard to models) probabilities and arbitrary backoffs", arrayDelim = ";")
	@PathCheck(exists = true)
	String[] modelsIn;

	@Override
	public Iterable<Class<?>> getConfigurables() {
		return ImmutableList.of();
	}

	@Override
	public void run(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		List<String> models = new ArrayList<String>(modelsIn.length);

		for (int i = 0; i < modelsIn.length; i++) {
			String path = modelsIn[i].trim();
			if (path.length() > 0) {
				models.add(path);
				System.err.println("Model " + (i + 1) + ": " + path);
			}
		}

		boolean readRawFormat = true;
		InterpolationWeightOptimizer.interpolate(new File(corpusIn), models, readRawFormat);
	}

}
