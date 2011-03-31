package bigfat.interpolate.step1;

import jannopts.Option;
import jannopts.validators.HdfsPathCheck;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step5.InterpolateOrdersInfo;

/**
 * Complaints about the length of this class name can be sent to /dev/null. I'm
 * sorry, okay?
 * 
 */
public class AttachBackoffsMapper extends
		Mapper<Ngram, InterpolateOrdersInfo, Ngram, InterpolateModelsInfo> {

	@Option(shortName = "i", longName = "modelsIn2", usage = "HDFS input paths (semicolon-delimited) to models with correct uninterpolated (with regard to models) probabilities and arbitrary backoffs", arrayDelim = ";")
	@HdfsPathCheck(exists = true)
	String[] modelsIn;

	// TODO: Are we guaranteed one map task per file?
	private int myId;

	private InterpolateModelsInfo modelInfo = new InterpolateModelsInfo();

	@Override
	public void setup(Context context) {

		HadoopUtils.configure(context.getConfiguration(), this);

		try {
			// assign IDs to filenames
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Map<String, Integer> weightMap = new HashMap<String, Integer>();
			for (int i = 0; i < modelsIn.length; i++) {
				if (!modelsIn[i].trim().isEmpty()) {
					String filename = modelsIn[i];
					String absPath = getAbsolutePath(fs, filename);
					weightMap.put(absPath, i);
				}
			}

			// figure out which file we're reading
			InputSplit inputSplit = context.getInputSplit();
			if (inputSplit instanceof FileSplit) {
				FileSplit split = (FileSplit) inputSplit;
				Path absShardPath = split.getPath().makeQualified(fs);
				// we want the path of the overall model, not just the current shard
				Path absModelPath = absShardPath.getParent();
				String filename = absModelPath.toUri().getPath();

				// map that to a weight
				Integer myId = weightMap.get(filename);
				if (myId == null) {
					throw new RuntimeException("Filename " + filename
							+ " not found in weight map " + weightMap);
				}
				this.myId = myId;
				System.err.println("Assigning " + filename + " as model ID " + myId);
			} else {
				throw new RuntimeException(
						"Not a FileSplit -- cannot determine input file.");
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private String getAbsolutePath(FileSystem fs, String filename) {
		Path absModelPath = new Path(filename).makeQualified(fs);
		String absPath = absModelPath.toUri().getPath();
		return absPath;
	}

	@Override
	public void map(Ngram ngram, InterpolateOrdersInfo value,
			Context hadoopContext) {

		try {
			if (myId == 0) {
				// this is the master LM
				modelInfo.setAllInfo(myId, value.getInterpolatedProb(),
						value.getBackoffWeight());
				hadoopContext.write(ngram, modelInfo);

			} else {
				modelInfo.setProbInfo(myId, value.getInterpolatedProb());
				hadoopContext.write(ngram, modelInfo);

				modelInfo.setBackoffInfo(myId, value.getBackoffWeight());
				hadoopContext.write(ngram, modelInfo);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
