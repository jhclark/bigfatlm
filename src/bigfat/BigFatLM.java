package bigfat;

import jannopts.ConfigurationException;
import jannopts.Configurator;
import jannopts.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigfat.hadoop.HadoopConfigApater;
import bigfat.interpolate.OptimizeInterpolateModelsIteration;
import bigfat.interpolate.step1.AttachBackoffsIteration;
import bigfat.interpolate.step2.InterpolateModelsIteration;
import bigfat.step1.VocabIteration;
import bigfat.step2.ExtractIteration;
import bigfat.step3.DiscountsIteration;
import bigfat.step4.UninterpolatedIteration;
import bigfat.step5.InterpolateOrdersIteration;
import bigfat.step6.RenormalizeBackoffsIteration;
import bigfat.step7.ArpaFilterIteration;
import bigfat.step7.ArpaMergeIteration;
import bigfat.step7.DArpaIteration;

import com.google.common.collect.ImmutableMap;

public class BigFatLM extends Configured implements Tool {

	public static final String PROGRAM_NAME = "BigFatLM";
	public static final int ZEROTON_ID = 0;
	public static final String UNK = "<unk>";
	public static final int UNK_ID = -1;
	public static final String BOS = "<s>";
	public static final int BOS_ID = -2;
	public static final String EOS = "</s>";
	public static final int EOS_ID = -3;
	public static final float LOG_PROB_OF_ZERO = -99;

	@Option(shortName = "v", longName = "verbosity", usage = "Verbosity level", defaultValue = "0")
	public static int verbosity;

	private static final ImmutableMap<String, HadoopIteration> modules = new ImmutableMap.Builder<String, HadoopIteration>()
			.put("vocab", new VocabIteration())
			.put("extract", new ExtractIteration())
			.put("discounts", new DiscountsIteration())
			.put("uninterp", new UninterpolatedIteration())
			.put("interpOrders", new InterpolateOrdersIteration())
			.put("renorm", new RenormalizeBackoffsIteration())
			.put("darpa", new DArpaIteration())
			.put("filter", new ArpaFilterIteration())
			.put("mergearpa", new ArpaMergeIteration())
			.put("findModelInterpWeights",
					new OptimizeInterpolateModelsIteration())
			.put("makeModelVectors", new AttachBackoffsIteration())
			.put("interpModels", new InterpolateModelsIteration()).build();

	public int run(String[] args) throws Exception {

		if (args.length == 0 || !modules.keySet().contains(args[0])) {
			System.err.println("Usage: program <module_name>");
			System.err.println("Available modules: "
					+ modules.keySet().toString());
			System.exit(1);
		} else {
			String moduleName = args[0];
			HadoopIteration module = modules.get(moduleName);
			Configurator opts = new Configurator().withProgramHeader(
					PROGRAM_NAME + " V0.1\nBy Jonathan Clark")
					.withModuleOptions(moduleName, module.getClass());

			for (Class<?> configurable : module.getConfigurables()) {
				opts.withModuleOptions(moduleName, configurable);
			}

			try {
				opts.readFrom(args);
				opts.configure(module);
			} catch (ConfigurationException e) {
				opts.printUsageTo(System.err);
				System.err.println("ERROR: " + e.getMessage() + "\n");
				System.exit(1);
			}

			Configuration hadoopConf = this.getConf();

			// TODO: Move this check to a member of each iteration
			CompressionType compression = getCompressionType(module, hadoopConf);
			if (compression == CompressionType.NONE) {
				System.err.println("Using no compression since "
						+ module.getClass().getSimpleName()
						+ " module/iteration doesn't support compression...");
				
			} else if (compression == CompressionType.GZIP) {
				hadoopConf.set("mapred.output.compression.codec",
						"org.apache.hadoop.io.compress.GzipCodec");
				hadoopConf.setBoolean("mapred.output.compress", true);
				hadoopConf.set("mapred.output.compression.type", "BLOCK");

			} else if(compression == CompressionType.BZIP) {
				hadoopConf.set("mapred.output.compression.codec",
						"org.apache.hadoop.io.compress.BZip2Codec");
				hadoopConf.setBoolean("mapred.output.compress", true);
				hadoopConf.set("mapred.output.compression.type", "BLOCK");
				System.err
						.println("WARNING: Using bzip2 compression since native gzip compression libraries are not available!");

			} else {
				throw new Error("Unrecognized compression type: " + compression);
			}

			HadoopConfigApater.dumpToHadoopConfig(opts, hadoopConf);
			module.run(hadoopConf);
		}

		return 0;
	}

	public enum CompressionType {
		NONE, GZIP, BZIP
	};

	public static CompressionType getCompressionType(HadoopIteration it, Configuration hadoopConf) {
		
		boolean gzipCompress = NativeCodeLoader.isNativeCodeLoaded()
		&& ZlibFactory.isNativeZlibLoaded(hadoopConf);
		if (it instanceof DiscountsIteration
				|| it instanceof VocabIteration) {
			return CompressionType.NONE;
		} else if(gzipCompress) {
			return CompressionType.GZIP;
		} else {
			return CompressionType.BZIP;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BigFatLM(), args);
		System.exit(res);
	}
}
