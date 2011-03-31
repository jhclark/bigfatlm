package bigfat.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import com.google.common.base.Joiner;

public class SortUtils {

	private static final Logger log = Logger.getLogger(SortUtils.class.getName());

	public static void sortInPlace(File file, int field, boolean numeric) throws IOException,
			InterruptedException {

		String strNumeric = numeric ? "-n" : "";
		ProcessBuilder pb =
				new ProcessBuilder("sort", strNumeric, "-r", "-k" + field, "-t\t", "-o",
						file.getAbsolutePath(), file.getAbsolutePath());
		String cmd = Joiner.on(" ").join(pb.command());
		log.info("Running external sort: " + cmd);
		Process proc = pb.start();
		int result = proc.waitFor();
		if (result != 0) {
			StringBuilder builder = new StringBuilder();
			BufferedReader err = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			String line;
			while ((line = err.readLine()) != null) {
				builder.append(line + "\n");
			}
			err.close();
			throw new RuntimeException("External sort command failed: " + cmd + "\n"
					+ builder.toString());
		}
	}
}
