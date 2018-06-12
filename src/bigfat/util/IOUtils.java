package bigfat.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import bigfat.util.MultiMemberGZIPInputStream;

public class IOUtils {
	public static BufferedReader getBufferedReader(File file) throws FileNotFoundException,
			IOException {
		if (file.getName().endsWith(".gz")) {
			return new BufferedReader(new InputStreamReader(new MultiMemberGZIPInputStream(
					new FileInputStream(file))));
		} else {
			return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		}
	}
}
