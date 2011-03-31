package bigfat.hadoop;

import java.util.Properties;

import jannopts.Configurator;

import org.apache.hadoop.conf.Configuration;

// TODO: Move this back to jannopts
public class HadoopConfigApater {
	public static void dumpToHadoopConfig(Configurator configurator, Configuration hadoopConfig) {
		Properties props = configurator.getProperties();
		for (Object k : props.keySet()) {
			String key = (String) k;
			String value = props.getProperty(key);
			hadoopConfig.set(key, value);
		}
	}
}
