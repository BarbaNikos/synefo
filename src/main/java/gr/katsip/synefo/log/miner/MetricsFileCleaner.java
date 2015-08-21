package gr.katsip.synefo.log.miner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Arrays;

public class MetricsFileCleaner {

	public static void main(String[] args) throws Exception {
		String[] metrics = null;
		File directory = new File(args[0]);
		metrics = Arrays.copyOf(args[1].split(","), args[1].split(",").length);
		if(directory.isDirectory()) {
			File[] files = directory.listFiles();
			for(File metricLog : files) {
				if(!metricLog.isFile() || !metricLog.getName().contains(".log"))
					continue;
				BufferedReader reader = new BufferedReader(new FileReader(metricLog));
				File modifiedOutputFile = new File(args[0] + File.separator + "modified-" + 
						metricLog.getName());
				if(modifiedOutputFile.exists())
					modifiedOutputFile.delete();
				modifiedOutputFile.createNewFile();
				PrintWriter writer = new PrintWriter(modifiedOutputFile);
				String line = "";
				while((line = reader.readLine()) != null) {
					if(line.contains("_"))
						continue;
					for(String metric : metrics) {
						if(line.contains(metric)) {
							//Log it in the output
							writer.println(line);
						}
					}
				}
				reader.close();
				writer.close();
			}
		}
	}

}
