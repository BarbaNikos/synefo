package gr.katsip.synefo.log.miner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MetricsFileCleaner {

	public static void main(String[] args) throws Exception {
        HashMap<String, List<Integer>> taskNameToIdentifiersMap = new HashMap<>();
        HashMap<Integer, HashMap<String, List<Double>>> taskIdentifierToMetricsMap = new HashMap<>();
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
                /**
                 * Format of each line (separated by tabs):
                 * date-time timestamp superviror:port task-id:task-name metric-name metric-value
                 */
				while((line = reader.readLine()) != null) {
                    String taskName = "";
                    Integer taskIdentifier = -1;
                    String[] lineTokens = line.split("\t");
                    taskName = lineTokens[3].split(":")[1];
                    taskIdentifier = Integer.parseInt(lineTokens[3].split(":")[0]);
                    if(taskNameToIdentifiersMap.containsKey(taskName)) {
                        List<Integer> identifiers = taskNameToIdentifiersMap.get(taskName);
                        identifiers.add(taskIdentifier);
                        taskNameToIdentifiersMap.put(taskName, identifiers);
                    }else {
                        List<Integer> identifiers = new ArrayList<>();
                        identifiers.add(taskIdentifier);
                        taskNameToIdentifiersMap.put(taskName, identifiers);
                    }
					for(String metric : metrics) {
						if(metric.equals(lineTokens[4])) {
                            /**
                             * Maintain the metric to the appropriate list
                             */
                            double value = Double.parseDouble(lineTokens[5]);
                            HashMap<String, List<Double>> metricToValuesMap = null;
                            if(taskIdentifierToMetricsMap.containsKey(taskIdentifier)) {
                                metricToValuesMap = taskIdentifierToMetricsMap.get(taskIdentifier);
                            }else {
                                metricToValuesMap = new HashMap<>();
                            }
                            List<Double> metricValues = null;
                            if(metricToValuesMap.containsKey(metric)) {
                                metricValues = metricToValuesMap.get(metric);
                            }else {
                                metricValues = new ArrayList<>();
                            }
                            metricValues.add(value);
                            metricToValuesMap.put(metric, metricValues);
                            taskIdentifierToMetricsMap.put(taskIdentifier, metricToValuesMap);
                        }
					}
				}
				reader.close();
				writer.close();
			}
		}
	}

}
