package gr.katsip.synefo.log.miner;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.*;
import java.util.*;

/**
 * Created by katsip on 9/3/2015.
 */
public class MetricOvertimeVisualizer {

    public static void main(String[] args) throws IOException {
        HashMap<String, List<Integer>> taskNameToIdentifiersMap = new HashMap<>();
        HashMap<Integer, HashMap<String, List<Double>>> taskIdentifierToMetricsMap = new HashMap<>();
        String[] metrics = null;
        File directory = new File(args[0]);
        metrics = Arrays.copyOf(args[1].split(","), args[1].split(",").length);
        int machineDirNumber = 0;
        int metricLogFileNumber = 0;
        if(directory.isDirectory()) {
            File[] machineDirectories = directory.listFiles();
            for(File machineDirectory : machineDirectories) {
                /**
                 * Check the directory for the corresponding machine to get all the available
                 * metrics files (normally it should be only one
                 */
                if(machineDirectory.isDirectory()) {
                    machineDirNumber += 1;
                    File[] metricLogs = machineDirectory.listFiles();
                    for (File metricLog : metricLogs) {
                        if (!metricLog.isFile() || !metricLog.getName().contains(".log"))
                            continue;
                        metricLogFileNumber += 1;
                        BufferedReader reader = new BufferedReader(new FileReader(metricLog));
                        String line = "";
                        /**
                         * Format of each line (separated by tabs):
                         * date-time timestamp superviror:port task-id:task-name metric-name metric-value
                         */
                        while ((line = reader.readLine()) != null) {
                            String taskName = "";
                            Integer taskIdentifier = -1;
                            String[] lineTokens = line.split("[\\t]");
                            int taskInfoIndex = -1;
                            boolean supervisorTokenFound = false;
                            for(int i = 0; i < lineTokens.length; i++) {
                                if(lineTokens[i].contains(":") && lineTokens[i].contains("supervisor"))
                                    supervisorTokenFound = true;
                                if(lineTokens[i].contains(":") && !lineTokens[i].contains("supervisor") && supervisorTokenFound && lineTokens[i].contains("_") == false) {
                                    taskName = lineTokens[i].split(":")[1].replaceAll("\\s", "");
                                    taskIdentifier = Integer.parseInt(lineTokens[i].split(":")[0].replaceAll("\\s", ""));
                                    taskInfoIndex = i;
                                    break;
                                }
                            }
                            if(taskInfoIndex == -1)
                                continue;
                            int metricNameIndex = -1;
                            String metricName = "";
                            for(int i = taskInfoIndex + 1; i < lineTokens.length; i++) {
                                String token = lineTokens[i];
                                if(!token.equals("")) {
                                    token = token.replaceAll("\\s", "");
                                    for(String metric : metrics) {
                                        if(metric.equals(token)) {
                                            metricNameIndex = i;
                                            metricName = token;
                                            break;
                                        }
                                    }
                                }
                            }
                            if(metricNameIndex == -1)
                                continue;
                            Double metricValue = -1.0;
                            for(int i = metricNameIndex + 1; i < lineTokens.length; i++) {
                                String token = lineTokens[i];
                                if(!token.equals("")) {
                                    String token2 = token.replaceAll("\\s", "");
                                    metricValue = Double.parseDouble(token2);
                                    break;
                                }
                            }
                            /**
                             * Add the newly-found taskIdentifier
                             */
                            if (taskNameToIdentifiersMap.containsKey(taskName)) {
                                List<Integer> identifiers = taskNameToIdentifiersMap.get(taskName);
                                if(!identifiers.contains(taskIdentifier))
                                    identifiers.add(taskIdentifier);
                                taskNameToIdentifiersMap.put(taskName, identifiers);
                            } else {
                                List<Integer> identifiers = new ArrayList<>();
                                identifiers.add(taskIdentifier);
                                taskNameToIdentifiersMap.put(taskName, identifiers);
                            }
                            /**
                             * Add the newly-found metric value
                             */
                            /**
                             * Maintain the metric to the appropriate list
                             */
                            HashMap<String, List<Double>> metricToValuesMap = null;
                            if (taskIdentifierToMetricsMap.containsKey(taskIdentifier)) {
                                metricToValuesMap = taskIdentifierToMetricsMap.get(taskIdentifier);
                            } else {
                                metricToValuesMap = new HashMap<>();
                            }
                            List<Double> metricValues = null;
                            if (metricToValuesMap.containsKey(metricName)) {
                                metricValues = metricToValuesMap.get(metricName);
                            } else {
                                metricValues = new ArrayList<>();
                            }
                            metricValues.add(metricValue);
                            metricToValuesMap.put(metricName, metricValues);
                            taskIdentifierToMetricsMap.put(taskIdentifier, metricToValuesMap);
                        }
                        reader.close();
                    }
                }
            }
            System.out.println("+++SCAN STATISTICS+++");
            System.out.println("- Machine directories visited: " + machineDirNumber);
            System.out.println("- Metric Log files scanned: " + metricLogFileNumber);
            System.out.println("- Task names encountered: " + taskNameToIdentifiersMap.size());
            System.out.println("- Task identifiers encountered: " + taskIdentifierToMetricsMap.size());

            System.out.println("Task names to task Identifiers:");
            Iterator taskNameIterator = taskNameToIdentifiersMap.entrySet().iterator();
            while(taskNameIterator.hasNext()) {
                Map.Entry<String, List<Integer>> entry = (Map.Entry) taskNameIterator.next();
                String taskName = entry.getKey();
                List<Integer> taskIdentifiers = entry.getValue();
                System.out.println("+" + taskName + " -> " + taskIdentifiers.toString());
                /**
                 * For each task-name, aggregate all metrics for each task-id
                 */
                for(Integer task : taskIdentifiers) {
                    HashMap<String, List<Double>> taskMetrics = taskIdentifierToMetricsMap.get(task);
                    Iterator<Map.Entry<String, List<Double>>> metricIterator = taskMetrics.entrySet().iterator();
                    while (metricIterator.hasNext()) {
                        Map.Entry<String, List<Double>> pair = metricIterator.next();
                        /**
                         * Create a new file with the metric values over time
                         */
                        File taskMetric = new File(args[0] + File.separator + taskName + "_" + task + "_" + pair.getKey() + ".log");
                        if(taskMetric.exists())
                            taskMetric.delete();
                        taskMetric.createNewFile();
                        PrintWriter writer = new PrintWriter(new FileWriter(taskMetric));
                        Integer count = 0;
                        for(Double point : pair.getValue()) {
                            writer.println(count + "," + point);
                            count += 1;
                        }
                        writer.flush();
                        writer.close();
                    }
                }
            }
        }
    }
}
