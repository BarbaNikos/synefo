package gr.katsip.synefo.log.miner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class AggregateStatisticsMerger {

	public static void main(String[] args) throws IOException {
		File directory = new File(args[0]);
		HashMap<String, ArrayList<File>> operatorIndex = new HashMap<String, ArrayList<File>>();
		HashMap<String, ArrayList<SummaryStatistics>> operatorStatIndex = 
				new HashMap<String, ArrayList<SummaryStatistics>>();
		if(directory.isDirectory()) {
			File[] files = directory.listFiles();
			for(File logFile : files) {
				if(!logFile.isFile() || !logFile.getName().contains("-stats.log"))
					continue;
				/**
				 * Just operator name
				 */
				String operator = logFile.getName().split("_")[0];
				/**
				 * Operator name with task id
				 */
				String operatorName = logFile.getName().split("%3A")[0];
				/**
				 * Keep track of the log files of different operators 
				 * participating in the same operator
				 */
				if(operatorIndex.containsKey(operator)) {
					ArrayList<File> fileHandlers = operatorIndex.get(operator);
					fileHandlers.add(logFile);
					operatorIndex.put(operator, fileHandlers);
				}else {
					ArrayList<File> fileHandlers = new ArrayList<File>();
					fileHandlers.add(logFile);
					operatorIndex.put(operator, fileHandlers);
				}
				SummaryStatistics cpuStatistics = new SummaryStatistics();
				SummaryStatistics memoryStatistics = new SummaryStatistics();
				SummaryStatistics stateSizeStatistics = new SummaryStatistics();
				SummaryStatistics latencyStatistics = new SummaryStatistics();
				SummaryStatistics operationalLatencyStatistics = new SummaryStatistics();
				SummaryStatistics throughputStatistics = new SummaryStatistics();
				BufferedReader reader = new BufferedReader(new FileReader(logFile));
				String line = "";
				while((line = reader.readLine()) != null) {
					String[] dataPoint = line.split(",");
					Long timestamp = Long.parseLong(dataPoint[0]);
					Double cpu = Double.parseDouble(dataPoint[1]);
					Double memory = Double.parseDouble(dataPoint[2]);
					Long stateSize = Long.parseLong(dataPoint[3]);
					Long latency = Long.parseLong(dataPoint[4]);
					Long operationalLatency = Long.parseLong(dataPoint[5]);
					Double throughput = Double.parseDouble(dataPoint[6]);
					cpuStatistics.addValue(cpu);
					memoryStatistics.addValue(memory);
					stateSizeStatistics.addValue(stateSize);
					latencyStatistics.addValue(latency);
					operationalLatencyStatistics.addValue(operationalLatency);
					throughputStatistics.addValue(throughput);
				}
				reader.close();
				ArrayList<SummaryStatistics> operatorStat = new ArrayList<SummaryStatistics>();
				operatorStat.add(cpuStatistics);
				operatorStat.add(memoryStatistics);
				operatorStat.add(stateSizeStatistics);
				operatorStat.add(latencyStatistics);
				operatorStat.add(operationalLatencyStatistics);
				operatorStat.add(throughputStatistics);
				operatorStatIndex.put(operatorName, operatorStat);
			}
			/**
			 * At this point:
			 * - operatorStatIndex: Contains all the statistics from each operatorName (name + task-id)
			 * - operatorIndex: Groups of operatorName (name + task-id) for each logical operation
			 */
			
		}
	}

}
