package gr.katsip.synefo.log.miner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class AggregateStatisticsMerger {

	public static void main(String[] args) throws IOException {
		File directory = new File(args[0]);
		HashMap<String, ArrayList<String>> operatorIndex = new HashMap<String, ArrayList<String>>();
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
				System.out.println("operator: " + operator + ", operatorName: " + operatorName);
				/**
				 * Keep track of the log files of different operators 
				 * participating in the same operator
				 */
				if(operatorIndex.containsKey(operator)) {
					ArrayList<String> fileHandlers = operatorIndex.get(operator);
					fileHandlers.add(operatorName);
					operatorIndex.put(operator, fileHandlers);
				}else {
					ArrayList<String> fileHandlers = new ArrayList<String>();
					fileHandlers.add(operatorName);
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
					@SuppressWarnings("unused")
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
			System.out.println("Operator Index size: " + operatorIndex.size() + ", Operator Stat Index: " + operatorStatIndex.size());
			HashMap<String, ArrayList<SummaryStatistics>> aggregateStatistics = 
					new HashMap<String, ArrayList<SummaryStatistics>>();
			Iterator<Entry<String, ArrayList<String>>> operatorItr = operatorIndex.entrySet().iterator();
			while(operatorItr.hasNext()) {
				Entry<String, ArrayList<String>> operator = operatorItr.next();
				SummaryStatistics operatorLatencyStatistics = new SummaryStatistics();
				SummaryStatistics operatorThroughputStatistics = new SummaryStatistics();
				ArrayList<String> operators = operator.getValue();
				for(String op : operators) {
					ArrayList<SummaryStatistics> operatorStatistics = operatorStatIndex.get(op);
					operatorLatencyStatistics.addValue(
							(operatorStatistics.get(3).getSum() / operatorStatistics.get(3).getN()));
					operatorThroughputStatistics.addValue(
							(operatorStatistics.get(5).getSum() / operatorStatistics.get(5).getN()));
				}
				ArrayList<SummaryStatistics> operatorAggregateStatistics = new ArrayList<SummaryStatistics>();
				operatorAggregateStatistics.add(operatorLatencyStatistics);
				operatorAggregateStatistics.add(operatorThroughputStatistics);
				aggregateStatistics.put(operator.getKey(), operatorAggregateStatistics);
			}
			/**
			 * At this point, for each operator the average latency and average throughput are stored.
			 * For the final aggregation, for each operator we need the average of all the latencies reported
			 * and the sum of all average throughputs reported.
			 */
			File aggregateOutputFile = new File(args[0] + File.separator + "aggregate-stats.txt");
			if(aggregateOutputFile.exists())
				aggregateOutputFile.delete();
			aggregateOutputFile.createNewFile();
			PrintWriter writer = new PrintWriter(aggregateOutputFile);
			Iterator<Entry<String, ArrayList<SummaryStatistics>>> statItr = 
					aggregateStatistics.entrySet().iterator();
			while(statItr.hasNext()) {
				Entry<String, ArrayList<SummaryStatistics>> stat = statItr.next();
				ArrayList<SummaryStatistics> stats = stat.getValue();
				writer.println(stat.getKey() + "," + (stats.get(0).getSum() / stats.get(0).getN()) + "," + (stats.get(1).getSum()) );
			}
			writer.flush();
			writer.close();
		}
	}

}
