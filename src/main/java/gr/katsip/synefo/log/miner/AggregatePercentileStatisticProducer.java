package gr.katsip.synefo.log.miner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class AggregatePercentileStatisticProducer {

	public static void main(String[] args) throws IOException {
		int case1 = 0, case2 = 0, case3 = 0, case4 = 0, case5 = 0, allCases = 0;
		int localDisorder = 0, receivedDisorder = 0;
		int moreThanSecond = 0, lessThanSecond = 0;
		int recMoreThanSecond = 0, recLessThanSecond = 0;
		File directory = new File(args[0]);
		HashMap<String, DescriptiveStatistics> percentileStatistics = 
				new HashMap<String, DescriptiveStatistics>();
		HashMap<String, ArrayList<String>> violations = new HashMap<String, ArrayList<String>>();
		if(directory.isDirectory()) {
			File[] files = directory.listFiles();
			for(File logFile : files) {
				if(!logFile.isFile() || !logFile.getName().contains("-scale-events.log"))
					continue;
				/**
				 * Just operator name
				 */
				String operator = logFile.getName().split("_")[0];
				/**
				 * Operator name with task id
				 */
				String operatorName = logFile.getName().split("%3A")[0];
				
				DescriptiveStatistics operatorStatistics = null;
				if(percentileStatistics.containsKey(operator)) {
					operatorStatistics = percentileStatistics.get(operator);
				}else {
					operatorStatistics = new DescriptiveStatistics();
				}
				BufferedReader reader = new BufferedReader(new FileReader(logFile));
				
				String line = "";
				ArrayList<String> operatorViolations = new ArrayList<String>();
				while((line = reader.readLine()) != null) {
					String[] timestamps = line.split(",")[2].replaceAll("\\[", "").replaceAll("\\]", "").split("-");
					Long latency = Long.parseLong(line.split(",")[1].replaceAll(" ", ""));
					operatorStatistics.addValue(latency);
					Long localThree = Long.parseLong(timestamps[0]);
					Long localTwo = Long.parseLong(timestamps[1]);
					Long localOne = Long.parseLong(timestamps[2]);
					Long receivedThree = Long.parseLong(timestamps[3]);
					Long receivedTwo = Long.parseLong(timestamps[4]);
					Long receivedOne = Long.parseLong(timestamps[5]);
					if(localOne < receivedOne)
						case1 += 1;
					if(localTwo < receivedTwo)
						case2 += 1;
					if(localThree < receivedThree)
						case3 += 1;
					if(localOne > receivedTwo)
						case4 += 1;
					if(localTwo > receivedThree)
						case5 += 1;
					allCases += 1;
					if(localOne > localTwo || localOne > localThree || localTwo > localThree)
						localDisorder += 1;
					if(receivedOne > receivedTwo || receivedOne > receivedThree || receivedTwo > receivedThree)
						receivedDisorder += 1;
					if(Math.abs(localThree - localTwo) > 1000 || Math.abs(localTwo - localOne) > 1000) {
						moreThanSecond += 1;
					}
					if(Math.abs(localThree - localTwo) < 1000 || Math.abs(localTwo - localOne) < 1000) {
						lessThanSecond += 1;
					}
					if(Math.abs(receivedThree - receivedTwo) > 1000 || Math.abs(receivedTwo - receivedOne) > 1000) {
						recMoreThanSecond += 1;
					}
					if(Math.abs(receivedThree - receivedTwo) < 1000) {
						recLessThanSecond += 1;
						System.out.println("2");
						operatorViolations.add(Arrays.toString(timestamps));
					}
					if(Math.abs(receivedTwo - receivedOne) < 1000) {
						System.out.println("1");
					}
				}
				violations.put(operatorName, operatorViolations);
				reader.close();
				percentileStatistics.put(operator, operatorStatistics);
			}
			File aggregateOutputFile = new File(args[0] + File.separator + "percentile-stats.txt");
			if(aggregateOutputFile.exists())
				aggregateOutputFile.delete();
			aggregateOutputFile.createNewFile();
			PrintWriter writer = new PrintWriter(aggregateOutputFile);
			Iterator<Entry<String, DescriptiveStatistics>> itr = percentileStatistics.entrySet().iterator();
			while(itr.hasNext()) {
				Entry<String, DescriptiveStatistics> op = itr.next();
				DescriptiveStatistics stats = op.getValue();
				System.out.println("Operator: " + op.getKey());
				writer.println("Operator: " + op.getKey());
				System.out.println("25% percentile: " + stats.getPercentile(25));
				writer.println("25% percentile: " + stats.getPercentile(25));
				System.out.println("50% percentile: " + stats.getPercentile(50));
				writer.println("50% percentile: " + stats.getPercentile(50));
				System.out.println("75% percentile: " + stats.getPercentile(75));
				writer.println("75% percentile: " + stats.getPercentile(75));
				System.out.println("90% percentile: " + stats.getPercentile(90));
				writer.println("90% percentile: " + stats.getPercentile(90));
			}
			writer.flush();
			writer.close();
		}
		System.out.println("case-1: " + case1 + ", case-2: " + case2 + ", case-3: " + case3 + ", case-4: " + case4 + ", case-5: " + case5);
		System.out.println("Local disorder: " + localDisorder + ", received disorder: " + receivedDisorder);
		System.out.println("More than second: " + moreThanSecond + ", less than second: " + lessThanSecond);
		System.out.println("(Rec) More than second: " + recMoreThanSecond + ", (Rec) less than second: " + recLessThanSecond);
		System.out.println("all cases: " + allCases);
		File violationsFile = new File(args[0] + File.separator + "violations.txt");
		if(violationsFile.exists())
			violationsFile.createNewFile();
		PrintWriter writer = new PrintWriter(violationsFile);
		Iterator<Entry<String, ArrayList<String>>> itr2 = violations.entrySet().iterator();
		while(itr2.hasNext()) {
			Entry<String, ArrayList<String>> op = itr2.next();
			System.out.println("Violations for " + op.getKey() + " are reported.");
			writer.println("####" + op.getKey() + "####");
			for(String violation : op.getValue()) {
				writer.println(violation);
			}
			writer.println("**************************");
		}
		writer.close();
	}

}
