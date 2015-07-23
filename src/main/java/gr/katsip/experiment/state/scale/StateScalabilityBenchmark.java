package gr.katsip.experiment.state.scale;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.Order;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StateScalabilityBenchmark {

	private long window;
	
	private long slide;
	
	private String orderFile;
	
	private String lineitemFile;
	
	/**
	 * constructor
	 * @param window the window size in seconds
	 * @param slide the slide of the window in seconds
	 */
	public StateScalabilityBenchmark(long window, long slide, String orderFile, String lineitemFile) {
		this.window = window;
		this.slide = slide;
		this.orderFile = orderFile;
		this.lineitemFile = lineitemFile;
	}
	
	public void benchmark() throws IOException {
		SlidingWindowJoin orderSlidingJoin = new SlidingWindowJoin(window, slide, 
				new Fields(Order.query5Schema), Order.query5Schema[0], "order", "lineitem");
		SlidingWindowJoin lineitemSlidingJoin = new SlidingWindowJoin(window, slide, 
				new Fields(LineItem.query5Schema), LineItem.query5Schema[0], "lineitem", "order");
		File orders = new File(orderFile);
		BufferedReader reader = new BufferedReader(new FileReader(orders));
		String line = null;
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values order = new Values();
			order.add(attributes[0]);
			order.add(attributes[1]);
			order.add(attributes[4]);
			orderSlidingJoin.insertTuple(System.currentTimeMillis(), order);
		}
		reader.close();
		
		DescriptiveStatistics lineitemInsertStatistics = new DescriptiveStatistics();
		DescriptiveStatistics lineOrderJoinStatistics = new DescriptiveStatistics();
		File lineitems = new File(lineitemFile);
		reader = new BufferedReader(new FileReader(lineitems));
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values lineitem = new Values();
			lineitem.add(attributes[0]);
			lineitem.add(attributes[2]);
			lineitem.add(attributes[5]);
			lineitem.add(attributes[6]);
			long currentTimestamp = System.currentTimeMillis();
			long insertStart = System.nanoTime();
			lineitemSlidingJoin.insertTuple(currentTimestamp, lineitem);
			long insertEnd = System.nanoTime();
			long joinStart = System.nanoTime();
			orderSlidingJoin.joinTuple(currentTimestamp, lineitem, new Fields(LineItem.query5Schema), "L_ORDERKEY");
			long joinEnd = System.nanoTime();
			lineitemInsertStatistics.addValue((insertEnd - insertStart));
			lineOrderJoinStatistics.addValue((joinEnd - joinStart));
		}
		reader.close();
		System.out.println("+++ Insert Operation +++");
		System.out.println("Average: " + (lineitemInsertStatistics.getSum() / lineitemInsertStatistics.getN()) + " nsec");
		System.out.println("Mean: " + lineitemInsertStatistics.getMean() + " nsec");
		System.out.println("Min: " + lineitemInsertStatistics.getMin() + " nsec");
		System.out.println("Max: " + lineitemInsertStatistics.getMax() + " nsec");
		System.out.println("25% Percentile: " + lineitemInsertStatistics.getPercentile(25) + " nsec");
		System.out.println("50% Percentile: " + lineitemInsertStatistics.getPercentile(50) + " nsec");
		System.out.println("75% Percentile: " + lineitemInsertStatistics.getPercentile(75) + " nsec");
		System.out.println("99% Percentile: " + lineitemInsertStatistics.getPercentile(99) + " nsec");
		System.out.println("+++ Join Operation +++");
		System.out.println("Average: " + (lineOrderJoinStatistics.getSum() / lineOrderJoinStatistics.getN()) + " nsec");
		System.out.println("Mean: " + lineOrderJoinStatistics.getMean() + " nsec");
		System.out.println("Min: " + lineOrderJoinStatistics.getMin() + " nsec");
		System.out.println("Max: " + lineOrderJoinStatistics.getMax() + " nsec");
		System.out.println("25% Percentile: " + lineOrderJoinStatistics.getPercentile(25) + " nsec");
		System.out.println("50% Percentile: " + lineOrderJoinStatistics.getPercentile(50) + " nsec");
		System.out.println("75% Percentile: " + lineOrderJoinStatistics.getPercentile(75) + " nsec");
		System.out.println("99% Percentile: " + lineOrderJoinStatistics.getPercentile(99) + " nsec");
	}

}
