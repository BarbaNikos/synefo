package gr.katsip.experiment.state.scale;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import gr.katsip.synefo.storm.operators.relational.elastic.joiner.SlidingWindowThetaJoin;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import gr.katsip.synefo.storm.operators.relational.elastic.joiner.SlidingWindowJoin;
import gr.katsip.tpch.LineItem;
import gr.katsip.tpch.Order;
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
				new Fields(Order.schema), Order.schema[0], "order", "lineitem");
		SlidingWindowJoin lineitemSlidingJoin = new SlidingWindowJoin(window, slide, 
				new Fields(LineItem.schema), LineItem.schema[0], "lineitem", "order");
        SlidingWindowThetaJoin.ThetaCondition theta = SlidingWindowThetaJoin.ThetaCondition.LESS_THAN;
        SlidingWindowThetaJoin thetaJoin = new SlidingWindowThetaJoin(window, slide,
                new Fields(LineItem.schema), LineItem.schema[0], "lineitem", "order", theta);
		File orders = new File(orderFile);
		BufferedReader reader = new BufferedReader(new FileReader(orders));
		String line = null;
//		while((line = reader.readLine()) != null) {
//			String[] attributes = line.split("\\|");
//			Values order = new Values();
//            for (String attribute : attributes) {
//                order.add(attribute);
//            }
//			orderSlidingJoin.insertTuple(System.currentTimeMillis(), order);
//		}
		reader.close();
		DescriptiveStatistics lineitemInsertStatistics = new DescriptiveStatistics();
		DescriptiveStatistics lineOrderJoinStatistics = new DescriptiveStatistics();
		File lineitems = new File(lineitemFile);
		reader = new BufferedReader(new FileReader(lineitems));
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values lineitem = new Values();
			for (String attribute : attributes) {
				lineitem.add(attribute);
			}
			long currentTimestamp = System.currentTimeMillis();
			long insertStart = System.currentTimeMillis();
//			lineitemSlidingJoin.insertTuple(currentTimestamp, lineitem);
            thetaJoin.insertTuple(currentTimestamp, lineitem);
			long insertEnd = System.currentTimeMillis();
			lineitemInsertStatistics.addValue((insertEnd - insertStart));
		}
		reader.close();

        reader = new BufferedReader(new FileReader(orders));
        long numberOfProducedTuples = 0L;
        while ((line = reader.readLine()) != null) {
            Values order = new Values();
            String[] attributes = line.split("\\|");
            for (String attribute : attributes) {
                order.add(attribute);
            }
            long currentTimestamp = System.currentTimeMillis();
            long joinStart = System.currentTimeMillis();
//			numberOfProducedTuples += lineitemSlidingJoin.joinTuple(currentTimestamp, order, new Fields(Order.schema), "O_ORDERKEY").size();
            numberOfProducedTuples += thetaJoin.joinTuple(currentTimestamp, order, new Fields(Order.schema), "O_ORDERKEY").size();
			long joinEnd = System.currentTimeMillis();
            lineOrderJoinStatistics.addValue((joinEnd - joinStart));
        }
        reader.close();

		System.out.println("+++ Insert Operation +++");
		System.out.println("Average: " + (lineitemInsertStatistics.getSum() / lineitemInsertStatistics.getN()) + " msec");
		System.out.println("Mean: " + lineitemInsertStatistics.getMean() + " msec");
		System.out.println("Min: " + lineitemInsertStatistics.getMin() + " msec");
		System.out.println("Max: " + lineitemInsertStatistics.getMax() + " msec");
		System.out.println("25% Percentile: " + lineitemInsertStatistics.getPercentile(25) + " msec");
		System.out.println("50% Percentile: " + lineitemInsertStatistics.getPercentile(50) + " msec");
		System.out.println("75% Percentile: " + lineitemInsertStatistics.getPercentile(75) + " msec");
		System.out.println("99% Percentile: " + lineitemInsertStatistics.getPercentile(99) + " msec");
		System.out.println("+++ Join Operation +++");
        System.out.println("Number of joined tuples: " + numberOfProducedTuples);
		System.out.println("Average: " + (lineOrderJoinStatistics.getSum() / lineOrderJoinStatistics.getN()) + " msec");
		System.out.println("Mean: " + lineOrderJoinStatistics.getMean() + " msec");
		System.out.println("Min: " + lineOrderJoinStatistics.getMin() + " msec");
		System.out.println("Max: " + lineOrderJoinStatistics.getMax() + " msec");
		System.out.println("25% Percentile: " + lineOrderJoinStatistics.getPercentile(25) + " msec");
		System.out.println("50% Percentile: " + lineOrderJoinStatistics.getPercentile(50) + " msec");
		System.out.println("75% Percentile: " + lineOrderJoinStatistics.getPercentile(75) + " msec");
		System.out.println("99% Percentile: " + lineOrderJoinStatistics.getPercentile(99) + " msec");
	}

}
