package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.Random;

import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin;
import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin.BasicWindow;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.Order;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Test;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SlidingWindowJoinTest {

	@Test
	public void test() {
		Fields orderSchema = new Fields(Order.query5Schema);
		Fields lineitemSchema = new Fields(LineItem.query5Schema);
		SummaryStatistics insertStatistics = new SummaryStatistics();
		SummaryStatistics stateSizeStatistics = new SummaryStatistics();
		long sumOfStateSamples = 0L;
		SummaryStatistics tupleSizeStatistics = new SummaryStatistics();
		long sumOfTupleSizeSamples = 0L;
		Long startTime = System.currentTimeMillis();
		SlidingWindowJoin slidingJoin = new SlidingWindowJoin(360000, 360000, lineitemSchema, LineItem.query5Schema[0]);
		for(long i = 0L; i < 1000L; i++) {
			Long currentTimestamp = System.currentTimeMillis();
			Random random = new Random();
			Integer orderKey = random.nextInt(10000000);
			Integer suppKey = random.nextInt(1000);
			Double extendedPrice = randomDecimal();
			Double discount = randomDecimal();
			Values tuple = new Values(orderKey.toString(), suppKey.toString(), extendedPrice.toString(), discount.toString());
			slidingJoin.insertTuple(currentTimestamp, tuple);
			Long insertEndTimestamp = System.currentTimeMillis();
			if(i == 0L) {
				System.out.println(tuple);
				System.out.println(tuple.toArray().toString().length());
			}
			stateSizeStatistics.addValue(slidingJoin.getStateSize());
			sumOfStateSamples += slidingJoin.getStateSize();
			tupleSizeStatistics.addValue(tuple.toArray().toString().length());
			sumOfTupleSizeSamples += tuple.toArray().toString().length();
			insertStatistics.addValue(insertEndTimestamp - currentTimestamp);
		}
		Long endTime = System.currentTimeMillis();
		LinkedList<BasicWindow> circularCache = slidingJoin.getCircularCache();
		System.out.println("Size of circular cache: " + circularCache.size() + " basic windows");
		SummaryStatistics cacheStatistics = new SummaryStatistics();
		double sum = 0.0;
		for(BasicWindow window : circularCache) {
			cacheStatistics.addValue(window.tuples.size());
			sum += window.tuples.size();
		}
		System.out.println("State size average: " + sumOfStateSamples / 1000L + " bytes.");
		System.out.println("State size max: " + stateSizeStatistics.getMax() + " bytes.");
		System.out.println("State size min: " + stateSizeStatistics.getMin() + " bytes.");
		System.out.println("Average basic window size: " + sum / circularCache.size() + " tuples");
		System.out.println("Max basic window size: " + cacheStatistics.getMax() + " tuples");
		System.out.println("Min basic window size: " + cacheStatistics.getMin() + " tuples");
		System.out.println("Variance of basic window size: " + cacheStatistics.getVariance() + " tuples");
		System.out.println("Mean basic window size: " + cacheStatistics.getMean() + " tuples");
		
		System.out.println("Added 1000000000 tuples in " + (endTime - startTime) / 1000L + " seconds.");
		System.out.println("Average time for each insertion: " + (insertStatistics.getSum() / 1000L) + " msec.");
		System.out.println("Mean value: " + insertStatistics.getMean() + " msec.");
		System.out.println("Variance: " + insertStatistics.getVariance() + ".");
		
		
	}
	
	public double randomDecimal() {
		Random random = new Random();
		int lowerBound = 1000;
	    int upperBound = 10000;
	    int decimalPlaces = 2;
	    double dbl =
	            ((random == null ? new Random() : random).nextDouble() //
	                * (upperBound - lowerBound))
	                + lowerBound;
	    return dbl;
	}

}
