package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;

import java.util.Random;

import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin;
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
		Long startTime = System.currentTimeMillis();
		SlidingWindowJoin slidingJoin = new SlidingWindowJoin(60000, 1000, lineitemSchema, LineItem.query5Schema[0]);
		for(int i = 0; i < 1000000000; i++) {
			Long currentTimestamp = System.currentTimeMillis();
			Random random = new Random();
			Integer orderKey = random.nextInt(10000000);
			Integer suppKey = random.nextInt(1000);
			Double extendedPrice = randomDecimal();
			Double discount = randomDecimal();
			Values tuple = new Values(orderKey.toString(), suppKey.toString(), extendedPrice.toString(), discount.toString());
			slidingJoin.insertTuple(currentTimestamp, tuple);
			Long insertEndTimestamp = System.currentTimeMillis();
			insertStatistics.addValue(insertEndTimestamp - currentTimestamp);
		}
		Long endTime = System.currentTimeMillis();
		System.out.println("Added 1000000000 tuples in " + (endTime - startTime) / 1000L + " seconds.");
		System.out.println("Average time for each insertion: " + (insertStatistics.getSum() / 1000000000) + " msec.");
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
