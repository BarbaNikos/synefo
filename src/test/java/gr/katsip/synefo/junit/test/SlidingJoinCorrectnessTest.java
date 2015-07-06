package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;
import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin;
import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin.BasicWindow;
import gr.katsip.synefo.tpch.Customer;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.Order;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Test;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SlidingJoinCorrectnessTest {

	@Test
	public void test() throws IOException {
		SlidingWindowJoin customerSlidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(Customer.query5schema), Customer.query5schema[0]);
		SlidingWindowJoin orderSlidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(Order.query5Schema), Order.query5Schema[0]);
		SlidingWindowJoin lineitemSlidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(LineItem.query5Schema), LineItem.query5Schema[0]);
		File customers = new File("customer.tbl");
		BufferedReader reader = new BufferedReader(new FileReader(customers));
		String line = "";
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values tuple = new Values();
			tuple.add(attributes[0]);
			tuple.add(attributes[3]);
			customerSlidingJoin.insertTuple(System.currentTimeMillis(), tuple);
		}
		reader.close();
		File orders = new File("orders.tbl");
		reader = new BufferedReader(new FileReader(orders));
		int joinedTuples = 0;
		int count = 0;
		SummaryStatistics orderJoinStatistics = new SummaryStatistics();
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values order = new Values();
			order.add(attributes[0]);
			order.add(attributes[1]);
			order.add(attributes[4]);
			if(attributes[4].equals("|")) {
				System.out.println("line:" + line);
				System.out.println("attributes: " + Arrays.toString(attributes));
				break;
			}
//			System.out.println("order: " + order.toString());
			orderSlidingJoin.insertTuple(System.currentTimeMillis(), order);
			long start = System.currentTimeMillis();
			ArrayList<Values> result = customerSlidingJoin.joinTuple(System.currentTimeMillis(), 
					order, new Fields(Order.query5Schema), "O_CUSTKEY");
			long end = System.currentTimeMillis();
			orderJoinStatistics.addValue((end - start));
			joinedTuples += result.size();
			count += 1;
			if(count % 1021 == 0) {
				System.out.println("Joined tuples: " + joinedTuples);
				for(Values t : result) {
					System.out.println("\t " + t.toString());
				}
			}
		}
		reader.close();
		File lineitems = new File("lineitem.tbl");
		reader = new BufferedReader(new FileReader(lineitems));
		joinedTuples = 0;
		count = 0;
		SummaryStatistics lineitemJoinStatistics = new SummaryStatistics();
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values lineitem = new Values();
			lineitem.add(attributes[0]);
			lineitem.add(attributes[2]);
			lineitem.add(attributes[5]);
			lineitem.add(attributes[6]);
			lineitemSlidingJoin.insertTuple(System.currentTimeMillis(), lineitem);
			long start = System.currentTimeMillis();
			ArrayList<Values> result = orderSlidingJoin.joinTuple(System.currentTimeMillis(), 
					lineitem, new Fields(LineItem.query5Schema), "L_ORDERKEY");
			long end = System.currentTimeMillis();
			lineitemJoinStatistics.addValue((end - start));
			joinedTuples += result.size();
			count += 1;
			if(count % 1021 == 0) {
				System.out.println("Joined tuples: " + joinedTuples);
				for(Values t : result) {
					System.out.println("\t " + t.toString());
				}
			}
		}
		System.out.println("+++Join Statistics+++");
		System.out.println("Order average: " + (orderJoinStatistics.getSum() / orderJoinStatistics.getN()));
		System.out.println("Order mean: " + orderJoinStatistics.getMean());
		System.out.println("Number of \"Order\" tuples stored: " + orderSlidingJoin.getNumberOfTuples());
		System.out.println("State size of \"Order\" tuples: " + orderSlidingJoin.getStateSize());
		LinkedList<BasicWindow> orderCache = orderSlidingJoin.getCircularCache();
		SummaryStatistics orderNumberOfTuples = new SummaryStatistics();
		for(BasicWindow window : orderCache) {
			long number = window.numberOfTuples;
			orderNumberOfTuples.addValue(number);
		}
		System.out.println("Average number of \"Order\" tuples per window: " + (orderNumberOfTuples.getSum() / orderNumberOfTuples.getN()));
		System.out.println("Min number of \"Order\" tuples per window: " + orderNumberOfTuples.getMin());
		System.out.println("Max number of \"Order\" tuples per window: " + orderNumberOfTuples.getMax());
		System.out.println("Mean number of \"Order\" tuples per window: " + orderNumberOfTuples.getMean());
		
		System.out.println("Lineitem average: " + (lineitemJoinStatistics.getSum() / lineitemJoinStatistics.getN()));
		System.out.println("Lineitem mean: " + lineitemJoinStatistics.getMean());
		System.out.println("Number of \"Lineitem\" tuples stored: " + lineitemSlidingJoin.getNumberOfTuples());
		System.out.println("State size of \"Lineitem\" tuples: " + lineitemSlidingJoin.getStateSize());
		LinkedList<BasicWindow> lineitemCache = lineitemSlidingJoin.getCircularCache();
		SummaryStatistics lineitemNumberOfTuples = new SummaryStatistics();
		for(BasicWindow window : lineitemCache) {
			long number = window.numberOfTuples;
			lineitemNumberOfTuples.addValue(number);
		}
		System.out.println("Average number of \"Lineitem\" tuples per window: " + (lineitemNumberOfTuples.getSum() / lineitemNumberOfTuples.getN()));
		System.out.println("Min number of \"Lineitem\" tuples per window: " + lineitemNumberOfTuples.getMin());
		System.out.println("Max number of \"Lineitem\" tuples per window: " + lineitemNumberOfTuples.getMax());
		System.out.println("Mean number of \"Lineitem\" tuples per window: " + lineitemNumberOfTuples.getMean());
	}

}
