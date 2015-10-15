package gr.katsip.synefo.junit.test;

import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin;
import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin.BasicWindow;
import gr.katsip.synefo.tpch.Customer;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.Order;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Test;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SlidingJoinCorrectnessTest {

	@Test
	public void test() throws IOException {
		SlidingWindowJoin customerSlidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(Customer.query5schema), Customer.query5schema[0], "customer", "order");
		SlidingWindowJoin orderSlidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(Order.query5Schema), Order.query5Schema[0], "order", "customer");
		SlidingWindowJoin lineitemSlidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(LineItem.query5Schema), LineItem.query5Schema[0], "lineitem", "order");
		File customers = new File("D:\\TPC-H\\tpch_2_17_0\\dbgen\\customer.tbl");
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
		File orders = new File("D:\\TPC-H\\tpch_2_17_0\\dbgen\\orders.tbl");
		reader = new BufferedReader(new FileReader(orders));
		int joinedTuples = 0;
		int count = 0;
		SummaryStatistics orderJoinStatistics = new SummaryStatistics();
		ArrayList<Values> customerOrderResult = new ArrayList<Values>();
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values order = new Values();
			order.add(attributes[0]);
			order.add(attributes[1]);
			order.add(attributes[4]);
			orderSlidingJoin.insertTuple(System.currentTimeMillis(), order);
			long start = System.currentTimeMillis();
			ArrayList<Values> result = customerSlidingJoin.joinTuple(System.currentTimeMillis(), 
					order, new Fields(Order.query5Schema), "O_CUSTKEY");
			customerOrderResult.addAll(result);
			long end = System.currentTimeMillis();
			orderJoinStatistics.addValue((end - start));
			joinedTuples += result.size();
			count += 1;
//			if(count % 1021 == 0) {
//				System.out.println("Joined tuples: " + joinedTuples);
//				for(Values t : result) {
//					System.out.println("\t " + t.toString());
//				}
//			}
		}
		Fields customerOrderFields = new Fields((String[]) ArrayUtils.addAll(Customer.query5schema, Order.query5Schema));
		System.out.println("Customer-Order schema: " + customerOrderFields.toList().toString());
		System.out.println("sample: " + customerOrderResult.get(0).toString());
		reader.close();
		String[] customerOrder = new String[customerOrderFields.toList().size()];
		customerOrder = customerOrderFields.toList().toArray(customerOrder);
		SlidingWindowJoin customerOrderSlidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(customerOrderFields.toList()), customerOrder[2], "customerorder", "lineitem");
		for(Values tuple : customerOrderResult) {
			customerOrderSlidingJoin.insertTuple(System.currentTimeMillis(), tuple);
		}
		
		joinedTuples = 0;
		count = 0;
		SummaryStatistics lineitemJoinStatistics = new SummaryStatistics();
		ArrayList<Values> customerOrderLineitemResult = new ArrayList<Values>();
		for(int i = 0; i < 4; i++) {
			File lineitems = new File("D:\\TPC-H\\tpch_2_17_0\\dbgen\\lineitem.tbl");
			reader = new BufferedReader(new FileReader(lineitems));
			while((line = reader.readLine()) != null) {
				String[] attributes = line.split("\\|");
				Values lineitem = new Values();
				lineitem.add(attributes[0]);
				lineitem.add(attributes[2]);
				lineitem.add(attributes[5]);
				lineitem.add(attributes[6]);
				lineitemSlidingJoin.insertTuple(System.currentTimeMillis(), lineitem);
//				long start = System.currentTimeMillis();
//				ArrayList<Values> result = customerOrderSlidingJoin.joinTuple(System.currentTimeMillis(), 
//						lineitem, new Fields(LineItem.query5Schema), "L_ORDERKEY");
//				long end = System.currentTimeMillis();
//				customerOrderLineitemResult.addAll(result);
//				lineitemJoinStatistics.addValue((end - start));
//				joinedTuples += result.size();
//				count += 1;
			}
			reader.close();
			System.out.println("Completed pass " + i + " of lineitem file.");
		}
		orders = new File("D:\\TPC-H\\tpch_2_17_0\\dbgen\\orders.tbl");
		reader = new BufferedReader(new FileReader(orders));
		joinedTuples = 0;
		count = 0;
		DescriptiveStatistics orderLineStatistics = new DescriptiveStatistics();
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("\\|");
			Values order = new Values();
			order.add(attributes[0]);
			order.add(attributes[1]);
			order.add(attributes[4]);
			orderSlidingJoin.insertTuple(System.currentTimeMillis(), order);
			long start = System.currentTimeMillis();
			ArrayList<Values> result = lineitemSlidingJoin.joinTuple(System.currentTimeMillis(), 
					order, new Fields(Order.query5Schema), "O_CUSTKEY");
			long end = System.currentTimeMillis();
			orderLineStatistics.addValue((end - start));
			joinedTuples += result.size();
			count += 1;
		}
		reader.close();
		Fields customerOrderLineitemFields = new Fields((String[]) ArrayUtils.addAll(customerOrder, LineItem.query5Schema));
		System.out.println("+++Join Statistics+++");
		System.out.println("Customer-Order-Lineitem schema: " + customerOrderLineitemFields.toList().toString());
//		System.out.println("Sample-1: " + customerOrderLineitemResult.get(0).toString());
//		System.out.println("Sample-2: " + customerOrderLineitemResult.get(1).toString());
//		System.out.println("Sample-3: " + customerOrderLineitemResult.get(2).toString());
//		System.out.println("Sample-4: " + customerOrderLineitemResult.get(customerOrderLineitemResult.size() - 3).toString());
//		System.out.println("Sample-5: " + customerOrderLineitemResult.get(customerOrderLineitemResult.size() - 2).toString());
//		System.out.println("Sample-6: " + customerOrderLineitemResult.get(customerOrderLineitemResult.size() - 1).toString());
		System.out.println("Order average: " + (orderJoinStatistics.getSum() / orderJoinStatistics.getN()));
		System.out.println("Order mean: " + orderJoinStatistics.getMean());
		System.out.println("Number of \"Order\" tuples stored: " + orderSlidingJoin.getNumberOfTuples());
		System.out.println("State size of \"Order\" tuples: " + orderSlidingJoin.getStateSize());
		LinkedList<BasicWindow> orderCache = orderSlidingJoin.getRingBuffer();
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
		LinkedList<BasicWindow> lineitemCache = lineitemSlidingJoin.getRingBuffer();
		SummaryStatistics lineitemNumberOfTuples = new SummaryStatistics();
		for(BasicWindow window : lineitemCache) {
			long number = window.numberOfTuples;
			lineitemNumberOfTuples.addValue(number);
		}
		System.out.println("Average number of \"Lineitem\" tuples per window: " + (lineitemNumberOfTuples.getSum() / lineitemNumberOfTuples.getN()));
		System.out.println("Min number of \"Lineitem\" tuples per window: " + lineitemNumberOfTuples.getMin());
		System.out.println("Max number of \"Lineitem\" tuples per window: " + lineitemNumberOfTuples.getMax());
		System.out.println("Mean number of \"Lineitem\" tuples per window: " + lineitemNumberOfTuples.getMean());
		
		System.out.println("Order join with LineItem average: " + (orderLineStatistics.getSum() / orderLineStatistics.getN()));
		System.out.println("Order join with LineItem max: " + (orderLineStatistics.getMax()));
		System.out.println("Order join with LineItem min: " + (orderLineStatistics.getMin()));
		System.out.println("Order join with LineItem Mean: " + (orderLineStatistics.getMean()));
		System.out.println("Order join with LineItem 25% percentile: " + orderLineStatistics.getPercentile(25));
		System.out.println("Order join with LineItem 50% percentile: " + orderLineStatistics.getPercentile(50));
		System.out.println("Order join with LineItem 75% percentile: " + orderLineStatistics.getPercentile(75));
		System.out.println("Order join with LineItem 90% percentile: " + orderLineStatistics.getPercentile(90));
		System.out.println("Number of joined tuples: " + (joinedTuples));
	}

}
