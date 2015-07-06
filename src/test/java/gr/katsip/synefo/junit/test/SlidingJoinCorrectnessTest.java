package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;
import gr.katsip.synefo.storm.operators.relational.elastic.SlidingWindowJoin;
import gr.katsip.synefo.tpch.Customer;
import gr.katsip.synefo.tpch.Order;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SlidingJoinCorrectnessTest {

	@Test
	public void test() throws IOException {
		SlidingWindowJoin slidingJoin = new SlidingWindowJoin(3600000, 1000, 
				new Fields(Customer.query5schema), Customer.query5schema[0]);
		File customers = new File("customer.tbl");
		BufferedReader reader = new BufferedReader(new FileReader(customers));
		String line = "";
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("|");
			Values tuple = new Values();
			tuple.add(attributes[0]);
			tuple.add(attributes[3]);
			slidingJoin.insertTuple(System.currentTimeMillis(), tuple);
		}
		reader.close();
		File orders = new File("order.tbl");
		reader = new BufferedReader(new FileReader(orders));
		int joinedTuples = 0;
		while((line = reader.readLine()) != null) {
			String[] attributes = line.split("|");
			Values order = new Values();
			order.add(attributes[0]);
			order.add(attributes[1]);
			order.add(attributes[4]);
			ArrayList<Values> result = slidingJoin.joinTuple(System.currentTimeMillis(), 
					order, new Fields(Order.query5Schema), "O_CUSTKEY");
			joinedTuples += result.size();
		}
	}

}
