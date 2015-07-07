package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;
import gr.katsip.synefo.storm.operators.relational.elastic.JoinDispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.JoinJoiner;
import gr.katsip.synefo.tpch.Customer;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.Order;
import gr.katsip.synefo.tpch.Supplier;
import gr.katsip.synefo.tpch.TpchTupleProducer;

import org.junit.Test;

import backtype.storm.tuple.Fields;

public class TpchQ5TopolotyTest {

	@Test
	public void test() {
		String[] dataSchema = { "attributes", "values" };
		TpchTupleProducer customerProducer = new TpchTupleProducer("0.0.0.0:6666", Customer.schema, Customer.query5schema);
		customerProducer.setSchema(new Fields(dataSchema));
		System.out.println("customer-producer schema: " + customerProducer.getSchema().toList().toString());
		TpchTupleProducer orderProducer = new TpchTupleProducer("0.0.0.0:6666", Order.schema, Order.query5Schema);
		orderProducer.setSchema(new Fields(dataSchema));
		System.out.println("customer-producer schema: " + orderProducer.getSchema().toList().toString());
		TpchTupleProducer lineitemProducer = new TpchTupleProducer("0.0.0.0:6666", LineItem.schema, LineItem.query5Schema);
		lineitemProducer.setSchema(new Fields(dataSchema));
		System.out.println("customer-producer schema: " + lineitemProducer.getSchema().toList().toString());
		TpchTupleProducer supplierProducer = new TpchTupleProducer("0.0.0.0:6666", Supplier.schema, Supplier.query5Schema);
		supplierProducer.setSchema(new Fields(dataSchema));
		System.out.println("customer-producer schema: " + supplierProducer.getSchema().toList().toString());
		
		JoinDispatcher dispatcher = new JoinDispatcher("customer", new Fields(Customer.query5schema), "order", 
				new Fields(Order.query5Schema), new Fields(dataSchema));
		System.out.println("dispatcher schema: " + dispatcher.getJoinOutputSchema().toList().toString());
		JoinDispatcher dispatcher2 = new JoinDispatcher("lineitem", new Fields(LineItem.query5Schema), 
				"supplier", new Fields(Supplier.query5Schema), new Fields(dataSchema));
		System.out.println("dispatcher2 schema: " + dispatcher2.getJoinOutputSchema().toList().toString());
		
		JoinJoiner joinerCustomer = new JoinJoiner("customer", new Fields(Customer.query5schema), "order", 
				new Fields(Order.query5Schema), "C_CUSTKEY", "O_CUSTKEY", 2400000, 1000);
		System.out.println("joinjoincust schema: " + joinerCustomer.getJoinOutputSchema().toList().toString());
		joinerCustomer.setOutputSchema(new Fields(dataSchema));
		System.out.println("joinjoincust (join) schema: " + joinerCustomer.getJoinOutputSchema().toList().toString());
		System.out.println("joinjoincust (out) schema: " + joinerCustomer.getOutputSchema().toList().toString());
		
		JoinJoiner joinerOrder = new JoinJoiner("order", new Fields(Order.query5Schema), "customer", 
				new Fields(Customer.query5schema), "O_CUSTKEY", "C_CUSTKEY", 2400000, 1000);
		System.out.println("joinjoinorder schema: " + joinerOrder.getJoinOutputSchema().toList().toString());
		joinerOrder.setOutputSchema(new Fields(dataSchema));
		System.out.println("joinjoinorder (join) schema: " + joinerOrder.getJoinOutputSchema().toList().toString());
		System.out.println("joinjoinorder (out) schema: " + joinerOrder.getOutputSchema().toList().toString());
		
		JoinJoiner joinerLineitem = new JoinJoiner("lineitem", new Fields(LineItem.query5Schema), 
				"supplier", new Fields(Supplier.query5Schema), "L_SUPPKEY", "S_SUPPKEY", 2400000, 1000);
		System.out.println("joinjoinlineitem schema: " + joinerLineitem.getJoinOutputSchema().toList().toString());
		joinerLineitem.setOutputSchema(new Fields(dataSchema));
		System.out.println("joinjoinlineitem (join) schema: " + joinerLineitem.getJoinOutputSchema().toList().toString());
		System.out.println("joinjoinlineitem (out) schema: " + joinerLineitem.getOutputSchema().toList().toString());
		
		JoinJoiner joinerSupplier = new JoinJoiner("supplier", new Fields(Supplier.query5Schema), 
				"lineitem", new Fields(LineItem.query5Schema), "S_SUPPKEY", "L_SUPPKEY", 2400000, 1000);
		System.out.println("joinjoinsupplier schema: " + joinerSupplier.getJoinOutputSchema().toList().toString());
		joinerSupplier.setOutputSchema(new Fields(dataSchema));
		System.out.println("joinjoinsupplier (join) schema: " + joinerSupplier.getJoinOutputSchema().toList().toString());
		System.out.println("joinjoinsupplier (out) schema: " + joinerSupplier.getOutputSchema().toList().toString());
		
		Fields joinOutputOne = joinerOrder.getJoinOutputSchema();
		Fields joinOutputTwo = joinerSupplier.getJoinOutputSchema();
		JoinDispatcher dispatcher3 = new JoinDispatcher("outputone", joinOutputOne, 
				"outputtwo", joinOutputTwo, new Fields(dataSchema));
		System.out.println("dispathcer3 (join) schema: " + dispatcher3.getJoinOutputSchema().toList().toString());
		System.out.println("dispathcer3 (out) schema: " + dispatcher3.getOutputSchema().toList().toString());
		
		JoinJoiner joinerOutputOne = new JoinJoiner("outputone", new Fields(joinOutputOne.toList()), "outputtwo", 
				new Fields(joinOutputTwo.toList()), "O_ORDERKEY", "L_ORDERKEY", 2400000, 1000);
		System.out.println("joinjoinoutputone schema: " + joinerOutputOne.getJoinOutputSchema().toList().toString());
		joinerOutputOne.setOutputSchema(new Fields(dataSchema));
		System.out.println("joinjoinoutputone (join) schema: " + joinerOutputOne.getJoinOutputSchema().toList().toString());
		System.out.println("joinjoinoutputone (out) schema: " + joinerOutputOne.getOutputSchema().toList().toString());
		
		JoinJoiner joinerOutputTwo = new JoinJoiner("outputtwo", new Fields(joinOutputTwo.toList()), "outputone", 
				new Fields(joinOutputOne.toList()), "L_ORDERKEY", "O_ORDERKEY", 240000, 1000);
		System.out.println("joinjoinoutputtwo schema: " + joinerOutputTwo.getJoinOutputSchema().toList().toString());
		joinerOutputTwo.setOutputSchema(new Fields(dataSchema));
		System.out.println("joinjoinoutputtwo (join) schema: " + joinerOutputTwo.getJoinOutputSchema().toList().toString());
		System.out.println("joinjoinoutputtwo (out) schema: " + joinerOutputTwo.getOutputSchema().toList().toString());
	}

}
