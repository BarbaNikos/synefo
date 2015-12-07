package gr.katsip.synefo.storm.topology;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.joiner.Joiner;
import gr.katsip.synefo.storm.producers.SerialControlledFileProducer;
import gr.katsip.tpch.Customer;
import gr.katsip.tpch.Order;

import java.io.File;

/**
 * Created by katsip on 12/7/2015.
 */
public class JoinTopologyTest {

    public static void main(String[] args) {
        double[] outputRate = { 10000 };
        int[] checkpoint = { 0 };
        Joiner joiner = new Joiner("customer", new Fields(Customer.query5schema), "order", new Fields(Order.query5Schema),
                Customer.query5schema[0], Order.query5Schema[1], (int) (0.1 * (60 * 1000)), 1000);
        SerialControlledFileProducer customerProducer = new SerialControlledFileProducer("Z:\\Documents\\TPC-H_2_17_0\\customer.tbl", Customer.schema,
                Customer.query5schema, outputRate, checkpoint);
        SerialControlledFileProducer orderProducer = new SerialControlledFileProducer("Z:\\Documents\\TPC-H_2_17_0\\orders.tbl", Order.schema,
                Order.query5Schema, outputRate, checkpoint);
        Values values = null;
        while ((values = customerProducer.nextTuple()) != null) {

        }
    }

}
