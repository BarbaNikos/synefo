package gr.katsip.synefo.storm.topology;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedDispatchBolt;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedDispatchWindow;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedWindowDispatcher;
import gr.katsip.synefo.storm.operators.joiner.collocated.CollocatedEquiJoiner;
import gr.katsip.synefo.storm.operators.joiner.collocated.CollocatedJoinBolt;
import gr.katsip.synefo.storm.producers.ElasticFileSpout;
import gr.katsip.synefo.storm.producers.SerialControlledFileProducer;
import gr.katsip.tpch.Customer;
import gr.katsip.tpch.LineItem;
import gr.katsip.tpch.Order;
import gr.katsip.tpch.Supplier;

/**
 * Created by Nick R. Katsipoulakis on 12/3/15.
 */
public class QueryFive {

    public static void main(String[] args) {
        String[] schema = { "attributes", "values" };
        long slideInMilliSeconds = 1000L;
        double windowInMinutes = 0.1;
        double[] outputRate = null;
        int[] checkpoint = null;
        SerialControlledFileProducer customerProducer = new SerialControlledFileProducer("path-to-file", Customer.schema,
                Customer.query5schema, outputRate, checkpoint);
        SerialControlledFileProducer lineitemProducer = new SerialControlledFileProducer("path-to-file", LineItem.schema,
                LineItem.query5Schema, outputRate, checkpoint);
        SerialControlledFileProducer orderProducer = new SerialControlledFileProducer("path-to-file", Order.schema,
                Order.query5Schema, outputRate, checkpoint);
        SerialControlledFileProducer supplierProducer = new SerialControlledFileProducer("path-to-file", Supplier.schema,
                Supplier.query5Schema, outputRate, checkpoint);

        TopologyBuilder builder = new TopologyBuilder();

        /**
         * CUSTOMER.C_CUSTKEY = ORDER.O_ORDERKEY
         */
        builder.setSpout("customer", new ElasticFileSpout("customer", "synefo-ip", 5555, customerProducer, "zoo:port"), 1);
        builder.setSpout("order", new ElasticFileSpout("order", "synefo-ip", 5555, orderProducer, "zoo-port"), 1);

        CollocatedWindowDispatcher dispatcher = new CollocatedWindowDispatcher("customer", new Fields(Customer.query5schema),
                Customer.query5schema[0], "order", new Fields(Order.query5Schema), Order.query5Schema[1], new Fields(schema),
                (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("cust_ord_dispatch", new CollocatedDispatchBolt("cust_ord_dispatch", "synefo-ip", 5555, dispatcher,
                "zoo:port", false), 1)
                .directGrouping("customer-data")
                .directGrouping("order-data")
                .directGrouping("cust_ord_join-control")
                .setNumTasks(1);

        CollocatedEquiJoiner joiner = new CollocatedEquiJoiner("customer", new Fields(Customer.query5schema), "order",
                new Fields(Order.query5Schema), Customer.query5schema[0], Order.query5Schema[1],
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust_ord_join", new CollocatedJoinBolt("cust_ord_join", "synefo-ip", 5555, joiner, "zoo:port"), 1)
                .directGrouping("cust_ord_dispatch-data")
                .directGrouping("cust_ord_dispatch-control")
                .setNumTasks(1);

        Fields customerOrderJoinSchema = joiner.getOutputSchema();

        /**
         * LINEITEM.L_SUPPKEY = SUPPLIER.S_SUPPKEY
         */
        builder.setSpout("lineitem", new ElasticFileSpout("lineitem", "synefo-ip", 5555, lineitemProducer, "zoo:port"), 1);
        builder.setSpout("supplier", new ElasticFileSpout("supplier", "synefo-ip", 5555, supplierProducer, "zoo:port"), 1);

        dispatcher = new CollocatedWindowDispatcher("lineitem", new Fields(LineItem.query5Schema), LineItem.query5Schema[1],
                "supplier", new Fields(Supplier.query5Schema), Supplier.query5Schema[0], new Fields(schema),
                (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("line_sup_dispatch", new CollocatedDispatchBolt("line_sup_dispatch", "synefo-ip", 5555, dispatcher,
                "zoo:port", false), 1)
                .directGrouping("lineitem-data")
                .directGrouping("supplier-data")
                .directGrouping("line_sup_join-control")
                .setNumTasks(1);

        joiner = new CollocatedEquiJoiner("lineitem", new Fields(LineItem.query5Schema), "supplier",
                new Fields(Supplier.query5Schema), LineItem.query5Schema[1], Supplier.query5Schema[0],
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("line_sup_join", new CollocatedJoinBolt("line_sup_join", "synefo-ip", 5555, joiner, "zoo:port"), 1)
                .directGrouping("line_sup_dispatch-data")
                .directGrouping("line_sup_dispatch-control")
                .setNumTasks(1);

        Fields lineitemSupplierJoinSchema = joiner.getOutputSchema();

        /**
         * CUSTOMER_ORDER.O_ORDERKEY = LINEITEM_SUPPLIER.L_ORDERKEY
         */

    }
}
