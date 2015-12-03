package gr.katsip.synefo.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedDispatchBolt;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedDispatchWindow;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedWindowDispatcher;
import gr.katsip.synefo.storm.operators.joiner.collocated.CollocatedEquiJoiner;
import gr.katsip.synefo.storm.operators.joiner.collocated.CollocatedJoinBolt;
import gr.katsip.synefo.storm.producers.ElasticFileSpout;
import gr.katsip.synefo.storm.producers.SerialControlledFileProducer;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.tpch.Customer;
import gr.katsip.tpch.LineItem;
import gr.katsip.tpch.Order;
import gr.katsip.tpch.Supplier;

import javax.sound.sampled.Line;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Nick R. Katsipoulakis on 12/3/15.
 */
public class QueryFiveDriver {

    private int scale;

    private boolean AUTO_SCALE;

    private String[] inputFile;

    private double[] outputRate;

    private int[] checkpoint;

    private float windowInMinutes;

    private long slideInMilliSeconds;

    private int numberOfWorkers;

    private static final String[] schema = { "attributes", "values" };

    private static final Integer synefoPort = 5555;

    private String synefoAddress;

    private String zookeeperAddress;

    private int maxSpoutPending;

    public void configure(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
            scale = Integer.parseInt(reader.readLine().split("=")[1]);
            inputFile = reader.readLine().split("=")[1].split(",");
            String[] strOutputRate = reader.readLine().split("=")[1].split(",");
            String[] strCheckpoint = reader.readLine().split("=")[1].split(",");
            outputRate = new double[strOutputRate.length];
            checkpoint = new int[strCheckpoint.length];
            for (int i = 0; i < strOutputRate.length; i++) {
                outputRate[i] = Double.parseDouble(strOutputRate[i]);
                checkpoint[i] = Integer.parseInt(strCheckpoint[i]);
            }
            windowInMinutes = Float.parseFloat(reader.readLine().split("=")[1]);
            slideInMilliSeconds = Long.parseLong(reader.readLine().split("=")[1]);
//            System.out.println("window-in-minutes: " + windowInMinutes + ", slide-in-msec: " + slideInMilliSeconds);
            numberOfWorkers = Integer.parseInt(reader.readLine().split("=")[1]);
            synefoAddress = reader.readLine().split("=")[1];
            zookeeperAddress = reader.readLine().split("=")[1];
            String strType = reader.readLine().split("=")[1].toUpperCase();
            String strReaderType = reader.readLine().split("=")[1].toUpperCase();
            String autoScale = reader.readLine().split("=")[1];
            if (autoScale.toLowerCase().equals("true"))
                AUTO_SCALE = true;
            else
                AUTO_SCALE = false;
            maxSpoutPending = Integer.parseInt(reader.readLine().split("=")[1]);
            reader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void submit() {
        Integer numberOfTasks = 0;
        Config conf = new Config();
        HashMap<String, List<String>> topology = new HashMap<>();
        List<String> tasks;
        TopologyBuilder builder = new TopologyBuilder();

        SerialControlledFileProducer customerProducer = new SerialControlledFileProducer(inputFile[0], Customer.schema,
                Customer.query5schema, outputRate, checkpoint);
        customerProducer.setSchema(new Fields(schema));
        SerialControlledFileProducer lineitemProducer = new SerialControlledFileProducer(inputFile[1], LineItem.schema,
                LineItem.query5Schema, outputRate, checkpoint);
        lineitemProducer.setSchema(new Fields(schema));
        SerialControlledFileProducer orderProducer = new SerialControlledFileProducer(inputFile[2], Order.schema,
                Order.query5Schema, outputRate, checkpoint);
        orderProducer.setSchema(new Fields(schema));
        SerialControlledFileProducer supplierProducer = new SerialControlledFileProducer(inputFile[3], Supplier.schema,
                Supplier.query5Schema, outputRate, checkpoint);
        supplierProducer.setSchema(new Fields(schema));
        /**
         * CUSTOMER.C_CUSTKEY = ORDER.O_ORDERKEY
         */
        builder.setSpout("customer", new ElasticFileSpout("customer", synefoAddress, synefoPort, customerProducer, zookeeperAddress), 1);
        builder.setSpout("order", new ElasticFileSpout("order", synefoAddress, synefoPort, orderProducer, zookeeperAddress), 1);
        numberOfTasks += 2;
        tasks = new ArrayList<>();
        tasks.add("cust_ord_dispatch");
        topology.put("customer", tasks);
        topology.put("order", new ArrayList<>(tasks));

        CollocatedWindowDispatcher dispatcher = new CollocatedWindowDispatcher("customer", new Fields(Customer.query5schema),
                Customer.query5schema[0], "order", new Fields(Order.query5Schema), Order.query5Schema[1], new Fields(schema),
                (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("cust_ord_dispatch", new CollocatedDispatchBolt("cust_ord_dispatch", synefoAddress, synefoPort, dispatcher,
                zookeeperAddress, AUTO_SCALE), 1)
                .directGrouping("customer", "customer")
                .directGrouping("order", "order")
                .directGrouping("cust_ord_join", "cust_ord_join-control")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("cust_ord_join");
        topology.put("cust_ord_dispatch", tasks);

        CollocatedEquiJoiner joiner = new CollocatedEquiJoiner("order", new Fields(Order.query5Schema), "customer", new Fields(Customer.query5schema),
                Order.query5Schema[1], Customer.query5schema[0], (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust_ord_join", new CollocatedJoinBolt("cust_ord_join", synefoAddress, synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("cust_ord_dispatch", "cust_ord_dispatch-data")
                .directGrouping("cust_ord_dispatch", "cust_ord_dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        tasks = new ArrayList<>();
        tasks.add("cust_ord_line_sup_dispatch");
        topology.put("cust_ord_join", tasks);

        Fields customerOrderJoinSchema = joiner.getOutputSchema();
        /**
         * LINEITEM.L_SUPPKEY = SUPPLIER.S_SUPPKEY
         */
        builder.setSpout("lineitem", new ElasticFileSpout("lineitem", synefoAddress, synefoPort, lineitemProducer, zookeeperAddress), 1);
        builder.setSpout("supplier", new ElasticFileSpout("supplier", synefoAddress, synefoPort, supplierProducer, zookeeperAddress), 1);

        dispatcher = new CollocatedWindowDispatcher("lineitem", new Fields(LineItem.query5Schema), LineItem.query5Schema[1],
                "supplier", new Fields(Supplier.query5Schema), Supplier.query5Schema[0], new Fields(schema),
                (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("line_sup_dispatch", new CollocatedDispatchBolt("line_sup_dispatch", synefoAddress, synefoPort, dispatcher,
                zookeeperAddress, AUTO_SCALE), 1)
                .directGrouping("lineitem", "lineitem")
                .directGrouping("supplier", "supplier")
                .directGrouping("line_sup_join", "line_sup_join-control")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("line_sup_join");
        topology.put("line_sup_dispatch", tasks);

        joiner = new CollocatedEquiJoiner("supplier", new Fields(Supplier.query5Schema),
                "lineitem", new Fields(LineItem.query5Schema), Supplier.query5Schema[0], LineItem.query5Schema[1],
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("line_sup_join", new CollocatedJoinBolt("line_sup_join", synefoAddress, synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("line_sup_dispatch", "line_sup_dispatch-data")
                .directGrouping("line_sup_dispatch", "line_sup_dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        tasks = new ArrayList<>();
        tasks.add("cust_ord_line_sup_dispatch");
        topology.put("line_sup_join", tasks);

        Fields lineitemSupplierJoinSchema = joiner.getOutputSchema();

        /**
         * CUSTOMER_ORDER.O_ORDERKEY = LINEITEM_SUPPLIER.L_ORDERKEY
         */
        dispatcher = new CollocatedWindowDispatcher("cust_ord", customerOrderJoinSchema, Order.query5Schema[1],
                "line_sup", lineitemSupplierJoinSchema, LineItem.query5Schema[0], new Fields(schema),
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust_ord_line_sup_dispatch", new CollocatedDispatchBolt("cust_ord_line_sup_dispatch", synefoAddress,
                synefoPort, dispatcher, zookeeperAddress, AUTO_SCALE), 1)
                .directGrouping("cust_ord_join", "cust_ord_join-data")
                .directGrouping("line_sup_join", "line_sup_join-data")
                .directGrouping("cust_ord_line_sup_join-control")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("cust_ord_line_sup_join");
        topology.put("cust_ord_line_sup_dispatch", tasks);

        joiner = new CollocatedEquiJoiner("line_sup", lineitemSupplierJoinSchema, "cust_ord", customerOrderJoinSchema, LineItem.query5Schema[0], Order.query5Schema[1],
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust_ord_line_sup_join", new CollocatedJoinBolt("cust_ord_line_sup_join", synefoAddress,
                synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("cust_ord_line_sup_dispatch", "cust_ord_line_sup_dispatch-data")
                .directGrouping("cust_ord_line_sup_dispatch", "cust_ord_line_sup_dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        topology.put("cust_ord_line_sup_join", new ArrayList<String>());
        try {
            Socket socket = new Socket(synefoAddress, synefoPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            SynefoMessage message = new SynefoMessage();
            message._values = new HashMap<>();
            message._values.put("TASK_TYPE", "TOPOLOGY");
            message._values.put("TASK_NUM", Integer.toString(numberOfTasks));
            out.writeObject(message);
            out.flush();
            Thread.sleep(100);
            out.writeObject(topology);
            out.flush();
            String ack = (String) in.readObject();
            if (!ack.equals("+EFO_ACK")) {
                System.err.println("failed to submit topology information to load balancer");
                System.exit(1);
            }
            in.close();
            out.close();
            socket.close();
        }catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conf.setDebug(false);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, numberOfWorkers);
        conf.setNumWorkers(numberOfWorkers);
        conf.setNumAckers(numberOfWorkers);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
                "-Xmx4096m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m " +
                        "-XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true"
        );
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        try {
            StormSubmitter.submitTopology("tpch-query-5", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
