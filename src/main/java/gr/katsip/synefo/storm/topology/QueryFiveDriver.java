package gr.katsip.synefo.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.operators.dispatcher.DispatchBolt;
import gr.katsip.synefo.storm.operators.dispatcher.ObliviousDispatcher;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedDispatchBolt;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedDispatchWindow;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedWindowDispatcher;
import gr.katsip.synefo.storm.operators.joiner.JoinBolt;
import gr.katsip.synefo.storm.operators.joiner.Joiner;
import gr.katsip.synefo.storm.operators.joiner.WindowEquiJoin;
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

    public void submitStateOfTheArt() {
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
        tasks.add("cust-ord-dispatch");
        topology.put("customer", tasks);
        topology.put("order", new ArrayList<>(tasks));

        ObliviousDispatcher dispatcher = new ObliviousDispatcher("customer", new Fields(Customer.query5schema), Customer.query5schema[0],
                Customer.query5schema[0],
                "order", new Fields(Order.query5Schema), Order.query5Schema[1], Order.query5Schema[1], new Fields(schema));
        builder.setBolt("cust-ord-dispatch", new DispatchBolt("cust-ord-dispatch", synefoAddress, synefoPort, dispatcher, zookeeperAddress), 1)
                .directGrouping("customer", "customer")
                .directGrouping("order", "order")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("cust-ord-join");
        tasks.add("ord-cust-join");
        topology.put("cust-ord-dispatch", tasks);

        Joiner joiner = new Joiner("customer", new Fields(Customer.query5schema), "order", new Fields(Order.query5Schema), Customer.query5schema[0],
                Order.query5Schema[1], (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust-ord-join", new JoinBolt("cust-ord-join", synefoAddress, synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("cust-ord-dispatch", "cust-ord-dispatch-data")
                .directGrouping("cust-ord-dispatch", "cust-ord-dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        tasks = new ArrayList<>();
        tasks.add("cust-ord-line-sup-dispatch");
        topology.put("cust-ord-join", tasks);

        joiner = new Joiner("order", new Fields(Order.query5Schema), "customer", new Fields(Customer.query5schema), Order.query5Schema[1],
                Customer.query5schema[0], (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("ord-cust-join", new JoinBolt("ord-cust-join", synefoAddress, synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("cust-ord-dispatch", "cust-ord-dispatch-data")
                .directGrouping("cust-ord-dispatch", "cust-ord-dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        tasks = new ArrayList<>();
        tasks.add("cust-ord-line-sup-dispatch");
        topology.put("ord-cust-join", tasks);
        /**
         * LINEITEM.L_SUPPKEY = SUPPLIER.S_SUPPKEY
         */
        builder.setSpout("lineitem", new ElasticFileSpout("lineitem", synefoAddress, synefoPort, lineitemProducer, zookeeperAddress), 1);
        builder.setSpout("supplier", new ElasticFileSpout("supplier", synefoAddress, synefoPort, supplierProducer, zookeeperAddress), 1);
        numberOfTasks += 2;
        tasks = new ArrayList<>();
        tasks.add("line-sup-dispatch");
        topology.put("lineitem", tasks);
        topology.put("supplier", new ArrayList<>(tasks));

        dispatcher = new ObliviousDispatcher("lineitem", new Fields(LineItem.query5Schema), LineItem.query5Schema[1],
                LineItem.query5Schema[1], "supplier", new Fields(Supplier.schema), Supplier.query5Schema[0], Supplier.query5Schema[0], new Fields(schema));
        builder.setBolt("line-sup-dispatch", new DispatchBolt("line-sup-dispatch", synefoAddress, synefoPort, dispatcher, zookeeperAddress), 1)
                .directGrouping("lineitem", "lineitem")
                .directGrouping("supplier", "supplier")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("line-sup-join");
        tasks.add("sup-line-join");
        topology.put("line-sup-dispatch", tasks);


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
        tasks.add("cust-ord-dispatch");
        topology.put("customer", tasks);
        topology.put("order", new ArrayList<>(tasks));

        CollocatedWindowDispatcher dispatcher = new CollocatedWindowDispatcher("customer", new Fields(Customer.query5schema),
                Customer.query5schema[0], "order", new Fields(Order.query5Schema), Order.query5Schema[1], new Fields(schema),
                (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("cust-ord-dispatch", new CollocatedDispatchBolt("cust-ord-dispatch", synefoAddress, synefoPort, dispatcher,
                zookeeperAddress, AUTO_SCALE), 1)
                .directGrouping("customer", "customer")
                .directGrouping("order", "order")
                .directGrouping("cust-ord-join", "cust-ord-join-control")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("cust-ord-join");
        topology.put("cust-ord-dispatch", tasks);

        CollocatedEquiJoiner joiner = new CollocatedEquiJoiner("order", new Fields(Order.query5Schema), "customer", new Fields(Customer.query5schema),
                Order.query5Schema[1], Customer.query5schema[0], (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust-ord-join", new CollocatedJoinBolt("cust-ord-join", synefoAddress, synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("cust-ord-dispatch", "cust-ord-dispatch-data")
                .directGrouping("cust-ord-dispatch", "cust-ord-dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        tasks = new ArrayList<>();
        tasks.add("cust-ord-line-sup-dispatch");
        topology.put("cust-ord-join", tasks);

        Fields customerOrderJoinSchema = joiner.getOutputSchema();
        /**
         * LINEITEM.L_SUPPKEY = SUPPLIER.S_SUPPKEY
         */
        builder.setSpout("lineitem", new ElasticFileSpout("lineitem", synefoAddress, synefoPort, lineitemProducer, zookeeperAddress), 1);
        builder.setSpout("supplier", new ElasticFileSpout("supplier", synefoAddress, synefoPort, supplierProducer, zookeeperAddress), 1);
        numberOfTasks += 2;
        tasks = new ArrayList<>();
        tasks.add("line-sup-dispatch");
        topology.put("lineitem", tasks);
        topology.put("supplier", new ArrayList<>(tasks));

        dispatcher = new CollocatedWindowDispatcher("lineitem", new Fields(LineItem.query5Schema), LineItem.query5Schema[1],
                "supplier", new Fields(Supplier.query5Schema), Supplier.query5Schema[0], new Fields(schema),
                (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("line-sup-dispatch", new CollocatedDispatchBolt("line-sup-dispatch", synefoAddress, synefoPort, dispatcher,
                zookeeperAddress, AUTO_SCALE), 1)
                .directGrouping("lineitem", "lineitem")
                .directGrouping("supplier", "supplier")
                .directGrouping("line-sup-join", "line-sup-join-control")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("line-sup-join");
        topology.put("line-sup-dispatch", tasks);

        joiner = new CollocatedEquiJoiner("supplier", new Fields(Supplier.query5Schema),
                "lineitem", new Fields(LineItem.query5Schema), Supplier.query5Schema[0], LineItem.query5Schema[1],
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("line-sup-join", new CollocatedJoinBolt("line-sup-join", synefoAddress, synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("line-sup-dispatch", "line-sup-dispatch-data")
                .directGrouping("line-sup-dispatch", "line-sup-dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        tasks = new ArrayList<>();
        tasks.add("cust-ord-line-sup-dispatch");
        topology.put("line-sup-join", tasks);

        Fields lineitemSupplierJoinSchema = joiner.getOutputSchema();

        /**
         * CUSTOMER_ORDER.O_ORDERKEY = LINEITEM_SUPPLIER.L_ORDERKEY
         */
        dispatcher = new CollocatedWindowDispatcher("cust_ord", customerOrderJoinSchema, Order.query5Schema[1],
                "line_sup", lineitemSupplierJoinSchema, LineItem.query5Schema[0], new Fields(schema),
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust-ord-line-sup-dispatch", new CollocatedDispatchBolt("cust-ord-line-sup-dispatch", synefoAddress,
                synefoPort, dispatcher, zookeeperAddress, AUTO_SCALE), 1)
                .directGrouping("cust-ord-join", "cust-ord-join-data")
                .directGrouping("line-sup-join", "line-sup-join-data")
                .directGrouping("cust-ord-line-sup-join", "cust-ord-line-sup-join-control")
                .setNumTasks(1);
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("cust-ord-line-sup-join");
        topology.put("cust-ord-line-sup-dispatch", tasks);

        joiner = new CollocatedEquiJoiner("line_sup", lineitemSupplierJoinSchema, "cust_ord", customerOrderJoinSchema, LineItem.query5Schema[0], Order.query5Schema[1],
                (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
        builder.setBolt("cust-ord-line-sup-join", new CollocatedJoinBolt("cust-ord-line-sup-join", synefoAddress,
                synefoPort, joiner, zookeeperAddress), scale)
                .directGrouping("cust-ord-line-sup-dispatch", "cust-ord-line-sup-dispatch-data")
                .directGrouping("cust-ord-line-sup-dispatch", "cust-ord-line-sup-dispatch-control")
                .setNumTasks(scale);
        numberOfTasks += scale;
        topology.put("cust-ord-line-sup-join", new ArrayList<String>());
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
        conf.setDebug(true);
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
