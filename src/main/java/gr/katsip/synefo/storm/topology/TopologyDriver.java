package gr.katsip.synefo.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.api.DispatchBolt;
import gr.katsip.synefo.storm.api.ElasticFileSpout;
import gr.katsip.synefo.storm.api.JoinBolt;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated.CollocatedDispatchBolt;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated.CollocatedWindowDispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.NewJoinJoiner;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated.CollocatedEquiJoiner;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated.CollocatedJoinBolt;
import gr.katsip.synefo.storm.producers.FileProducer;
import gr.katsip.synefo.storm.producers.LocalControlledFileProducer;
import gr.katsip.synefo.storm.producers.LocalFileProducer;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.Dispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.HistoryDispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.ObliviousDispatcher;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.WindowDispatcher;
import gr.katsip.tpch.*;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by nick on 10/13/15.
 */
public class TopologyDriver {

    private int scale;

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

    private DispatcherType type;

    private FileReaderType readerType;

    public enum DispatcherType {
        OBLIVIOUS_DISPATCH,
        WINDOW_DISPATCH,
        HISTORY_DISPATCH,
        COLLOCATED_WINDOW_DISPATCH
    }

    public enum FileReaderType {
        DEFAULT_FILE_READER,
        CONTROLLED_FILE_READER
    }

    public TopologyDriver() {

    }

    public TopologyDriver(int scale, String[] inputFile, double[] outputRate, int[] checkpoint, float windowInMinutes,
                          long slideInMilliSeconds, int numberOfWorkers) {
        this.scale = scale;
        this.inputFile = inputFile;
        this.outputRate = outputRate;
        this.checkpoint = checkpoint;
        this.windowInMinutes = windowInMinutes;
        this.slideInMilliSeconds = slideInMilliSeconds;
        this.numberOfWorkers = numberOfWorkers;
    }

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
            numberOfWorkers = Integer.parseInt(reader.readLine().split("=")[1]);
            synefoAddress = reader.readLine().split("=")[1];
            zookeeperAddress = reader.readLine().split("=")[1];
            String strType = reader.readLine().split("=")[1].toUpperCase();
            if (DispatcherType.valueOf(strType) == DispatcherType.OBLIVIOUS_DISPATCH) {
                type = DispatcherType.OBLIVIOUS_DISPATCH;
            } else if (DispatcherType.valueOf(strType) == DispatcherType.WINDOW_DISPATCH) {
                type = DispatcherType.WINDOW_DISPATCH;
            } else if (DispatcherType.valueOf(strType) == DispatcherType.COLLOCATED_WINDOW_DISPATCH) {
                type = DispatcherType.COLLOCATED_WINDOW_DISPATCH;
            } else {
                type = DispatcherType.HISTORY_DISPATCH;
            }
            String strReaderType = reader.readLine().split("=")[1].toUpperCase();
            if (FileReaderType.valueOf(strReaderType) == FileReaderType.DEFAULT_FILE_READER) {
                readerType = FileReaderType.DEFAULT_FILE_READER;
            }else if (FileReaderType.valueOf(strReaderType) == FileReaderType.CONTROLLED_FILE_READER) {
                readerType = FileReaderType.CONTROLLED_FILE_READER;
            }else {
                readerType = FileReaderType.DEFAULT_FILE_READER;
            }
            System.out.println("driver located dispatcher type: " + type);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void submit(int maxSpoutPending) throws ClassNotFoundException {
        Integer numberOfTasks = 0;
        ArrayList<String> tasks;
        HashMap<String, ArrayList<String>> topology = new HashMap<>();
        int executorNumber = scale;
        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        FileProducer order, lineitem, supplier, customer;
        switch (readerType) {
            case DEFAULT_FILE_READER:
                order = new LocalFileProducer(inputFile[0], Order.schema, Order.schema);
                order.setSchema(new Fields(schema));
                lineitem = new LocalFileProducer(inputFile[1], LineItem.schema, LineItem.schema);
                lineitem.setSchema(new Fields(schema));
//                supplier = new LocalFileProducer(inputFile[2], Supplier.schema, Supplier.schema);
//                supplier.setSchema(new Fields(schema));
//                customer = new LocalFileProducer(inputFile[3], Customer.schema, Customer.schema);
//                customer.setSchema(new Fields(schema));
                break;
            case CONTROLLED_FILE_READER:
                order = new LocalControlledFileProducer(inputFile[0], Order.schema, Order.schema, outputRate,
                        checkpoint);
                order.setSchema(new Fields(schema));
                lineitem = new LocalControlledFileProducer(inputFile[1], LineItem.schema, LineItem.schema, outputRate,
                        checkpoint);
                lineitem.setSchema(new Fields(schema));
//                supplier = new LocalControlledFileProducer(inputFile[2], Supplier.schema, Supplier.schema, outputRate,
//                        checkpoint);
//                supplier.setSchema(new Fields(schema));
//                customer = new LocalControlledFileProducer(inputFile[3], Customer.schema, Customer.schema, outputRate,
//                        checkpoint);
//                customer.setSchema(new Fields(schema));
                break;
            default:
                order = new LocalFileProducer(inputFile[0], Order.schema, Order.schema);
                order.setSchema(new Fields(schema));
                lineitem = new LocalFileProducer(inputFile[1], LineItem.schema, LineItem.schema);
                lineitem.setSchema(new Fields(schema));
//                supplier = new LocalFileProducer(inputFile[2], Supplier.schema, Supplier.schema);
//                supplier.setSchema(new Fields(schema));
//                customer = new LocalFileProducer(inputFile[3], Customer.schema, Customer.schema);
//                customer.setSchema(new Fields(schema));
        }
        builder.setSpout("order", new ElasticFileSpout("order", synefoAddress, synefoPort, order, zookeeperAddress), scale);
        builder.setSpout("lineitem", new ElasticFileSpout("lineitem", synefoAddress, synefoPort, lineitem, zookeeperAddress), scale);
//        builder.setSpout("customer", new ElasticFileSpout("customer", synefoAddress, synefoPort, customer, zookeeperAddress), scale);
//        builder.setSpout("supplier", new ElasticFileSpout("supplier", synefoAddress, synefoPort, supplier, zookeeperAddress), scale);
        numberOfTasks += 2 * scale;
        tasks = new ArrayList<>();
        tasks.add("dispatch");
        topology.put("order", tasks);
        topology.put("lineitem", new ArrayList<>(tasks));

        Dispatcher dispatcher = null, dispatcher1 = null;
        CollocatedWindowDispatcher collocatedWindowDispatcher = null, collocatedWindowDispatcher1 = null;
        switch (type) {
            case OBLIVIOUS_DISPATCH:
                dispatcher = new ObliviousDispatcher("order", new Fields(Order.schema), Order.schema[0],
                        Order.schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema));
                break;
            case WINDOW_DISPATCH:
                dispatcher = new WindowDispatcher("order", new Fields(Order.schema), Order.schema[0],
                        Order.schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema),
                        (long) (windowInMinutes * 2 * (60 * 1000)), slideInMilliSeconds);
                break;
            case HISTORY_DISPATCH:
                dispatcher = new HistoryDispatcher("order", new Fields(Order.schema), Order.schema[0],
                        Order.schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema));
                break;
            case COLLOCATED_WINDOW_DISPATCH:
                collocatedWindowDispatcher = new CollocatedWindowDispatcher("order", new Fields(Order.schema), Order.schema[0],
                        "lineitem", new Fields(LineItem.schema), LineItem.schema[0],
                        new Fields(schema), (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
                break;
            default:
                dispatcher = new ObliviousDispatcher("order", new Fields(Order.schema), Order.query5Schema[0],
                        Order.query5Schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema));
        }
        if (collocatedWindowDispatcher == null) {
            builder.setBolt("dispatch", new DispatchBolt("dispatch", synefoAddress, synefoPort, dispatcher, zookeeperAddress),
                    scale)
                    .setNumTasks(scale)
                    .directGrouping("order")
                    .directGrouping("lineitem");
            numberOfTasks += scale;
            tasks = new ArrayList<>();
            tasks.add("joinorder");
            tasks.add("joinline");
            topology.put("dispatch", tasks);
            NewJoinJoiner joiner = new NewJoinJoiner("order", new Fields(Order.schema), "lineitem", new Fields(LineItem.schema),
                    "O_ORDERKEY", "L_ORDERKEY", (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
            joiner.setOutputSchema(new Fields(schema));
            builder.setBolt("joinorder", new JoinBolt("joinorder", synefoAddress, synefoPort, joiner, zookeeperAddress),
                    scale)
                    .setNumTasks(scale)
                    .directGrouping("dispatch");
            numberOfTasks += scale;
            topology.put("joinorder", new ArrayList<String>());

            joiner = new NewJoinJoiner("lineitem", new Fields(LineItem.schema), "order", new Fields(Order.schema),
                    "L_ORDERKEY", "O_ORDERKEY", (int) (windowInMinutes * (60 * 1000)), (int) slideInMilliSeconds);
            joiner.setOutputSchema(new Fields(schema));
            builder.setBolt("joinline", new JoinBolt("joinline", synefoAddress, synefoPort, joiner, zookeeperAddress),
                    scale)
                    .setNumTasks(scale)
                    .directGrouping("dispatch");
            numberOfTasks += scale;
            topology.put("joinline", new ArrayList<String>());
        }else {
            /**
             * Dispatcher's scale is only 1
             */
            builder.setBolt("dispatch", new CollocatedDispatchBolt("dispatch", synefoAddress, synefoPort,
                            collocatedWindowDispatcher, zookeeperAddress), 1)
                    .setNumTasks(1)
                    .directGrouping("order")
                    .directGrouping("lineitem");
            numberOfTasks += 1;
            tasks = new ArrayList<>();
            tasks.add("joiner");
            topology.put("dispatch", tasks);
            CollocatedEquiJoiner joiner = new CollocatedEquiJoiner("lineitem", new Fields(LineItem.schema), "order",
                    new Fields(Order.schema), LineItem.schema[0], Order.schema[0], (int) (windowInMinutes * (60 * 1000)),
                    (int) slideInMilliSeconds);
            joiner.setOutputSchema(new Fields(schema));
            builder.setBolt("joiner", new CollocatedJoinBolt("joiner", synefoAddress, synefoPort, joiner, zookeeperAddress),
                    scale)
                    .setNumTasks(scale)
                    .directGrouping("dispatch");
            numberOfTasks += scale;
            topology.put("joiner", new ArrayList<String>());
        }
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        conf.setDebug(false);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, numberOfTasks);
        conf.setNumWorkers(numberOfWorkers);
        conf.setNumAckers(numberOfWorkers);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000 -Xmx8192m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true"
        );
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        try {
            StormSubmitter.submitTopology("elastic-join", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
