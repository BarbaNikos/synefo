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
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.elastic.*;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.LocalFileProducer;
import gr.katsip.synefo.tpch.Order;
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

    public enum DispatcherType {
        OBLIVIOUS_DISPATCH,
        WINDOW_DISPATCH,
        HISTORY_DISPATCH
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
            }else if (DispatcherType.valueOf(strType) == DispatcherType.WINDOW_DISPATCH) {
                type = DispatcherType.WINDOW_DISPATCH;
            }else {
                type = DispatcherType.HISTORY_DISPATCH;
            }
            System.out.println("driver located dispatcher type: " + type);
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

        LocalFileProducer order = new LocalFileProducer(inputFile[0], Order.schema, Order.schema, outputRate,
                checkpoint);
        order.setSchema(new Fields(schema));
        LocalFileProducer lineitem = new LocalFileProducer(inputFile[1], LineItem.schema, LineItem.schema, outputRate,
                checkpoint);
        lineitem.setSchema(new Fields(schema));

        builder.setSpout("order", new ElasticFileSpout("order", synefoAddress, synefoPort, order, zookeeperAddress), scale);
        builder.setSpout("lineitem", new ElasticFileSpout("lineitem", synefoAddress, synefoPort, lineitem, zookeeperAddress), scale);
        numberOfTasks += 2*scale;
        tasks = new ArrayList<>();
        tasks.add("dispatch");
        topology.put("order", tasks);
        topology.put("lineitem", new ArrayList<>(tasks));

        Dispatcher dispatcher;
        switch (type) {
            case OBLIVIOUS_DISPATCH:
                dispatcher = new StatelessDispatcher("order", new Fields(Order.schema), Order.query5Schema[0],
                        Order.query5Schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema));
                break;
            case WINDOW_DISPATCH:
                dispatcher = new WindowDispatcher("order", new Fields(Order.schema), Order.query5Schema[0],
                        Order.query5Schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema),
                        (long) windowInMinutes * 2, slideInMilliSeconds);
                break;
            case HISTORY_DISPATCH:
                dispatcher = new FullStateDispatcher("order", new Fields(Order.schema), Order.query5Schema[0],
                        Order.query5Schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema));
                break;
            default:
                dispatcher = new StatelessDispatcher("order", new Fields(Order.schema), Order.query5Schema[0],
                        Order.query5Schema[0], "lineitem", new Fields(LineItem.schema),
                        LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(schema));
        }
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
                "O_ORDERKEY", "L_ORDERKEY", (int) windowInMinutes, (int) slideInMilliSeconds);
        joiner.setOutputSchema(new Fields(schema));
        builder.setBolt("joinorder", new JoinBolt("joinorder", synefoAddress, synefoPort, joiner, zookeeperAddress),
                scale)
                .setNumTasks(scale)
                .directGrouping("dispatch");
        numberOfTasks += scale;
        topology.put("joinorder", new ArrayList<String>());

        joiner = new NewJoinJoiner("lineitem", new Fields(LineItem.schema), "order", new Fields(Order.schema),
                "L_ORDERKEY", "O_ORDERKEY", (int) windowInMinutes, (int) slideInMilliSeconds);
        joiner.setOutputSchema(new Fields(schema));
        builder.setBolt("joinline", new JoinBolt("joinline", synefoAddress, synefoPort, joiner, zookeeperAddress),
                scale)
                .setNumTasks(scale)
                .directGrouping("dispatch");
        numberOfTasks += scale;
        topology.put("joinline", new ArrayList<String>());

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
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, scale);
        conf.setNumWorkers(numberOfWorkers);
        conf.setNumAckers(numberOfWorkers);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
                "-Xmx4096m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true"
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
