package gr.katsip.synefo.storm.topology;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.api.DispatchBolt;
import gr.katsip.synefo.storm.api.ElasticFileSpout;
import gr.katsip.synefo.storm.operators.relational.elastic.*;
import gr.katsip.synefo.tpch.LineItem;
import gr.katsip.synefo.tpch.LocalFileProducer;
import gr.katsip.synefo.tpch.Order;

import javax.sound.sampled.Line;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

    public enum DispatcherType {
        OBLIVIOUS_DISPATCH,
        WINDOW_DISPATCH,
        HISTORY_DISPATCH
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

    public void submit(String synefoAddress, String zookeeperAddress, int maxSpoutPending, DispatcherType type) {
        Integer numberOfTasks = 0;
        ArrayList<String> tasks = null;
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
    }
}
