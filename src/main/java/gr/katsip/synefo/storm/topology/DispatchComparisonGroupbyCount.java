package gr.katsip.synefo.storm.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedDispatchBolt;
import gr.katsip.synefo.storm.operators.dispatcher.collocated.CollocatedWindowDispatcher;
import gr.katsip.synefo.storm.operators.joiner.collocated.CollocatedGroupByCounter;
import gr.katsip.synefo.storm.operators.joiner.collocated.GroupbyBolt;
import gr.katsip.synefo.storm.producers.ControlledFileProducer;
import gr.katsip.synefo.storm.producers.ElasticFileSpout;
import gr.katsip.synefo.storm.producers.FileProducer;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.tpch.Inner;
import gr.katsip.tpch.Outer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by katsip on 1/22/2016.
 */
public class DispatchComparisonGroupbyCount {

    private static final String[] schema = { "attributes", "values" };

    private static final Integer synefoPort = 5555;

    private static final String synefoAddress = "localhost";

    private static int numberOfWorkers = 1;

    private static int maxSpoutPending = 500;

    /**
     * Application for executing Count Group-By with different dispatchers
     * @param args input-file-comma-separated-list outputRate checkpoint zookeeper-ip:zookeeper-port window-in-minutes slide
     */
    public static void main(String[] args) {
        if (args.length < 6) {
            System.exit(1);
        }
        String[] inputFile = args[0].split(",");
        String literaloutputRate = args[1];
        String literalCheckpoint = args[2];
        double[] outputRate = new double[1];
        outputRate[0] = Double.parseDouble(literaloutputRate);
        int[] checkpoint = new int[1];
        checkpoint[0] = Integer.parseInt(literalCheckpoint);
        String zookeeperAddress = args[3];
        int windowInMinutes = Integer.parseInt(args[4]);
        int slideInMilliSeconds = Integer.parseInt(args[5]);
        Integer numberOfTasks = 0;
        ArrayList<String> tasks;
        int scale = 1;
        HashMap<String, ArrayList<String>> topology = new HashMap<>();
        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        FileProducer inner, outer;
        inner = new ControlledFileProducer(inputFile[0], Inner.schema, Inner.schema, outputRate,
                checkpoint);
        inner.setSchema(new Fields(schema));
        outer = new ControlledFileProducer(inputFile[1], Outer.schema, Outer.schema, outputRate,
                checkpoint);
        outer.setSchema(new Fields(schema));
        builder.setSpout("inner", new ElasticFileSpout("inner", synefoAddress, synefoPort, inner, zookeeperAddress), 1);
        builder.setSpout("outer", new ElasticFileSpout("outer", synefoAddress, synefoPort, outer, zookeeperAddress), 1);
        numberOfTasks += 2;
        tasks = new ArrayList<>();
        tasks.add("dispatch");
        topology.put("inner", tasks);
        topology.put("outer", new ArrayList<>(tasks));
        CollocatedWindowDispatcher collocatedWindowDispatcher = new CollocatedWindowDispatcher("inner",
                new Fields(Inner.schema), Inner.schema[0], "outer", new Fields(Outer.schema), Outer.schema[0],
                new Fields(schema), (long) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        builder.setBolt("dispatch", new CollocatedDispatchBolt("dispatch", synefoAddress, synefoPort,
                collocatedWindowDispatcher, zookeeperAddress, false), 1)
                .setNumTasks(1)
                .directGrouping("inner", "inner")
                .directGrouping("outer", "outer")
                .directGrouping("groupby", "groupby-control");
        numberOfTasks += 1;
        tasks = new ArrayList<>();
        tasks.add("groupby");
        topology.put("dispatch", tasks);

        CollocatedGroupByCounter groupByCounter = new CollocatedGroupByCounter("inner", new Fields(Inner.schema), Inner.schema[1],
                (int) (windowInMinutes * (60 * 1000)), slideInMilliSeconds);
        groupByCounter.setOutputSchema(new Fields(schema));
        builder.setBolt("groupby", new GroupbyBolt("groupby", synefoAddress, synefoPort, groupByCounter, zookeeperAddress),
        scale)
                .setNumTasks(scale)
                .directGrouping("dispatch", "dispatch-control")
                .directGrouping("dispatch", "dispatch-data");
        numberOfTasks += scale;
        topology.put("groupby", new ArrayList<String>());
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
            StormSubmitter.submitTopology("dispatch-groupby", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
