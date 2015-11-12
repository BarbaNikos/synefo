package gr.katsip.deprecated.deprecated;

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
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.NewJoinJoiner;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.WindowDispatcher;
import gr.katsip.tpch.LineItem;
import gr.katsip.synefo.storm.producers.ControlledFileProducer;
import gr.katsip.tpch.Order;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by katsip on 10/12/2015.
 * @deprecated
 */
public class WindowDispatchTopology {
    public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException,
            ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
        String synefoAddress = "";
        Integer synefoPort = 5555;
        String[] data = null;
        String zooIP = "";
        Integer scaleFactor = -1;
        HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
        ArrayList<String> taskList;
        Integer taskNumber = 0;
        Integer windowSizeInMinutes = -1;
        Integer workerNum = -1;
        Integer maxSpoutPending = 250;
        if(args.length < 7) {
            System.err.println("Arguments: <synefo-IP> <file-1,file-2> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN> <S: scale factor> <W: window size in minutes> <N: number of workers> <MP: max spout pending>");
            System.exit(1);
        }else {
            synefoAddress = args[0];
            data = args[1].split(",");
            zooIP = args[2];
            scaleFactor = Integer.parseInt(args[3]);
            windowSizeInMinutes = 60000 * Integer.parseInt(args[4]);
            workerNum = Integer.parseInt(args[5]);
            maxSpoutPending = Integer.parseInt(args[6]);
        }
        int executorNumber = scaleFactor;
        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        /**
         * Stage 0: 2 input streams (lineitem, orders)
         */
        String[] dataSchema = { "attributes", "values" };
        double[] outputRate = { 3000, 3000, 3000};
        int[] checkpoints = { 0, 30, 60 };
        ControlledFileProducer order = new ControlledFileProducer(data[0], Order.schema, Order.query5Schema, outputRate, checkpoints);
        order.setSchema(new Fields(dataSchema));
        ControlledFileProducer lineitem = new ControlledFileProducer(data[1], LineItem.schema, LineItem.query5Schema, outputRate, checkpoints);
        lineitem.setSchema(new Fields(dataSchema));
        builder.setSpout("order",
                new ElasticFileSpout("order", synefoAddress, synefoPort, order, zooIP), 2);
        taskNumber += 2;
        builder.setSpout("lineitem",
                new ElasticFileSpout("lineitem", synefoAddress, synefoPort, lineitem, zooIP), 2);
        taskNumber += 2;
        taskList = new ArrayList<String>();
        taskList.add("dispatch");
        topology.put("order", taskList);
        topology.put("lineitem", taskList);
        /**
         * Stage 1: join dispatchers
         */
        WindowDispatcher dispatcher = new WindowDispatcher("order", new Fields(Order.query5Schema),
                Order.query5Schema[0], Order.query5Schema[0],
                "lineitem", new Fields(LineItem.query5Schema),
                LineItem.query5Schema[0], LineItem.query5Schema[0], new Fields(dataSchema), windowSizeInMinutes*2, 1000);
        builder.setBolt("dispatch", new DispatchBolt("dispatch", synefoAddress, synefoPort, dispatcher, zooIP),
                executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("order")
                .directGrouping("lineitem");
        taskNumber += scaleFactor;
        taskList = new ArrayList<>();
        taskList.add("joinorder");
        taskList.add("joinline");
        topology.put("dispatch", taskList);
        /**
         * Stage 2 : join joiners
         */
        NewJoinJoiner joiner = new NewJoinJoiner("order", new Fields(Order.query5Schema), "lineitem",
                new Fields(LineItem.query5Schema), "O_ORDERKEY", "L_ORDERKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinorder", new JoinBolt("joinorder", synefoAddress, synefoPort,
                joiner, zooIP), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("dispatch");
        taskNumber += scaleFactor;
        topology.put("joinorder", new ArrayList<String>());

        joiner = new NewJoinJoiner("lineitem", new Fields(LineItem.query5Schema),
                "order", new Fields(Order.query5Schema), "L_ORDERKEY", "O_ORDERKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinline", new JoinBolt("joinline", synefoAddress, synefoPort,
                joiner, zooIP), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("dispatch");
        taskNumber += scaleFactor;
        topology.put("joinline", new ArrayList<String>());
        /**
         * Notify SynEFO server about the
         * Topology
         */
        System.out.println("About to connect to synefo: " + synefoAddress + ":" + synefoPort);
        Socket synEFOSocket = new Socket(synefoAddress, synefoPort);
        ObjectOutputStream _out = new ObjectOutputStream(synEFOSocket.getOutputStream());
        ObjectInputStream _in = new ObjectInputStream(synEFOSocket.getInputStream());
        SynefoMessage msg = new SynefoMessage();
        msg._values = new HashMap<String, String>();
        msg._values.put("TASK_TYPE", "TOPOLOGY");
        msg._values.put("TASK_NUM", Integer.toString(taskNumber));
        _out.writeObject(msg);
        _out.flush();
        Thread.sleep(100);
        _out.writeObject(topology);
        _out.flush();
        String _ack = null;
        _ack = (String) _in.readObject();
        if(_ack.equals("+EFO_ACK") == false) {
            System.err.println("+EFO returned different message other than +EFO_ACK");
            System.exit(1);
        }
        _in.close();
        _out.close();
        synEFOSocket.close();

        conf.setDebug(false);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, scaleFactor);
        conf.setNumWorkers(workerNum);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
                "-Xmx4096m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true");
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);
        conf.setNumAckers(workerNum);
//        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
//        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
//        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        StormSubmitter.submitTopology("join-window-dispatch", conf, builder.createTopology());
    }
}
