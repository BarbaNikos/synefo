package gr.katsip.deprecated.deprecated;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import gr.katsip.deprecated.TpchTupleProducer;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.deprecated.JoinDispatcher;
import gr.katsip.deprecated.NaiveJoinJoiner;
import gr.katsip.tpch.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by katsip on 9/10/2015.
 * @deprecated
 */
public class NaiveTpchQueryFiveTopology {

    public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException,
            ClassNotFoundException, AlreadyAliveException, InvalidTopologyException {
        String synefoIP = "";
        Integer synefoPort = 5555;
        String[] streamIPs = null;
        String zooIP = "";
        Integer scaleFactor = -1;
        HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
        ArrayList<String> taskList;
        Integer taskNumber = 0;
        Integer windowSizeInMinutes = -1;
        Integer workerNum = -1;
        Integer maxSpoutPending = 250;
        if(args.length < 7) {
            System.err.println("Arguments: <synefo-IP> <stream-IP1:port1,stream-IP2:port2,...,streamIP4:port4> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN> <S: scale factor> <W: window size in minutes> <N: number of workers> <MP: max spout pending>");
            System.exit(1);
        }else {
            synefoIP = args[0];
            streamIPs = args[1].split(",");
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
         * Stage 0: 6 input streams (customer, lineitem, nation, orders, region, supplier)
         */
        String[] dataSchema = { "attributes", "values" };
        TpchTupleProducer customerProducer = new TpchTupleProducer(streamIPs[0], Customer.schema, Customer.query5schema);
        customerProducer.setSchema(new Fields(dataSchema));
        TpchTupleProducer orderProducer = new TpchTupleProducer(streamIPs[1], Order.schema, Order.query5Schema);
        orderProducer.setSchema(new Fields(dataSchema));
        TpchTupleProducer lineitemProducer = new TpchTupleProducer(streamIPs[2], LineItem.schema, LineItem.query5Schema);
        lineitemProducer.setSchema(new Fields(dataSchema));
        TpchTupleProducer supplierProducer = new TpchTupleProducer(streamIPs[3], Supplier.schema, Supplier.query5Schema);
        supplierProducer.setSchema(new Fields(dataSchema));
        builder.setSpout("customer",
                new SynefoSpout("customer", synefoIP, synefoPort, customerProducer, zooIP), 1);
        taskNumber += 1;
        builder.setSpout("order",
                new SynefoSpout("order", synefoIP, synefoPort, orderProducer, zooIP), 1);
        taskNumber += 1;
        builder.setSpout("lineitem",
                new SynefoSpout("lineitem", synefoIP, synefoPort, lineitemProducer, zooIP), 1);
        taskNumber += 1;
        builder.setSpout("supplier",
                new SynefoSpout("supplier", synefoIP, synefoPort, supplierProducer, zooIP), 1);
        taskNumber += 1;
        taskList = new ArrayList<String>();
        taskList.add("joindispatch");
        topology.put("customer", taskList);
        topology.put("order", new ArrayList<String>(taskList));
        taskList = new ArrayList<String>();
        taskList.add("joindispatch2");
        topology.put("supplier", new ArrayList<String>(taskList));
        topology.put("lineitem", new ArrayList<String>(taskList));
        /**
         * Stage 1a: join dispatchers
         */
        JoinDispatcher dispatcher = new JoinDispatcher("customer", new Fields(Customer.query5schema), "order",
                new Fields(Order.query5Schema), new Fields(dataSchema));
        builder.setBolt("joindispatch", new SynefoJoinBolt("joindispatch", synefoIP, synefoPort,
                dispatcher, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("customer")
                .directGrouping("order");
        taskNumber += scaleFactor;
        taskList = new ArrayList<String>();
        taskList.add("joinjoincust");
        taskList.add("joinjoinorder");
        topology.put("joindispatch", taskList);

        dispatcher = new JoinDispatcher("lineitem", new Fields(LineItem.query5Schema),
                "supplier", new Fields(Supplier.query5Schema), new Fields(dataSchema));
        builder.setBolt("joindispatch2", new SynefoJoinBolt("joindispatch2", synefoIP, synefoPort,
                dispatcher, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("lineitem")
                .directGrouping("supplier");
        taskNumber += scaleFactor;
        taskList = new ArrayList<String>();
        taskList.add("joinjoinline");
        taskList.add("joinjoinsup");
        topology.put("joindispatch2", taskList);

        /**
         * Stage 1b : join joiners
         */
        NaiveJoinJoiner joiner = new NaiveJoinJoiner("customer", new Fields(Customer.query5schema), "order",
                new Fields(Order.query5Schema), "C_CUSTKEY", "O_CUSTKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinjoincust", new SynefoJoinBolt("joinjoincust", synefoIP, synefoPort,
                joiner, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("joindispatch");
        taskNumber += scaleFactor;
        taskList = new ArrayList<String>();
        taskList.add("joindispatch3");
        topology.put("joinjoincust", taskList);
        joiner = new NaiveJoinJoiner("order", new Fields(Order.query5Schema), "customer",
                new Fields(Customer.query5schema), "O_CUSTKEY", "C_CUSTKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinjoinorder", new SynefoJoinBolt("joinjoinorder", synefoIP, synefoPort,
                joiner, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("joindispatch");
        taskNumber += scaleFactor;
        taskList = new ArrayList<String>();
        taskList.add("joindispatch3");
        topology.put("joinjoinorder", taskList);

        Fields joinOutputOne = joiner.getJoinOutputSchema();
        System.out.println("output-one schema: " + joinOutputOne.toList().toString());

        joiner = new NaiveJoinJoiner("lineitem", new Fields(LineItem.query5Schema),
                "supplier", new Fields(Supplier.query5Schema), "L_SUPPKEY", "S_SUPPKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinjoinline", new SynefoJoinBolt("joinjoinline", synefoIP, synefoPort,
                joiner, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("joindispatch2");
        taskNumber += scaleFactor;
        joiner = new NaiveJoinJoiner("supplier", new Fields(Supplier.query5Schema),
                "lineitem", new Fields(LineItem.query5Schema), "S_SUPPKEY", "L_SUPPKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinjoinsup", new SynefoJoinBolt("joinjoinsup", synefoIP, synefoPort,
                joiner, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("joindispatch2");
        taskNumber += scaleFactor;
        taskList = new ArrayList<String>();
        taskList.add("joindispatch3");
        topology.put("joinjoinline", new ArrayList<String>(taskList));
        topology.put("joinjoinsup", new ArrayList<String>(taskList));

        Fields joinOutputTwo = joiner.getJoinOutputSchema();
        System.out.println("output-two schema: " + joinOutputTwo.toList().toString());

        /**
         * Stage 2a: Dispatch of combine stream
         */
        dispatcher = new JoinDispatcher("outputone", joinOutputOne,
                "outputtwo", joinOutputTwo, new Fields(dataSchema));
        builder.setBolt("joindispatch3", new SynefoJoinBolt("joindispatch3", synefoIP, synefoPort,
                dispatcher, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("joinjoincust")
                .directGrouping("joinjoinorder")
                .directGrouping("joinjoinline")
                .directGrouping("joinjoinsup");
        taskNumber += scaleFactor;
        taskList = new ArrayList<String>();
        taskList.add("joinjoinoutputone");
        taskList.add("joinjoinoutputtwo");
        topology.put("joindispatch3", new ArrayList<String>(taskList));

        /**
         * Stage 2b: Join of combine stream
         */
        joiner = new NaiveJoinJoiner("outputone", new Fields(joinOutputOne.toList()), "outputtwo",
                new Fields(joinOutputTwo.toList()), "O_ORDERKEY", "L_ORDERKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinjoinoutputone", new SynefoJoinBolt("joinjoinoutputone", synefoIP, synefoPort,
                joiner, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("joindispatch3");
        taskNumber += scaleFactor;
        joiner = new NaiveJoinJoiner("outputtwo", new Fields(joinOutputTwo.toList()), "outputone",
                new Fields(joinOutputOne.toList()), "L_ORDERKEY", "O_ORDERKEY", windowSizeInMinutes, 1000);
        joiner.setOutputSchema(new Fields(dataSchema));
        builder.setBolt("joinjoinoutputtwo", new SynefoJoinBolt("joinjoinoutputtwo", synefoIP, synefoPort,
                joiner, zooIP, false), executorNumber)
                .setNumTasks(scaleFactor)
                .directGrouping("joindispatch3");
        taskNumber += scaleFactor;
        topology.put("joinjoinoutputone", new ArrayList<String>());
        topology.put("joinjoinoutputtwo", new ArrayList<String>());
        /**
         * Notify SynEFO server about the
         * Topology
         */
        System.out.println("About to connect to synefo: " + synefoIP + ":" + synefoPort);
        Socket synEFOSocket = new Socket(synefoIP, synefoPort);
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
                "-Xmx8192m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true");
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);
        conf.setNumAckers(workerNum);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        StormSubmitter.submitTopology("naive-tpch-q5", conf, builder.createTopology());
    }
}
