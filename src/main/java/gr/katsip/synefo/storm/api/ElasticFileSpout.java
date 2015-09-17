package gr.katsip.synefo.storm.api;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.tpch.LocalFileProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by katsip on 9/15/2015.
 */
public class ElasticFileSpout extends BaseRichSpout {

    Logger logger = LoggerFactory.getLogger(ElasticFileSpout.class);

    private static int METRIC_FREQ_SEC = 2;

    private SpoutOutputCollector spoutOutputCollector;

    private String taskName;

    private String synefoAddress;

    private int synefoPort;

    private LocalFileProducer producer;

    private String zooAddress;

    private int workerPort;

    private int reportCounter;

    private ArrayList<String> downstreamTaskNames = null;

    private ArrayList<Integer> downstreamTaskIdentifiers = null;

    private ArrayList<String> activeDownstreamTaskNames = null;

    private ArrayList<Integer> activeDownstreamTaskIdentifiers = null;

    private int index;

    private ZooPet zookeeperClient;

    private String taskAddress;

    private int taskIdentifier;

    private AssignableMetric completeLatency;

    private AssignableMetric inputRate;

    private HashMap<Values, Long> tupleStatistics;

    public ElasticFileSpout(String taskName, String synefoIpAddress, Integer synefoPort,
                            LocalFileProducer producer, String zooAddress) {
        this.taskName = taskName;
        workerPort = -1;
        downstreamTaskNames = null;
        activeDownstreamTaskNames = null;
        this.synefoAddress = synefoIpAddress;
        this.synefoPort = synefoPort;
        this.producer = producer;
        this.zooAddress = zooAddress;
        reportCounter = 0;
    }

    public void register() {
        Socket socket;
        ObjectOutputStream output = null;
        ObjectInputStream input = null;
        socket = null;
        SynefoMessage msg = new SynefoMessage();
        msg._type = SynefoMessage.Type.REG;
        msg._values.put("TASK_TYPE", "SPOUT");
        msg._values.put("TASK_NAME", taskName);
        msg._values.put("WORKER_PORT", Integer.toString(workerPort));
        msg._values.put("TASK_IP", taskAddress);
        msg._values.put("TASK_ID", Integer.toString(taskIdentifier));
        try {
            socket = new Socket(synefoAddress, synefoPort);
            output = new ObjectOutputStream(socket.getOutputStream());
            input = new ObjectInputStream(socket.getInputStream());
            output.writeObject(msg);
            output.flush();
            msg = null;
            ArrayList<String> downstream = null;
            downstream = (ArrayList<String>) input.readObject();
            if(downstream != null && downstream.size() > 0) {
                downstreamTaskNames = new ArrayList<String>(downstream);
                downstreamTaskIdentifiers = new ArrayList<Integer>();
                for(String task : downstreamTaskNames) {
                    String[] tokens = task.split("[:@]");
                    downstreamTaskIdentifiers.add(Integer.parseInt(tokens[1]));
                }
            }else {
                downstreamTaskNames = new ArrayList<String>();
                downstreamTaskIdentifiers = new ArrayList<Integer>();
            }
            ArrayList<String> activeDownstream = null;
            activeDownstream = (ArrayList<String>) input.readObject();
            if(activeDownstream != null && activeDownstream.size() > 0) {
                activeDownstreamTaskNames = new ArrayList<String>(activeDownstream);
                activeDownstreamTaskIdentifiers = new ArrayList<Integer>();
                for(String task : activeDownstreamTaskNames) {
                    String[] tokens = task.split("[:@]");
                    activeDownstreamTaskIdentifiers.add(Integer.parseInt(tokens[1]));
                }
                index = 0;
            }else {
                activeDownstreamTaskNames = new ArrayList<String>();
                activeDownstreamTaskIdentifiers = new ArrayList<Integer>();
                index = 0;
            }
            /**
             * Closing channels of communication with
             * SynEFO server
             */
            output.flush();
            output.close();
            input.close();
            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (EOFException e) {
            logger.error("ELASTIC-SPOUT-" + taskName + ":" + taskIdentifier + " threw an exception: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            logger.error("ELASTIC-SPOUT-" + taskName + ":" + taskIdentifier + " threw an exception: " + e.getMessage());
        }
        /**
         * Handshake with ZooKeeper
         */
        zookeeperClient.start();
        zookeeperClient.getScaleCommand();
        logger.info("ELASTIC-SPOUT-" + taskName + ":" + taskIdentifier + " registered (time: " + System.currentTimeMillis() + ")");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> producerSchema = new ArrayList<String>();
        producerSchema.add("SYNEFO_HEADER");
        producerSchema.addAll(producer.getSchema().toList());
        outputFieldsDeclarer.declare(new Fields(producerSchema));
    }

    private void initMetrics(TopologyContext context) {
        completeLatency = new AssignableMetric(null);
        context.registerMetric("comp-latency", completeLatency, METRIC_FREQ_SEC);
        tupleStatistics = new HashMap<Values, Long>();
        inputRate = new AssignableMetric(null);
        context.registerMetric("input-rate", inputRate, METRIC_FREQ_SEC);
    }

    public void ack(Object msgId) {
        Long currentTimestamp = System.currentTimeMillis();
        Values values = (Values) msgId;
        if (tupleStatistics.containsKey(values)) {
            Long emitTimestamp = tupleStatistics.remove(values);
            completeLatency.setValue((currentTimestamp - emitTimestamp));
        }
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        taskIdentifier = topologyContext.getThisTaskId();
        workerPort = topologyContext.getThisWorkerPort();
        /**
         * Update taskName with task-id so that multi-core is supported
         */
        taskName = taskName + "_" + taskIdentifier;
        try {
            taskAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        }
        zookeeperClient = new ZooPet(zooAddress, taskName, taskIdentifier, taskAddress);
        if (activeDownstreamTaskNames == null && downstreamTaskNames == null)
            register();
        initMetrics(topologyContext);
        try {
            producer.init();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        if (activeDownstreamTaskIdentifiers != null && activeDownstreamTaskNames.size() > 0) {
            int value = producer.nextTuple(spoutOutputCollector,
                    activeDownstreamTaskIdentifiers.get(index), tupleStatistics);
            if (value >= 0) {
                inputRate.setValue(value);
            }
            if (index >= (activeDownstreamTaskIdentifiers.size() - 1))
                index = 0;
            else
                index += 1;
        }
    }
}
