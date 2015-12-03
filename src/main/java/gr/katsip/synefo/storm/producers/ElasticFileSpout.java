package gr.katsip.synefo.storm.producers;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.ZookeeperClient;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.synefo.utils.SynefoConstant;
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
 * Created by Nick R. Katsipoulakis on 9/15/2015.
 */
public class ElasticFileSpout extends BaseRichSpout {

    Logger logger = LoggerFactory.getLogger(ElasticFileSpout.class);

    private static int METRIC_FREQ_SEC = 1;

    private SpoutOutputCollector spoutOutputCollector;

    private String taskName;

    private String streamIdentifier;

    private String synefoAddress;

    private int synefoPort;

    private FileProducer producer;

    private String zooAddress;

    private int workerPort;

    private ArrayList<String> downstreamTaskNames = null;

    private ArrayList<Integer> downstreamTaskIdentifiers = null;

    private ArrayList<String> activeDownstreamTaskNames = null;

    private ArrayList<Integer> activeDownstreamTaskIdentifiers = null;

    private int index;

    private ZookeeperClient zookeeperClient;

    private String taskAddress;

    private int taskIdentifier;

    private AssignableMetric completeLatency;

    private AssignableMetric inputRate;

    private HashMap<String, Long> tupleStatistics;

    private long startTime = -1;

    private long endTime = -1;

    private AssignableMetric timeToScanInput;

    private boolean fileScanned;

    public ElasticFileSpout(String taskName, String synefoIpAddress, Integer synefoPort,
                            FileProducer producer, String zooAddress) {
        streamIdentifier = taskName;
        this.taskName = taskName;
        workerPort = -1;
        downstreamTaskNames = null;
        activeDownstreamTaskNames = null;
        this.synefoAddress = synefoIpAddress;
        this.synefoPort = synefoPort;
        this.producer = producer;
        this.zooAddress = zooAddress;
        fileScanned = false;
    }

    public void register() {
        Socket socket;
        ObjectOutputStream output = null;
        ObjectInputStream input = null;
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
            ArrayList<String> downstream = (ArrayList<String>) input.readObject();
            if (downstream.size() > 0) {
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
            ArrayList<String> activeDownstream = (ArrayList<String>) input.readObject();
            if (activeDownstream.size() > 0) {
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
        zookeeperClient.init();
        zookeeperClient.getScaleCommand();
        logger.info("ELASTIC-SPOUT-" + taskName + ":" + taskIdentifier + " registered (time: " + System.currentTimeMillis() + ")");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> producerSchema = new ArrayList<String>();
        producerSchema.add("SYNEFO_HEADER");
        producerSchema.addAll(producer.getSchema().toList());
        outputFieldsDeclarer.declareStream(streamIdentifier, true, new Fields(producerSchema));
    }

    private void initMetrics(TopologyContext context) {
        completeLatency = new AssignableMetric(null);
        context.registerMetric("comp-latency", completeLatency, METRIC_FREQ_SEC);
        tupleStatistics = new HashMap<>();
        inputRate = new AssignableMetric(null);
        context.registerMetric("input-rate", inputRate, METRIC_FREQ_SEC);
        timeToScanInput = new AssignableMetric(null);
        context.registerMetric("time-to-scan", timeToScanInput, METRIC_FREQ_SEC);
    }

    public void ack(Object msgId) {
        Long currentTimestamp = System.currentTimeMillis();
        Values values = (Values) msgId;
        if (tupleStatistics.containsKey(values.toString())) {
            Long emitTimestamp = tupleStatistics.remove(values.toString());
            completeLatency.setValue((currentTimestamp - emitTimestamp));
        }
        if (fileScanned && tupleStatistics.isEmpty()) {
            logger.info("spout-" + taskName + "scanned file completely and all tuples were acknowledged.");
            endTime = System.currentTimeMillis();
            timeToScanInput.setValue((endTime - startTime));
            close();
        }
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        streamIdentifier = taskName;
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
        zookeeperClient = new ZookeeperClient(zooAddress, taskName, taskIdentifier, taskAddress);
        if (activeDownstreamTaskNames == null && downstreamTaskNames == null)
            register();
        initMetrics(topologyContext);
        producer.init();
        fileScanned = false;
    }

    @Override
    public void nextTuple() {
        if (startTime == -1)
            startTime = System.currentTimeMillis();
        if (fileScanned)
            return;
        if (activeDownstreamTaskIdentifiers != null && activeDownstreamTaskNames.size() > 0) {
            int value = producer.nextTuple(spoutOutputCollector, streamIdentifier,
                    activeDownstreamTaskIdentifiers.get(index), tupleStatistics);
            if (value >= 0) {
                inputRate.setValue(value);
            }else {
                if (value == -1) {
                    fileScanned = true;
                    return;
                }
            }
            if (index >= (activeDownstreamTaskIdentifiers.size() - 1))
                index = 0;
            else
                index += 1;
        }
        String command = "";
        if (!zookeeperClient.commands.isEmpty()) {
            command = zookeeperClient.commands.poll();
            manageCommand(command);
        }
    }

    private void manageCommand(String command) {
        if(command.length() > 0) {
            logger.info("ELASTIC-SPOUT-" + taskName + ":" + taskIdentifier + " received a scale-command \"" + command + "\"");
            String[] scaleCommandTokens = command.split("[~:@]");
            String action = scaleCommandTokens[0];
            String taskWithAddress = scaleCommandTokens[1] + ":" + scaleCommandTokens[2] + "@" + scaleCommandTokens[3];
            String taskAddress = scaleCommandTokens[3];
            String task = scaleCommandTokens[1];
            Integer taskIdentifier = Integer.parseInt(scaleCommandTokens[2]);
            StringBuilder strBuild = new StringBuilder();
            strBuild.append(SynefoConstant.PUNCT_TUPLE_TAG + "/");
            index = 0;
            if (action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {
                activeDownstreamTaskNames = new ArrayList<>(zookeeperClient.getActiveDownstreamTasks());
                activeDownstreamTaskIdentifiers = new ArrayList<>(zookeeperClient.getActiveDownstreamTaskIdentifiers());
            }else {
                if(action.toLowerCase().contains("add")) {
                    activeDownstreamTaskNames = new ArrayList<>(zookeeperClient.getActiveDownstreamTasks());
                    activeDownstreamTaskIdentifiers = new ArrayList<>(zookeeperClient.getActiveDownstreamTaskIdentifiers());
                    strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION + "/");
                }else if(action.toLowerCase().contains("remove")) {
                    strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION + "/");
                }
                strBuild.append(SynefoConstant.COMP_TAG + ":" + task + ":" + taskIdentifier + "/");
                strBuild.append(SynefoConstant.COMP_NUM_TAG + ":" + activeDownstreamTaskNames.size() + "/");
                strBuild.append(SynefoConstant.COMP_IP_TAG + ":" + taskAddress + "/");
                /**
                 * Populate other schema fields with null values,
                 * after SYNEFO_HEADER
                 */
                Values punctValue = new Values();
                punctValue.add(strBuild.toString());
                for(int i = 0; i < producer.getSchema().size(); i++)
                    punctValue.add(null);
                for(Integer d_task : activeDownstreamTaskIdentifiers)
                    spoutOutputCollector.emitDirect(d_task, punctValue);
                /**
                 * In the case of removing a downstream task
                 * we remove it after sending the punctuation tuples, so
                 * that the removed task is notified to share state
                 */
                if(action.toLowerCase().contains("remove") && activeDownstreamTaskNames.indexOf(taskWithAddress) >= 0) {
                    activeDownstreamTaskNames = new ArrayList<>(zookeeperClient.getActiveDownstreamTasks());
                    activeDownstreamTaskIdentifiers = new ArrayList<>(zookeeperClient.getActiveDownstreamTaskIdentifiers());
                }
            }
        }
    }
}
