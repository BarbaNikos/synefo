package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.api.ZookeeperClient;
import gr.katsip.synefo.utils.SynefoConstant;
import gr.katsip.synefo.utils.SynefoMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by katsip on 10/22/2015.
 */
public class CollocatedDispatchBolt extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(CollocatedDispatchBolt.class);

    private static final int METRIC_REPORT_FREQ_SEC = 5;

    private static final int WARM_UP_THRESHOLD = 10000;

    private OutputCollector collector;

    private String taskName;

    private int taskIdentifier;

    private String taskAddress;

    private int workerPort;

    private String synefoAddress;

    private int synefoPort;

    private String zookeeperAddress;

    private ZookeeperClient zookeeperClient;

    private List<String> downstreamTaskNames;

    private List<Integer> downstreamTaskIdentifiers;

    private List<String> activeDownstreamTaskNames;

    private List<Integer> activeDownstreamTaskIdentifiers;

    private CollocatedWindowDispatcher dispatcher;

    private boolean SYSTEM_WARM_FLAG;

    private int tupleCounter;

    private transient AssignableMetric executeLatency;

    private transient AssignableMetric stateSize;

    private transient AssignableMetric inputRate;

    private transient AssignableMetric throughput;

    private transient AssignableMetric stateTransferTime;

    private long startTransferTimestamp;

    private int temporaryInputRate;

    private int temporaryThroughput;

    private long lastExecuteLatencyMetric = 0L;

    private long lastStateSizeMetric = 0L;

    private long inputRateCurrentTimestamp;

    private long inputRatePreviousTimestamp;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    private boolean SCALE_ACTION_FLAG = false;

    private String scaleAction = "";

    private String elasticTask = "";

    private LinkedList<Tuple> scaleTupleBuffer;

    private int numberOfConnections;

    private int stateTaskNumber;

    private int stateTaskIdentifier;

    private boolean SCALE_RECEIVE_STATE;

    private boolean SCALE_SEND_STATE;

    private HashMap<String, List<Integer>> relationTaskIndex;

    public CollocatedDispatchBolt(String taskName, String synefoAddress, Integer synefoPort,
                                  CollocatedWindowDispatcher dispatcher, String zookeeperAddress) {
        this.taskName = taskName;
        this.workerPort = -1;
        this.synefoAddress = synefoAddress;
        this.synefoPort = synefoPort;
        downstreamTaskNames = null;
        downstreamTaskIdentifiers = null;
        activeDownstreamTaskNames = null;
        activeDownstreamTaskIdentifiers = null;
        this.dispatcher = dispatcher;
        this.zookeeperAddress = zookeeperAddress;
        SYSTEM_WARM_FLAG = false;
        tupleCounter = 0;
    }

    public void register() {
        Socket socket;
        ObjectOutputStream output;
        ObjectInputStream input;
        SynefoMessage msg = new SynefoMessage();
        msg._type = SynefoMessage.Type.REG;
        msg._values.put("TASK_IP", taskAddress);
        msg._values.put("TASK_TYPE", "JOIN_BOLT");
        msg._values.put("JOIN_STEP", "COL_DISPATCH");
        msg._values.put("TASK_NAME", taskName);
        msg._values.put("TASK_ID", Integer.toString(taskIdentifier));
        msg._values.put("WORKER_PORT", Integer.toString(workerPort));
        try {
            socket = new Socket(synefoAddress, synefoPort);
            output = new ObjectOutputStream(socket.getOutputStream());
            input = new ObjectInputStream(socket.getInputStream());
            output.writeObject(msg);
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": connected to synefo");
            ArrayList<String> downstream = (ArrayList<String>) input.readObject();
            if (downstream.size() > 0) {
                downstreamTaskNames = new ArrayList<>(downstream);
                downstreamTaskIdentifiers = new ArrayList<>();
                Iterator<String> itr = downstreamTaskNames.iterator();
                while (itr.hasNext()) {
                    String[] tokens = itr.next().split("[:@]");
                    Integer task = Integer.parseInt(tokens[1]);
                    downstreamTaskIdentifiers.add(task);
                }
            }else {
                downstreamTaskNames = new ArrayList<>();
                downstreamTaskIdentifiers = new ArrayList<>();
            }
            ArrayList<String> activeDownstream = (ArrayList<String>) input.readObject();
            if (activeDownstream.size() > 0) {
                activeDownstreamTaskNames = new ArrayList<>(activeDownstream);
                activeDownstreamTaskIdentifiers = new ArrayList<>();
                Iterator<String> itr = activeDownstreamTaskNames.iterator();
                while (itr.hasNext()) {
                    String[] tokens = itr.next().split("[:@]");
                    Integer task = Integer.parseInt(tokens[1]);
                    activeDownstreamTaskIdentifiers.add(task);
                }
            }else {
                activeDownstreamTaskNames = new ArrayList<>();
                activeDownstreamTaskIdentifiers = new ArrayList<>();
            }
            dispatcher.setTaskToRelationIndex(activeDownstreamTaskIdentifiers);
            output.close();
            input.close();
            socket.close();
        } catch (EOFException e) {
            logger.info("COL-DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": threw " + e.getMessage());
        } catch (IOException
                | ClassNotFoundException e) {
            logger.info("COL-DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": threw " + e.getMessage());
            e.printStackTrace();
        } catch (NullPointerException e) {
            logger.info("COL-DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": threw " + e.getMessage());
        }
        zookeeperClient.init();
        zookeeperClient.getScaleCommand();
        StringBuilder strBuild = new StringBuilder();
        strBuild.append("COL-DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": active tasks: ");
        for(String activeTask : activeDownstreamTaskNames) {
            strBuild.append(activeTask + " ");
        }
        logger.info(strBuild.toString());
        logger.info("COL-DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + " registered to load-balancer");
        inputRatePreviousTimestamp = System.currentTimeMillis();
        throughputPreviousTimestamp = System.currentTimeMillis();
        temporaryInputRate = 0;
        temporaryThroughput = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskIdentifier = topologyContext.getThisTaskId();
        workerPort = topologyContext.getThisWorkerPort();
        taskName = taskName + "_" + taskIdentifier;
        try {
            taskAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        }
        zookeeperClient = new ZookeeperClient(zookeeperAddress, taskName, taskIdentifier, taskAddress);
        if(downstreamTaskNames == null && activeDownstreamTaskNames == null)
            register();
        initMetrics(topologyContext);
        SYSTEM_WARM_FLAG = false;
        tupleCounter = 0;
        SCALE_ACTION_FLAG = false;
        elasticTask = "";
        scaleAction = "";
        zookeeperClient.clearActionData();
        SCALE_RECEIVE_STATE = false;
        SCALE_SEND_STATE = false;
    }

    private void initMetrics(TopologyContext context) {
        executeLatency = new AssignableMetric(null);
        stateSize = new AssignableMetric(null);
        inputRate = new AssignableMetric(null);
        throughput = new AssignableMetric(null);
        stateTransferTime = new AssignableMetric(null);
        context.registerMetric("execute-latency", executeLatency, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-size", stateSize, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("input-rate", inputRate, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("throughput", throughput, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-transfer", stateTransferTime, METRIC_REPORT_FREQ_SEC);
    }

    private boolean isScaleHeader(String header) {
        return (header.contains(SynefoConstant.PUNCT_TUPLE_TAG) == true &&
                header.contains(SynefoConstant.ACTION_PREFIX) == true &&
                header.contains(SynefoConstant.COMP_IP_TAG) == true &&
                header.split("/")[0].equals(SynefoConstant.PUNCT_TUPLE_TAG));
    }

    private boolean isStateHeader(String header) {
        return ( header.split("/")[0].equals(SynefoConstant.STATE_PREFIX) &&
                header.split("[/:]")[1].equals(SynefoConstant.COMP_TAG));
    }

    @Override
    public void execute(Tuple tuple) {
        String header = "";
        if (!tuple.getFields().contains("SYNEFO_HEADER")) {
            logger.error("COL-DISPATCH-BOLT-" + taskName + ":" + taskIdentifier +
                    " missing synefo header (source: " +
                    tuple.getSourceTask() + ")");
            collector.fail(tuple);
            return;
        }else {
            if (SCALE_ACTION_FLAG) {
                scaleTupleBuffer.addFirst(tuple);
                List<String> result = zookeeperClient.getScaleResult();
                if (result != null) {
                    String relation = "";
                    Iterator<Map.Entry<String, List<Integer>>> iterator = relationTaskIndex.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, List<Integer>> entry = iterator.next();
                        if (entry.getValue().lastIndexOf(Integer.parseInt(elasticTask.split(":")[1])) >= 0) {
                            relation = entry.getKey();
                            break;
                        }
                    }
                    //TODO: Not yet supported!
                    dispatcher.updateIndex(scaleAction, elasticTask, relation, result);
                    zookeeperClient.clearActionData();
                    elasticTask = "";
                    scaleAction = "";
                    SCALE_ACTION_FLAG = false;
                    while (!scaleTupleBuffer.isEmpty()) {
                        execute(scaleTupleBuffer.removeFirst());
                    }
                }
            }else {
                header = tuple.getString(tuple.getFields()
                        .fieldIndex("SYNEFO_HEADER"));
                if (header != null && !header.equals("") && header.contains("/") &&
                        isScaleHeader(header)) {
                    altManageScaleTuple(tuple);
                    collector.ack(tuple);
                    return;
                }else if ((SCALE_SEND_STATE || SCALE_RECEIVE_STATE) && isStateHeader(header)) {
                    manageStateTuple(tuple);
                    collector.ack(tuple);
                    return;
                }
                inputRateCurrentTimestamp = System.currentTimeMillis();
                if ((inputRateCurrentTimestamp - inputRatePreviousTimestamp) >= 1000L) {
                    inputRatePreviousTimestamp = inputRateCurrentTimestamp;
                    inputRate.setValue(temporaryInputRate);
                    zookeeperClient.addInputRateData((double) temporaryInputRate);
                    temporaryInputRate = 0;
                }else {
                    temporaryInputRate++;
                }
                /**
                 * Remove from both values and fields SYNEFO_HEADER (SYNEFO_TIMESTAMP)
                 */
                Values values = new Values(tuple.getValues().toArray());
                values.remove(0);
                Fields fields = new Fields(((Fields) values.get(0)).toList());
                Values tupleValues = (Values) values.get(1);
                int numberOfTuplesDispatched = 0;
                long startTime = System.currentTimeMillis();
                if (activeDownstreamTaskIdentifiers.size() > 0)
                    numberOfTuplesDispatched = dispatcher.execute(tuple, collector, fields, tupleValues);
                else
                    numberOfTuplesDispatched = dispatcher.execute(tuple, null, fields, tupleValues);
                collector.ack(tuple);
                temporaryThroughput += numberOfTuplesDispatched;
                long endTime = System.currentTimeMillis();
                lastExecuteLatencyMetric = endTime - startTime;
                lastStateSizeMetric = dispatcher.getStateSize();
                executeLatency.setValue(lastExecuteLatencyMetric);
                stateSize.setValue(lastStateSizeMetric);
                throughputCurrentTimestamp = System.currentTimeMillis();
                if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                    throughputPreviousTimestamp = throughputCurrentTimestamp;
                    throughput.setValue(temporaryThroughput);
                    //TODO: change the following
//                    zookeeperClient.addInputRateData((double) temporaryInputRate);
                    temporaryThroughput = 0;
                }
                tupleCounter++;
                if (tupleCounter >= WARM_UP_THRESHOLD && !SYSTEM_WARM_FLAG)
                    SYSTEM_WARM_FLAG = true;

                String command = "";
                if (!zookeeperClient.commands.isEmpty() && SCALE_ACTION_FLAG == false) {
                    command = zookeeperClient.commands.poll();
                    //TODO: Not yet!
                    manageCommand(command);
                }
            }
        }
    }

    public void manageStateTuple(Tuple tuple) {
        //TODO: Not yet!
    }

    public void altManageScaleTuple(Tuple tuple) {
        //TODO: Not yet!
    }

    public void manageCommand(String command) {
        //TODO: Not supported yet!
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> schema = new ArrayList<>();
        schema.add("SYNEFO_HEADER");
        schema.add("attributes");
        schema.add("values");
        outputFieldsDeclarer.declare(new Fields(schema));
    }
}
