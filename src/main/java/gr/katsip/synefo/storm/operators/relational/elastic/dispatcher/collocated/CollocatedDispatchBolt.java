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

    private static final int LOAD_CHECK_PERIOD = 2000;

    private static final int LOAD_RELUCTANCY = 3;

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

    /**
     * Scale action information
     */

    private List<Integer> overloadedTaskLog;

    private boolean SCALE_ACTION_FLAG = false;

    private List<String> migratedKeys;

    private int scaledTask;

    private int candidateTask;

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
        migratedKeys = null;
        overloadedTaskLog = new ArrayList<>();
        scaledTask = -1;
        candidateTask = -1;
        zookeeperClient.clearActionData();
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
        return (header.contains(SynefoConstant.COL_SCALE_ACTION_PREFIX) == true &&
                header.contains(SynefoConstant.COL_COMPLETE_ACTION) == true &&
                header.contains(SynefoConstant.COL_PEER) == true);
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
            header = tuple.getString(tuple.getFields()
                    .fieldIndex("SYNEFO_HEADER"));
            if (header != null && !header.equals("") && header.contains("/") &&
                    isScaleHeader(header)) {
                manageScaleTuple(header);
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
            if (activeDownstreamTaskIdentifiers.size() > 0) {
                if (SCALE_ACTION_FLAG)
                    numberOfTuplesDispatched = dispatcher.execute(tuple, collector, fields, tupleValues, migratedKeys,
                            scaledTask, candidateTask);
                else
                    numberOfTuplesDispatched = dispatcher.execute(tuple, collector, fields, tupleValues, null, -1, -1);
            }else {
                if (SCALE_ACTION_FLAG)
                    numberOfTuplesDispatched = dispatcher.execute(tuple, null, fields, tupleValues, migratedKeys,
                            scaledTask, candidateTask);
                else
                    numberOfTuplesDispatched = dispatcher.execute(tuple, null, fields, tupleValues, null, -1, -1);
            }
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
                temporaryThroughput = 0;
            }
            tupleCounter++;
            if (tupleCounter >= WARM_UP_THRESHOLD && !SYSTEM_WARM_FLAG)
                SYSTEM_WARM_FLAG = true;
            if (tupleCounter >= LOAD_CHECK_PERIOD && !SCALE_ACTION_FLAG) {
                /**
                 * Check if one of the nodes is overloaded
                 */
                if (activeDownstreamTaskIdentifiers.size() < downstreamTaskIdentifiers.size()) {
                    HashMap<Integer, Long> numberOfTuplesPerTask = dispatcher.getNumberOfTuplesPerTask();
                    int overloadedTask = -1;
                    long maxNumberOfTuples = 0;
                    for (Integer task : numberOfTuplesPerTask.keySet()) {
                        if (maxNumberOfTuples < numberOfTuplesPerTask.get(task)) {
                            overloadedTask = task;
                            maxNumberOfTuples = numberOfTuplesPerTask.get(task);
                        }
                    }
                    if (overloadedTask != -1) {
                        overloadedTaskLog.add(overloadedTask);
                        if (overloadedTaskLog.size() >= LOAD_RELUCTANCY) {
                            scaledTask = overloadedTask;
                            boolean scaleNeeded = true;
                            for (int i = overloadedTaskLog.size() - 1; i >= (overloadedTaskLog.size() - LOAD_RELUCTANCY); i--) {
                                if (overloadedTaskLog.get(i) != scaledTask)
                                    scaleNeeded = false;
                            }
                            if (scaleNeeded) {
                                //Overloaded task id is overloadedTask
                                //Divide tasks keys into two sets (migratedKeys are going to be handled by new task
                                List<String> keys = dispatcher.getKeysForATask(scaledTask);
                                migratedKeys = keys.subList(0, (keys.size() / 2));
                                SCALE_ACTION_FLAG = true;
                                //Pick random in-active task (candidate)
                                List<Integer> candidates = new ArrayList<>(downstreamTaskIdentifiers);
                                candidates.removeAll(activeDownstreamTaskIdentifiers);
                                Random random = new Random();
                                candidateTask = candidates.get(random.nextInt(candidates.size()));
                                //Set value for the candidate? or No?
                                StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder.append(SynefoConstant.COL_SCALE_ACTION_PREFIX + ":" + SynefoConstant.COL_ADD_ACTION);
                                stringBuilder.append("|" + SynefoConstant.COL_KEYS + ":");
                                for (String key : migratedKeys) {
                                    stringBuilder.append(key + ",");
                                }
                                if (stringBuilder.length() > 0)
                                    stringBuilder.setLength(stringBuilder.length() - 1);
                                stringBuilder.append("|" + SynefoConstant.COL_PEER + ":" + candidateTask);
                                Values scaleTuple = new Values();
                                scaleTuple.add(stringBuilder.toString());
                                scaleTuple.add("");
                                scaleTuple.add("");
                                collector.emitDirect(scaledTask, scaleTuple);
                                collector.emitDirect(candidateTask, scaleTuple);
                                startTransferTimestamp = System.currentTimeMillis();
                            }
                        }
                    }
                    //Check for scale-in action
                    //TODO: Finish that
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> schema = new ArrayList<>();
        schema.add("SYNEFO_HEADER");
        schema.add("attributes");
        schema.add("values");
        outputFieldsDeclarer.declare(new Fields(schema));
    }

    public void manageScaleTuple(String header) {
        String actionWithStatus = header.split("|")[0];
        String peerInformation = header.split("|")[2];
        if (SCALE_ACTION_FLAG && migratedKeys.size() > 0 && candidateTask != -1 && scaledTask != -1) {
            if (actionWithStatus.split(":")[1].equals(SynefoConstant.COL_COMPLETE_ACTION)) {
                if (peerInformation.split(":")[0].equals(SynefoConstant.COL_PEER) &&
                        peerInformation.split(":")[1].equals(Integer.toString(scaledTask))) {
                    /**
                     * SCALE-ACTION is complete
                     * Re-initialize
                     */
                    SCALE_ACTION_FLAG = false;
                    migratedKeys.clear();
                    candidateTask = -1;
                    scaledTask = -1;
                    tupleCounter = 0;
                    long timestamp = System.currentTimeMillis();
                    stateTransferTime.setValue((timestamp - startTransferTimestamp));
                }
            }
        }
    }

}
