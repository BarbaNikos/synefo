package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated;

import backtype.storm.Config;
import backtype.storm.Constants;
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
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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

    private static final int METRIC_REPORT_FREQ_SEC = 1;

    private static final int LOAD_CHECK_PERIOD = 200;

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

    private int tupleCounter;

    private transient AssignableMetric executeLatency;

    private transient AssignableMetric stateSize;

    private transient AssignableMetric inputRate;

    private transient AssignableMetric throughput;

    private transient AssignableMetric stateTransferTime;

    private transient AssignableMetric controlTupleInterval;

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

    private String action;

    private HashMap<Integer, List<Long>> capacityHistory;

    private List<Integer> strugglersHistory;

    private List<Integer> slackersHistory;

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
        tupleCounter = 0;
        SCALE_ACTION_FLAG = false;
        migratedKeys = new ArrayList<>();
        strugglersHistory = new ArrayList<>();
        slackersHistory = new ArrayList<>();
        capacityHistory = new HashMap<>();
        scaledTask = -1;
        candidateTask = -1;
        action = "";
        zookeeperClient.clearActionData();
    }

    private void initMetrics(TopologyContext context) {
        executeLatency = new AssignableMetric(null);
        stateSize = new AssignableMetric(null);
        inputRate = new AssignableMetric(null);
        throughput = new AssignableMetric(null);
        stateTransferTime = new AssignableMetric(null);
        controlTupleInterval = new AssignableMetric(null);
        context.registerMetric("execute-latency", executeLatency, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-size", stateSize, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("input-rate", inputRate, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("throughput", throughput, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-transfer", stateTransferTime, METRIC_REPORT_FREQ_SEC);
        context.registerMetric("control-interval", controlTupleInterval, METRIC_REPORT_FREQ_SEC);
    }

    public static boolean isScaleHeader(String header) {
        return (header.contains(SynefoConstant.COL_SCALE_ACTION_PREFIX) == true &&
                header.contains(SynefoConstant.COL_COMPLETE_ACTION) == true &&
                header.contains(SynefoConstant.COL_KEYS) == true &&
                header.contains(SynefoConstant.COL_PEER) == true);
    }

    public static boolean isControlTuple(String header) {
        return (header.contains(SynefoConstant.COL_TICK_HEADER + ":"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamIdentifier = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID) &&
                sourceStreamIdentifier.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void execute(Tuple tuple) {
        String header = "";
        if (isTickTuple(tuple)) {
//            if (!SCALE_ACTION_FLAG) {
                /**
                 * Send out CTRL tuples if no scale-action is in progress
                 */
                Values controlTuple = new Values();
                StringBuilder stringBuilder = new StringBuilder();
                long timestamp = System.nanoTime();
                stringBuilder.append(SynefoConstant.COL_TICK_HEADER + ":" + timestamp);
                controlTuple.add(stringBuilder.toString());
                controlTuple.add(null);
                controlTuple.add(null);
                for (Integer task : activeDownstreamTaskIdentifiers) {
                    collector.emitDirect(task, controlTuple);
                }
//            }
//            logger.info("received a tick-tuple");
            collector.ack(tuple);
            return;
        }
        if (!tuple.getFields().contains("SYNEFO_HEADER")) {
            logger.error("COL-DISPATCH-BOLT-" + taskName + ":" + taskIdentifier +
                    " missing synefo header (source: " +
                    tuple.getSourceTask() + ")");
            collector.fail(tuple);
            return;
        }else {
            header = tuple.getString(tuple.getFields()
                    .fieldIndex("SYNEFO_HEADER"));
            if (header != null && !header.equals("") && isScaleHeader(header)) {
                manageScaleTuple(header);
                collector.ack(tuple);
                return;
            }
            if (header != null && !header.equals("") && isControlTuple(header)) {
//                logger.info("dispatcher received control tuple.");
                int task = tuple.getSourceTask();
                long start = Long.parseLong(header.split(":")[1]);
                long end = System.nanoTime();
                List<Long> intervals = null;
//                if (capacityHistory.containsKey(task))
//                    intervals = capacityHistory.get(intervals);
//                else
//                    intervals = new ArrayList<>();
//                intervals.add(end - start);
//                capacityHistory.put(task, intervals);
                collector.ack(tuple);
//                logger.info("received control interval tuple (" + (end - start) + ")");
                controlTupleInterval.setValue(tuple.getSourceTask() + "-" + (end - start));
                return;
            }

            inputRateCurrentTimestamp = System.currentTimeMillis();
            if ((inputRateCurrentTimestamp - inputRatePreviousTimestamp) >= 1000L) {
                inputRatePreviousTimestamp = inputRateCurrentTimestamp;
                inputRate.setValue(temporaryInputRate);
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
                numberOfTuplesDispatched = dispatcher.execute(tuple, collector, fields, tupleValues, migratedKeys,
                        scaledTask, candidateTask, this.action);
            }else {
                numberOfTuplesDispatched = dispatcher.execute(tuple, null, fields, tupleValues, migratedKeys,
                        scaledTask, candidateTask, this.action);
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
            if (!SCALE_ACTION_FLAG)
                tupleCounter++;
            if (tupleCounter >= LOAD_CHECK_PERIOD && !SCALE_ACTION_FLAG)
                scale();
        }
    }

    public void scale() {
        /**
         * Check if one of the nodes is overloaded
         */
        DescriptiveStatistics statistics = new DescriptiveStatistics();
        int overloadedTask = -1, slackerTask = -1;
        long maxNumberOfTuples = 0, minNumberOfTuples = Long.MAX_VALUE;
        HashMap<Integer, Long> numberOfTuplesPerTask = dispatcher.getNumberOfTuplesPerTask();
        for (Integer task : numberOfTuplesPerTask.keySet()) {
            if (maxNumberOfTuples < numberOfTuplesPerTask.get(task)) {
                overloadedTask = task;
                maxNumberOfTuples = numberOfTuplesPerTask.get(task);
            }
            if (minNumberOfTuples > numberOfTuplesPerTask.get(task)) {
                minNumberOfTuples = numberOfTuplesPerTask.get(task);
                slackerTask = task;
            }
            statistics.addValue((double) numberOfTuplesPerTask.get(task));
        }
        if (activeDownstreamTaskIdentifiers.size() < downstreamTaskIdentifiers.size()) {
            if (overloadedTask != -1) {
                strugglersHistory.add(overloadedTask);
                if (strugglersHistory.size() >= LOAD_RELUCTANCY) {
                    boolean scaleNeeded = true;
                    for (int i = strugglersHistory.size() - 1; i >= (strugglersHistory.size() - LOAD_RELUCTANCY) && i >= 0; i--) {
                        if (strugglersHistory.get(i) != overloadedTask) {
                            scaleNeeded = false;
                            break;
                        }
                    }
                    if (scaleNeeded) {
                        scaledTask = overloadedTask;
                        //Overloaded task id is overloadedTask
                        //Divide tasks keys into two sets (migratedKeys are going to be handled by new task
                        List<String> keys = dispatcher.getKeysForATask(scaledTask);
                        migratedKeys = keys.subList(0, (int) Math.ceil((double) (keys.size() / 2)));
                        SCALE_ACTION_FLAG = true;
                        action = SynefoConstant.COL_ADD_ACTION;
                        //Pick random in-active task (candidate)
                        List<Integer> candidates = new ArrayList<>(downstreamTaskIdentifiers);
                        candidates.removeAll(activeDownstreamTaskIdentifiers);
                        Random random = new Random();
                        candidateTask = candidates.get(random.nextInt(candidates.size()));
                        logger.info("decided to scale-out " + scaledTask + " and transfer keys " + migratedKeys.toString() +
                                " to task " + candidateTask);
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append(SynefoConstant.COL_SCALE_ACTION_PREFIX + ":" + SynefoConstant.COL_ADD_ACTION);
                        stringBuilder.append("|" + SynefoConstant.COL_KEYS + ":");
                        for (String key : migratedKeys) {
                            stringBuilder.append(key + ",");
                        }
                        if (stringBuilder.length() > 0 && stringBuilder.charAt(stringBuilder.length() - 1) == ',')
                            stringBuilder.setLength(stringBuilder.length() - 1);
                        stringBuilder.append("|" + SynefoConstant.COL_PEER + ":" + candidateTask);
                        Values scaleTuple = new Values();
                        scaleTuple.add(stringBuilder.toString());
                        scaleTuple.add("");
                        scaleTuple.add("");
                        collector.emitDirect(scaledTask, scaleTuple);
                        collector.emitDirect(candidateTask, scaleTuple);
                        startTransferTimestamp = System.currentTimeMillis();
                        /**
                         * Add the candidate-task to the active task list
                         */
                        activeDownstreamTaskIdentifiers.add(candidateTask);
                        dispatcher.setTaskToRelationIndex(activeDownstreamTaskIdentifiers);
                        return;
                    }
                }
            }
        }
        //Check for scale-in action
        /**
         * Dispatcher makes a histogram of tuples per node
         * and scales-in the one that has the least tuples (global minimum)
         */
        if (activeDownstreamTaskIdentifiers.size() > 1 && slackerTask != -1 && SCALE_ACTION_FLAG != true) {
            slackersHistory.add(slackerTask);
            if (slackersHistory.size() >= LOAD_RELUCTANCY) {
                boolean scaleNeeded = true;
                for (int i = slackersHistory.size() - 1; i >= (slackersHistory.size() - LOAD_RELUCTANCY) && i >= 0; i--) {
                    if (slackersHistory.get(i) != slackerTask) {
                        scaleNeeded = false;
                        break;
                    }
                }
                if (scaleNeeded) {
                    scaledTask = slackerTask;
                    List<String> keys = dispatcher.getKeysForATask(scaledTask);
//                    migratedKeys = keys.subList(0, (int) Math.ceil((double) (keys.size() / 2)));
                    migratedKeys = keys;
                    SCALE_ACTION_FLAG = true;
                    action = SynefoConstant.COL_REMOVE_ACTION;
                    List<Integer> candidates = new ArrayList<>(activeDownstreamTaskIdentifiers);
                    candidates.remove(candidates.indexOf(scaledTask));
                    Random random = new Random();
                    candidateTask = candidates.get(random.nextInt(candidates.size()));
                    logger.info("decided to scale-in " + scaledTask + " and transfer keys " + migratedKeys.toString() +
                            " to task " + candidateTask);
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(SynefoConstant.COL_SCALE_ACTION_PREFIX + ":" + SynefoConstant.COL_REMOVE_ACTION);
                    stringBuilder.append("|" + SynefoConstant.COL_KEYS + ":");
                    for (String key : migratedKeys) {
                        stringBuilder.append(key + ",");
                    }
                    if (stringBuilder.length() > 0 && stringBuilder.charAt(stringBuilder.length() - 1) == ',')
                        stringBuilder.setLength(stringBuilder.length() - 1);
                    stringBuilder.append("|" + SynefoConstant.COL_PEER + ":" + scaledTask);
                    Values scaleTuple = new Values();
                    scaleTuple.add(stringBuilder.toString());
                    scaleTuple.add("");
                    scaleTuple.add("");
                    collector.emitDirect(scaledTask, scaleTuple);
                    collector.emitDirect(candidateTask, scaleTuple);
                    startTransferTimestamp = System.currentTimeMillis();
                    activeDownstreamTaskIdentifiers.remove(activeDownstreamTaskIdentifiers.indexOf(scaledTask));
                    this.action = SynefoConstant.COL_REMOVE_ACTION;
                    dispatcher.setTaskToRelationIndex(activeDownstreamTaskIdentifiers);
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
        String actionWithStatus = header.split("[|]")[0];
        String peerInformation = header.split("[|]")[2];
        if (SCALE_ACTION_FLAG && migratedKeys.size() > 0 && candidateTask != -1 && scaledTask != -1) {
            if (actionWithStatus.split(":")[1].equals(SynefoConstant.COL_COMPLETE_ACTION)) {
                if (peerInformation.split(":")[0].equals(SynefoConstant.COL_PEER) &&
                        peerInformation.split(":")[1].equals(Integer.toString(scaledTask))) {
                    /**
                     * SCALE-ACTION is complete
                     * Re-initialize
                     */
                    logger.info("completed scale-action " + action + " between scaled-task " + scaledTask + " and" +
                            candidateTask + " for keys " + migratedKeys.toString());
                    SCALE_ACTION_FLAG = false;
//                    if (this.action.equals(SynefoConstant.COL_REMOVE_ACTION)) {
//                        activeDownstreamTaskIdentifiers.remove(scaledTask);
//                    }
                    action = "";
                    migratedKeys.clear();
                    candidateTask = -1;
                    scaledTask = -1;
                    strugglersHistory.clear();
                    slackersHistory.clear();
                    tupleCounter = 0;
                    long timestamp = System.currentTimeMillis();
                    stateTransferTime.setValue((timestamp - startTransferTimestamp));
                }
            }
        }
    }

}
