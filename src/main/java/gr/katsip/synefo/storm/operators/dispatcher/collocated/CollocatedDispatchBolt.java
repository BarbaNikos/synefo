package gr.katsip.synefo.storm.operators.dispatcher.collocated;

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
import gr.katsip.synefo.metric.StatisticFileWriter;
import gr.katsip.synefo.storm.operators.ZookeeperClient;
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
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Nick R. Katsipoulakis on 10/22/2015.
 */
public class CollocatedDispatchBolt extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(CollocatedDispatchBolt.class);

//    private static final int METRIC_REPORT_FREQ_SEC = 1;

    private static final int LOAD_CHECK_PERIOD = 200;

    private static final int LOAD_RELUCTANCY = 3;

    private OutputCollector collector;

    private String taskName;

    private String streamIdentifier;

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

//    private transient AssignableMetric executeLatency;
//
//    private transient AssignableMetric stateSize;
//
//    private transient AssignableMetric inputRate;
//
//    private transient AssignableMetric throughput;
//
//    private transient AssignableMetric stateTransferTime;
//
//    private transient AssignableMetric controlTupleInterval;

    private StatisticFileWriter writer;

    private long startTransferTimestamp;

    private int temporaryInputRate;

    private int temporaryThroughput;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    /**
     * Scale action information
     */
    private String action;

    private HashMap<Integer, List<Long>> capacityHistory;

    private HashMap<Integer, Long> responseTime;

    private List<Integer> strugglersHistory;

    private List<Integer> slackersHistory;

    private boolean SCALE_ACTION_FLAG = false;

    private List<String> migratedKeys;

    private int scaledTask;

    private int candidateTask;

    private boolean AUTO_SCALE;

    public CollocatedDispatchBolt(String taskName, String synefoAddress, Integer synefoPort,
                                  CollocatedWindowDispatcher dispatcher, String zookeeperAddress, boolean AUTO_SCALE) {
        this.taskName = taskName;
        streamIdentifier = taskName;
        this.workerPort = -1;
        this.synefoAddress = synefoAddress;
        this.synefoPort = synefoPort;
        downstreamTaskNames = null;
        downstreamTaskIdentifiers = null;
        activeDownstreamTaskNames = null;
        activeDownstreamTaskIdentifiers = null;
        this.dispatcher = dispatcher;
        this.zookeeperAddress = zookeeperAddress;
        this.AUTO_SCALE = AUTO_SCALE;
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
        throughputPreviousTimestamp = System.currentTimeMillis();
        temporaryInputRate = 0;
        temporaryThroughput = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskIdentifier = topologyContext.getThisTaskId();
        workerPort = topologyContext.getThisWorkerPort();
        streamIdentifier = taskName;
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
        if (!AUTO_SCALE)
            SCALE_ACTION_FLAG = true;
        else
            SCALE_ACTION_FLAG = false;
        migratedKeys = new ArrayList<>();
        strugglersHistory = new ArrayList<>();
        slackersHistory = new ArrayList<>();
        capacityHistory = new HashMap<>();
        responseTime = new HashMap<>();
        scaledTask = -1;
        candidateTask = -1;
        action = "";
        zookeeperClient.clearActionData();
        /**
         * Just added the following cause it is not needed anymore
         */
        zookeeperClient.disconnect();
        writer = new StatisticFileWriter("/u/katsip", taskName);
    }

    private void initMetrics(TopologyContext context) {
//        executeLatency = new AssignableMetric(null);
//        stateSize = new AssignableMetric(null);
//        inputRate = new AssignableMetric(null);
//        throughput = new AssignableMetric(null);
//        stateTransferTime = new AssignableMetric(null);
//        controlTupleInterval = new AssignableMetric(null);
//        context.registerMetric("execute-latency", executeLatency, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("state-size", stateSize, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("input-rate", inputRate, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("throughput", throughput, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("state-transfer", stateTransferTime, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("control-interval", controlTupleInterval, METRIC_REPORT_FREQ_SEC);
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
                long end = System.currentTimeMillis();
                int task = tuple.getSourceTask();
                long start = Long.parseLong(header.split(":")[1]);
                List<Long> intervals;
                if (capacityHistory.containsKey(task)) {
                    intervals = capacityHistory.get(task);
                } else {
                    intervals = new ArrayList<>();
                }
                responseTime.put(task, new Long( (end - start) ));
                intervals.add((end - start));
                capacityHistory.put(task, intervals);
                collector.ack(tuple);
//                controlTupleInterval.setValue(tuple.getSourceTask() + "-" + (end - start));
                writer.writeData(System.currentTimeMillis() + ",control-interval," + tuple.getSourceTask() + "-" + (end - start) + "\n");
                return;
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
                numberOfTuplesDispatched = dispatcher.execute(streamIdentifier + "-data", tuple, collector, fields,
                        tupleValues, migratedKeys, scaledTask, candidateTask, this.action);
            }else {
                numberOfTuplesDispatched = dispatcher.execute(streamIdentifier + "-data", tuple, null, fields,
                        tupleValues, migratedKeys, scaledTask, candidateTask, this.action);
            }
            long endTime = System.currentTimeMillis();

            collector.ack(tuple);

            throughputCurrentTimestamp = System.currentTimeMillis();
            if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                throughputPreviousTimestamp = throughputCurrentTimestamp;
//                throughput.setValue(temporaryThroughput);
                writer.writeData(throughputCurrentTimestamp + ",throughput," + temporaryThroughput + "\n");
//                inputRate.setValue(temporaryInputRate);
                writer.writeData(throughputCurrentTimestamp + ",input-rate," + temporaryInputRate + "\n");
                temporaryThroughput = 0;
                temporaryInputRate = 0;
//                executeLatency.setValue((endTime - startTime));
                writer.writeData(throughputCurrentTimestamp + ",execute-latency," + (endTime - startTime) + "\n");
//                stateSize.setValue(dispatcher.getStateSize());
                writer.writeData(throughputCurrentTimestamp + ",state-size," + dispatcher.getStateSize() + "\n");
            } else {
                temporaryThroughput += numberOfTuplesDispatched;
                temporaryInputRate++;
            }
            if (!SCALE_ACTION_FLAG) {
                tupleCounter++;
                if (tupleCounter >= LOAD_CHECK_PERIOD)
                    scale();
            }
        }
    }

    public void scale() {
        tupleCounter = 0;
        //Send out interval control tuple every LOAD_CHECK_PERIOD
        /**
         * Send out CTRL tuples if no scale-action is in progress
         */
        Values controlTuple = new Values();
        StringBuilder stringBuilder = new StringBuilder();
        long timestamp = System.currentTimeMillis();
        stringBuilder.append(SynefoConstant.COL_TICK_HEADER + ":" + timestamp);
        controlTuple.add(stringBuilder.toString());
        controlTuple.add(null);
        controlTuple.add(null);
        for (Integer task : activeDownstreamTaskIdentifiers) {
            collector.emitDirect(task, streamIdentifier + "-control", controlTuple);
        }
        /**
         * Check if one of the nodes is overloaded
         */
        int overloadedTask = -1, slackerTask = -1;
        long maxNumberOfTuples = 0, minNumberOfTuples = Long.MAX_VALUE;
        DescriptiveStatistics statistics = new DescriptiveStatistics();
        HashMap<Integer, Long> numberOfTuplesPerTask = dispatcher.getNumberOfTuplesPerTask();
        for (Integer task : numberOfTuplesPerTask.keySet()) {
            if (activeDownstreamTaskIdentifiers.contains(task)) {
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
        }
        if (!SCALE_ACTION_FLAG && activeDownstreamTaskIdentifiers.size() < downstreamTaskIdentifiers.size() && overloadedTask != -1) {
            if (strugglersHistory.size() >= (LOAD_RELUCTANCY * 3)) {
                List<Integer> temp = new ArrayList<>(strugglersHistory.subList(strugglersHistory.size() - LOAD_RELUCTANCY,
                        strugglersHistory.size()));
                strugglersHistory.clear();
                strugglersHistory.addAll(temp);
            }
            strugglersHistory.add(overloadedTask);
            if (strugglersHistory.size() >= LOAD_RELUCTANCY) {
                boolean scaleNeeded = true;
                for (int i = strugglersHistory.size() - 1; i >= (strugglersHistory.size() - Math.ceil(LOAD_RELUCTANCY / 2)) && i >= 0; i--) {
                    if (strugglersHistory.get(i) != overloadedTask) {
                        scaleNeeded = false;
                        break;
                    }
                }
                long responseInterval = -1L;
                if (responseTime.containsKey(overloadedTask))
                    responseInterval = responseTime.get(overloadedTask);
                if (scaleNeeded && responseInterval > 0 && responseInterval > 2L) {
                    scaledTask = overloadedTask;
                    //Overloaded task id is overloadedTask
                    //Divide tasks keys into two sets (migratedKeys are going to be handled by new task
                    List<String> keys = dispatcher.getKeysForATask(scaledTask);
                    migratedKeys.addAll(keys.subList(0, (int) Math.ceil((double) (keys.size() / 2))));
                    if (migratedKeys.size() == 0) {
                        scaledTask = -1;
                        migratedKeys.clear();
                        return;
                    }
                    SCALE_ACTION_FLAG = true;
                    action = SynefoConstant.COL_ADD_ACTION;
                    //Pick random in-active task (candidate)
                    List<Integer> candidates = new ArrayList<>(downstreamTaskIdentifiers);
                    candidates.removeAll(activeDownstreamTaskIdentifiers);
                    Random random = new Random();
                    candidateTask = candidates.get(random.nextInt(candidates.size()));
                    logger.info("decided to scale-out " + scaledTask + " and transfer keys " + migratedKeys.toString() +
                            " to task " + candidateTask);
                    /**
                     * The following part is for experimenting for instant transfer of state
                     * The dispatcher drops all information on dispatching to see if that sets
                     * the latency back to acceptable levels.
                     *
                     */
                    dispatcher.reassignKeys(migratedKeys, scaledTask, candidateTask);
//                    dispatcher.reinitializeBuffer();
                    /**
                     * END of experiment for instant transfer of state
                     */
                    stringBuilder = new StringBuilder();
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
                    collector.emitDirect(scaledTask, streamIdentifier + "-control", scaleTuple);
                    collector.emitDirect(candidateTask, streamIdentifier + "-control", scaleTuple);
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
        //Check for scale-in action
        /**
         * Dispatcher makes a histogram of tuples per node
         * and scales-in the one that has the least tuples (global minimum)
         */
//        if (activeDownstreamTaskIdentifiers.size() > 1 && slackerTask != -1 && !SCALE_ACTION_FLAG) {
//            /**
//             * GC slacker-history
//             */
//            if (slackersHistory.size() >= (LOAD_RELUCTANCY * 3)) {
//                List<Integer> temp = new ArrayList<>(slackersHistory.subList(slackersHistory.size() - LOAD_RELUCTANCY,
//                        slackersHistory.size()));
//                slackersHistory.clear();
//                slackersHistory.addAll(temp);
//            }
//            slackersHistory.add(slackerTask);
//            if (slackersHistory.size() >= LOAD_RELUCTANCY * 2) {
//                boolean scaleNeeded = true;
//                for (int i = slackersHistory.size() - 1; i >= (slackersHistory.size() - LOAD_RELUCTANCY * 2) && i >= 0; i--) {
//                    if (slackersHistory.get(i) != slackerTask) {
//                        scaleNeeded = false;
//                        break;
//                    }
//                }
////                if (!scaleNeeded)
////                    logger.info("failed the reluctancy test for slacker-task: " + slackerTask + " history: " + slackersHistory);
//                long responseInterval = -1L;
//                if (responseTime.containsKey(slackerTask))
//                    responseInterval = responseTime.get(slackerTask);
////                if (responseInterval <= 2L)
////                    logger.info("succeeded the response interval test for slacker-task: " + slackerTask +
////                            ", reluctancy test: " + scaleNeeded + " interval: " + responseInterval);
//                if (scaleNeeded && responseInterval > 0 && responseInterval <= 2L) {
//                    scaledTask = slackerTask;
//                    List<String> keys = dispatcher.getKeysForATask(scaledTask);
//                    migratedKeys.addAll(keys);
//                    SCALE_ACTION_FLAG = true;
//                    action = SynefoConstant.COL_REMOVE_ACTION;
//                    List<Integer> candidates = new ArrayList<>(activeDownstreamTaskIdentifiers);
//                    candidates.remove(candidates.indexOf(scaledTask));
//                    Random random = new Random();
//                    candidateTask = candidates.get(random.nextInt(candidates.size()));
//                    logger.info("decided to scale-in " + scaledTask + " and transfer keys " + migratedKeys.toString() +
//                            " to task " + candidateTask);
//                    stringBuilder = new StringBuilder();
//                    stringBuilder.append(SynefoConstant.COL_SCALE_ACTION_PREFIX + ":" + SynefoConstant.COL_REMOVE_ACTION);
//                    stringBuilder.append("|" + SynefoConstant.COL_KEYS + ":");
//                    for (String key : migratedKeys) {
//                        stringBuilder.append(key + ",");
//                    }
//                    if (stringBuilder.length() > 0 && stringBuilder.charAt(stringBuilder.length() - 1) == ',')
//                        stringBuilder.setLength(stringBuilder.length() - 1);
//                    stringBuilder.append("|" + SynefoConstant.COL_PEER + ":" + scaledTask);
//                    Values scaleTuple = new Values();
//                    scaleTuple.add(stringBuilder.toString());
//                    scaleTuple.add("");
//                    scaleTuple.add("");
//                    collector.emitDirect(scaledTask, streamIdentifier + "-control", scaleTuple);
//                    collector.emitDirect(candidateTask, streamIdentifier + "-control", scaleTuple);
//                    startTransferTimestamp = System.currentTimeMillis();
//                    activeDownstreamTaskIdentifiers.remove(activeDownstreamTaskIdentifiers.indexOf(scaledTask));
//                    this.action = SynefoConstant.COL_REMOVE_ACTION;
//                    dispatcher.setTaskToRelationIndex(activeDownstreamTaskIdentifiers);
//                }
//            }
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> schema = new ArrayList<>();
        schema.add("SYNEFO_HEADER");
        schema.add("attributes");
        schema.add("values");
        outputFieldsDeclarer.declareStream(streamIdentifier + "-data", true, new Fields(schema));
        outputFieldsDeclarer.declareStream(streamIdentifier + "-control", true, new Fields(schema));
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
                    logger.info("completed scale-action " + action + " between scaled-task " + scaledTask + " and " +
                            candidateTask + " for keys " + migratedKeys.toString());
                    SCALE_ACTION_FLAG = false;
                    action = "";
                    migratedKeys.clear();
                    candidateTask = -1;
                    scaledTask = -1;
                    strugglersHistory.clear();
                    slackersHistory.clear();
                    tupleCounter = 0;
                    long timestamp = System.currentTimeMillis();
//                    stateTransferTime.setValue((timestamp - startTransferTimestamp));
                }
            }
        }
    }

}
