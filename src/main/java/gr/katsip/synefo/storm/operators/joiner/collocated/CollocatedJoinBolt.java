package gr.katsip.synefo.storm.operators.joiner.collocated;

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
import gr.katsip.synefo.utils.Pair;
import gr.katsip.synefo.utils.SynefoConstant;
import gr.katsip.synefo.utils.SynefoMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by Nick R. Katsipoulakis on 10/22/2015.
 */
public class CollocatedJoinBolt extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(CollocatedJoinBolt.class);

//    private static final int METRIC_REPORT_FREQ_SEC = 1;

    private OutputCollector collector;

    private String streamIdentifier;

    private String taskName;

    private Integer taskIdentifier;

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

    private Integer downstreamIndex;

    private CollocatedEquiJoiner joiner;

//    private transient AssignableMetric throughput;
//
//    private transient AssignableMetric executeLatency;
//
//    private transient AssignableMetric nonExecuteLatency;
//
//    private transient AssignableMetric stateSize;
//
//    private transient AssignableMetric inputRate;
//
//    private transient AssignableMetric stateTransferTime;
//
//    private transient AssignableMetric numberOfTuples;

    private StatisticFileWriter writer;

    private long startTransferTimestamp;

    private int temporaryInputRate;

    private int temporaryThroughput;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    private long lastStateSizeMetric = 0L;

    private List<String> migratedKeys;

    private int candidateTask;

    private String scaleAction;

    public CollocatedJoinBolt(String taskName, String synefoAddress, Integer synefoPort,
                              CollocatedEquiJoiner joiner, String zookeeperAddress) {
        this.taskName = taskName;
        streamIdentifier = taskName;
        this.workerPort = -1;
        this.synefoAddress = synefoAddress;
        this.synefoPort = synefoPort;
        downstreamTaskNames = null;
        downstreamTaskIdentifiers = null;
        activeDownstreamTaskNames = null;
        activeDownstreamTaskIdentifiers = null;
        this.joiner = joiner;
        this.zookeeperAddress = zookeeperAddress;
    }

    public void register() {
        Socket socket;
        ObjectOutputStream output;
        ObjectInputStream input;
        SynefoMessage message = new SynefoMessage();
        message._type = SynefoMessage.Type.REG;
        message._values.put("TASK_IP", taskAddress);
        message._values.put("TASK_TYPE", "JOIN_BOLT");
        message._values.put("JOIN_STEP", "COL_JOIN");
        message._values.put("TASK_NAME", taskName);
        message._values.put("TASK_ID", Integer.toString(taskIdentifier));
        message._values.put("WORKER_PORT", Integer.toString(workerPort));
        try {
            socket = new Socket(synefoAddress, synefoPort);
            output = new ObjectOutputStream(socket.getOutputStream());
            input = new ObjectInputStream(socket.getInputStream());
            output.writeObject(message);
            output.flush();
            logger.info("JOIN-BOLT-" + taskName + ":" + taskIdentifier + ": connected to synefo");
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
            output.close();
            input.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        zookeeperClient.init();
        zookeeperClient.getScaleCommand();
        StringBuilder strBuild = new StringBuilder();
        strBuild.append("COL-JOIN-BOLT-" + taskName + ":" + taskIdentifier + ": active tasks: ");
        for(String activeTask : activeDownstreamTaskNames) {
            strBuild.append(activeTask + " ");
        }
        logger.info(strBuild.toString());
        logger.info("COL-JOIN-BOLT-" + taskName + ":" + taskIdentifier + " registered to load-balancer");
        downstreamIndex = 0;
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
        } catch(UnknownHostException e) {
            e.printStackTrace();
        }
        zookeeperClient = new ZookeeperClient(zookeeperAddress, taskName, taskIdentifier, taskAddress);
        if(downstreamTaskNames == null && activeDownstreamTaskNames == null)
            register();
//        initMetrics(topologyContext);
        migratedKeys = new ArrayList<>();
        candidateTask = -1;
        scaleAction = "";
        zookeeperClient.disconnect();
        writer = new StatisticFileWriter("/u/katsip", taskName);
    }

    private void initMetrics(TopologyContext context) {
//        executeLatency = new AssignableMetric(null);
//        nonExecuteLatency = new AssignableMetric(null);
//        stateSize = new AssignableMetric(null);
//        inputRate = new AssignableMetric(null);
//        throughput = new AssignableMetric(null);
//        stateTransferTime = new AssignableMetric(null);
//        numberOfTuples = new AssignableMetric(null);
//        context.registerMetric("execute-latency", executeLatency, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("nonexecute-latency", nonExecuteLatency, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("state-size", stateSize, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("input-rate", inputRate, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("throughput", throughput, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("state-transfer", stateTransferTime, METRIC_REPORT_FREQ_SEC);
//        context.registerMetric("number-of-tuples", numberOfTuples, METRIC_REPORT_FREQ_SEC);
    }

    public static boolean isScaleHeader(String header) {
        return (header.contains(SynefoConstant.COL_SCALE_ACTION_PREFIX) == true &&
                (header.contains(SynefoConstant.COL_ADD_ACTION) == true || header.contains(SynefoConstant.COL_REMOVE_ACTION) == true) &&
                header.contains(SynefoConstant.COL_KEYS) == true &&
                header.contains(SynefoConstant.COL_PEER) == true);
    }

    public static boolean isControlTuple(String header) {
        return (header.contains(SynefoConstant.COL_TICK_HEADER + ":"));
    }

    @Override
    public void execute(Tuple tuple) {
        Long t1 = System.currentTimeMillis();
        String header = "";
        if (!tuple.getFields().contains("SYNEFO_HEADER")) {
            logger.error("JOIN-BOLT-" + taskName + ":" + taskIdentifier +
                    " missing synefo header (source: " +
                    tuple.getSourceTask() + ")");
            collector.fail(tuple);
            return;
        }else {
            header = tuple.getString(tuple.getFields()
                    .fieldIndex("SYNEFO_HEADER"));
            if (header != null && !header.equals("") && isScaleHeader(header)) {
                manageScaleTuple(t1, tuple, header);
                collector.ack(tuple);
                return;
            }else if (header != null && !header.equals("") && isControlTuple(header)) {
                Values controlTuple = new Values();
                controlTuple.add(header);
                controlTuple.add(null);
                controlTuple.add(null);
                collector.emitDirect(tuple.getSourceTask(), streamIdentifier + "-control", controlTuple);
                collector.ack(tuple);
                return;
            }else {
                /**
                 * Remove from both values and fields SYNEFO_HEADER (SYNEFO_TIMESTAMP)
                 */
                collector.ack(tuple);
                Values values = new Values(tuple.getValues().toArray());
                values.remove(0);
                Fields fields = null;
                if (values.size() <= 0)
                    logger.error("something is wrong, received a tuple without a schema: " + values.toString());
                try {
                    fields = new Fields(((Fields) values.get(0)).toList());
                }catch (ClassCastException e) {
                    logger.error("received tuple with values: " + tuple.getValues());
                    logger.error("tried to get fields but instead got: " + values.get(0));
                    e.printStackTrace();
                }
                Values tupleValues = (Values) values.get(1);
                List<Long> times = new ArrayList<>();
                Long t2 = System.currentTimeMillis();

                Pair<Integer, Integer> pair = joiner.execute(streamIdentifier + "-data", tuple, collector, activeDownstreamTaskIdentifiers,
                        downstreamIndex, fields, tupleValues, times);
                downstreamIndex = pair.first;

                Long t3 = System.currentTimeMillis();
                temporaryThroughput += pair.second;
                temporaryInputRate++;
                throughputCurrentTimestamp = System.currentTimeMillis();
                if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                    throughputPreviousTimestamp = throughputCurrentTimestamp;
//                    throughput.setValue(temporaryThroughput);
                    writer.writeData(throughputCurrentTimestamp + ",throughput," + Integer.toString(temporaryThroughput) + "\n");
//                    inputRate.setValue(temporaryInputRate);
                    writer.writeData(throughputCurrentTimestamp + ",input-rate," + Integer.toString(temporaryInputRate) + "\n");
                    temporaryInputRate = 0;
                    temporaryThroughput = 0;
                    lastStateSizeMetric = joiner.getStateSize();
                    writer.writeData(throughputCurrentTimestamp + ",state-size," + lastStateSizeMetric + "\n");
//                    stateSize.setValue(lastStateSizeMetric);
//                    executeLatency.setValue(Arrays.toString(times.toArray()));
                    writer.writeData(throughputCurrentTimestamp + ",execute-latency," + Arrays.toString(times.toArray()) + "\n");
//                    numberOfTuples.setValue(joiner.getNumberOfTuples());
                    writer.writeData(throughputCurrentTimestamp + ",number-of-tuples," + joiner.getNumberOfTuples() + "\n");
                }
                /**
                 * Check if SCALE-ACTION concluded (previous state expired)
                 */
                if (candidateTask != -1 && migratedKeys.size() == 0) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(SynefoConstant.COL_SCALE_ACTION_PREFIX + ":" + SynefoConstant.COL_COMPLETE_ACTION +
                            "|" + SynefoConstant.COL_KEYS + ":|" + SynefoConstant.COL_PEER + ":" + taskIdentifier);
                    Values scaleCompleteTuple = new Values();
                    scaleCompleteTuple.add(stringBuilder.toString());
                    scaleCompleteTuple.add("");
                    scaleCompleteTuple.add("");
                    collector.emitDirect(tuple.getSourceTask(), streamIdentifier + "-control", scaleCompleteTuple);
                    candidateTask = -1;
                    long currentTimestamp = System.currentTimeMillis();
//                    stateTransferTime.setValue((currentTimestamp - startTransferTimestamp));
                    if (scaleAction.equals(SynefoConstant.COL_REMOVE_ACTION))
                        initializeStats();
                    scaleAction = "";
                }
                Long t4 = System.currentTimeMillis();
                times = new ArrayList<>();
                times.add(new Long((t2 - t1)));
                times.add(new Long((t3 - t2)));
                times.add(new Long((t4 - t3)));
                if (temporaryInputRate == 0) {
//                    nonExecuteLatency.setValue(Arrays.toString(times.toArray()));
                }
            }
        }
    }

    private void initializeStats() {
//        inputRate.setValue(null);
//        stateSize.setValue(null);
//        throughput.setValue(null);
//        executeLatency.setValue(null);
//        stateTransferTime.setValue(null);
    }

    public void manageScaleTuple(Long timestamp, Tuple tuple, String header) {
        String action = header.split("[|]")[0].split(":")[1];
        String serializedMigratedKeys = header.split("[|]")[1].split(":")[1];
        String candidateTask = header.split("[|]")[2].split(":")[1];
        if (action.equals(SynefoConstant.COL_ADD_ACTION)) {
            if (Integer.parseInt(candidateTask) != taskIdentifier) {
                migratedKeys.clear();
                for (String key : serializedMigratedKeys.split(",")) {
                    migratedKeys.add(key);
                }
                this.candidateTask = Integer.parseInt(candidateTask);
                joiner.initializeScaleOut(timestamp, migratedKeys);
                startTransferTimestamp = System.currentTimeMillis();
                logger.info("JOIN-BOLT-" + taskName + ":" + taskIdentifier +
                        "participates into scale-action ADD and it is NOT the candidate");
                scaleAction = SynefoConstant.COL_ADD_ACTION;
                /**
                 * The next part is for experimentation purposes.
                 * It simulates the case where scaling happens instantly by dropping
                 * all the data (QoD = 0)
                 */
//                migratedKeys.clear();
//                joiner.initializeBuffer();
//                if (this.candidateTask != -1 && migratedKeys.size() == 0) {
//                    StringBuilder stringBuilder = new StringBuilder();
//                    stringBuilder.append(SynefoConstant.COL_SCALE_ACTION_PREFIX + ":" + SynefoConstant.COL_COMPLETE_ACTION +
//                            "|" + SynefoConstant.COL_KEYS + ":|" + SynefoConstant.COL_PEER + ":" + taskIdentifier);
//                    Values scaleCompleteTuple = new Values();
//                    scaleCompleteTuple.add(stringBuilder.toString());
//                    scaleCompleteTuple.add("");
//                    scaleCompleteTuple.add("");
//                    collector.emitDirect(tuple.getSourceTask(), streamIdentifier + "-control", scaleCompleteTuple);
//                    this.candidateTask = -1;
//                    long currentTimestamp = System.currentTimeMillis();
//                    stateTransferTime.setValue((currentTimestamp - startTransferTimestamp));
//                    if (scaleAction.equals(SynefoConstant.COL_REMOVE_ACTION))
//                        initializeStats();
//                    scaleAction = "";
//                }
                /**
                 * The next part is for experimentation purposes.
                 * It simulates the case where scaling happens normally, by waiting the
                 * data to expire (QoD = 1)
                 */
                long start = System.currentTimeMillis();
                joiner.initializeScaleOut(timestamp, migratedKeys);
                long end = System.currentTimeMillis();
                logger.info("separation of keys took: " + (end - start) / 1000L + " seconds");
            } else {
                logger.info("JOIN-BOLT-" + taskName + ":" + taskIdentifier +
                        " participates into scale-action ADD and it is the candidate.");
//                inputRate.setValue(null);
//                stateSize.setValue(null);
//                throughput.setValue(null);
//                executeLatency.setValue(null);
//                stateTransferTime.setValue(null);
            }
        } else if (action.equals(SynefoConstant.COL_REMOVE_ACTION)) {
            if (Integer.parseInt(candidateTask) == taskIdentifier) {
                migratedKeys.clear();
                for (String key : serializedMigratedKeys.split(",")) {
                    migratedKeys.add(key);
                }
                this.candidateTask = Integer.parseInt(candidateTask);
                joiner.initializeScaleIn(timestamp, migratedKeys);
                startTransferTimestamp = System.currentTimeMillis();
                logger.info("JOIN-BOLT-" + taskName + ":" + taskIdentifier +
                        "participates into scale-action REMOVE and it is the candidate");
                scaleAction = SynefoConstant.COL_REMOVE_ACTION;
            } else {
                logger.info("JOIN-BOLT-" + taskName + ":" + taskIdentifier +
                        "participates into scale-action REMOVE and it is NOT the candidate");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> schema = new ArrayList<String>();
        schema.add("SYNEFO_HEADER");
        schema.add("attributes");
        schema.add("values");
        outputFieldsDeclarer.declareStream(streamIdentifier + "-data", true, new Fields(schema));
        outputFieldsDeclarer.declareStream(streamIdentifier + "-control", true, new Fields(schema));
    }


}
