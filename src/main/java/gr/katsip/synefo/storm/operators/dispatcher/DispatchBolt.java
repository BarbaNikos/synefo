package gr.katsip.synefo.storm.operators.dispatcher;

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
import gr.katsip.synefo.storm.operators.ZookeeperClient;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.synefo.utils.SynefoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by Nick R. Katsipoulakis on 9/14/2015.
 */
public class DispatchBolt extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(DispatchBolt.class);

    private static final int METRIC_REPORT_FREQ_SEC = 1;

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

    private Dispatcher dispatcher;

    private HashMap<String, List<Integer>> relationTaskIndex;

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

    public DispatchBolt(String taskName, String synefoAddress, Integer synefoPort,
                        Dispatcher dispatcher, String zookeeperAddress) {
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
        relationTaskIndex = null;
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
        msg._values.put("JOIN_STEP", "DISPATCH");
        msg._values.put("JOIN_RELATION", "NA");
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
            /**
             * The following step is only for Dispatch-Bolts:
             * Isolating the downstream tasks and initializing the dispatcher object's
             * relationToTaskIndex structure.
             */
            HashMap<String, List<Integer>> activeRelationToTaskIndex = new HashMap<>();
            HashMap<String, ArrayList<String>> verbalRelationTaskIndex =
                    (HashMap<String, ArrayList<String>>) input.readObject();
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": retrieved task-to-relation index: " +
                    verbalRelationTaskIndex.toString());
            relationTaskIndex = new HashMap<String, List<Integer>>();
            Iterator<Map.Entry<String, ArrayList<String>>> itr = verbalRelationTaskIndex.entrySet()
                    .iterator();
            while (itr.hasNext()) {
                Map.Entry<String, ArrayList<String>> pair = itr.next();
                List<Integer> tasks = new ArrayList<Integer>();
                String relation = pair.getKey();
                ArrayList<Integer> identifiers = new ArrayList<Integer>();
                for (String task : pair.getValue()) {
                    Integer identifier = Integer.parseInt(task.split("[:@]")[1]);
                    identifiers.add(identifier);
                    if (activeDownstreamTaskIdentifiers.lastIndexOf(identifier) >= 0)
                        tasks.add(identifier);
                }
                activeRelationToTaskIndex.put(relation, tasks);
                relationTaskIndex.put(relation, identifiers);
            }
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": task-to-relation index after processing: " +
                    relationTaskIndex);
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": (active) task-to-relation index after processing: " +
                    activeRelationToTaskIndex);
            output.writeObject(new String("OK"));
            dispatcher.setTaskToRelationIndex(activeRelationToTaskIndex);
            output.close();
            input.close();
            socket.close();
        } catch (EOFException e) {
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": threw " + e.getMessage());
        } catch (IOException
                | ClassNotFoundException e) {
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": threw " + e.getMessage());
            e.printStackTrace();
        } catch (NullPointerException e) {
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": threw " + e.getMessage());
        }
        zookeeperClient.init();
        zookeeperClient.getScaleCommand();
        StringBuilder strBuild = new StringBuilder();
        strBuild.append("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": active tasks: ");
        for(String activeTask : activeDownstreamTaskNames) {
            strBuild.append(activeTask + " ");
        }
        logger.info(strBuild.toString());
        logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + " registered to load-balancer");
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
        controlTupleInterval = new AssignableMetric(null);
        context.registerMetric("execute-latency", executeLatency, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-size", stateSize, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("input-rate", inputRate, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("throughput", throughput, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-transfer", stateTransferTime, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("control-interval", controlTupleInterval, METRIC_REPORT_FREQ_SEC);
    }

    private boolean isScaleHeader(String header) {
        return (header.contains(SynefoConstant.PUNCT_TUPLE_TAG) == true &&
                header.contains(SynefoConstant.ACTION_PREFIX) == true &&
                header.contains(SynefoConstant.COMP_IP_TAG) == true &&
                header.split("/")[0].equals(SynefoConstant.PUNCT_TUPLE_TAG));
    }

    public static boolean isControlTuple(String header) {
        return (header.contains(SynefoConstant.COL_TICK_HEADER + ":"));
    }

    private boolean isStateHeader(String header) {
        return ( header.split("/")[0].equals(SynefoConstant.STATE_PREFIX) &&
                header.split("[/:]")[1].equals(SynefoConstant.COMP_TAG));
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
            logger.error("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier +
                    " missing synefo header (source: " +
                    tuple.getSourceTask() + ")");
            collector.fail(tuple);
            return;
        }else {
            header = tuple.getString(tuple.getFields()
                    .fieldIndex("SYNEFO_HEADER"));
            if (header != null && !header.equals("") && isControlTuple(header)) {
                long end = System.currentTimeMillis();
                long start = Long.parseLong(header.split(":")[1]);
                collector.ack(tuple);
                controlTupleInterval.setValue(tuple.getSourceTask() + "-" + (end - start));
                return;
            }
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
                    numberOfTuplesDispatched = dispatcher.execute(streamIdentifier + "-data", tuple, collector, fields, tupleValues);
                }else {
                    numberOfTuplesDispatched = dispatcher.execute(streamIdentifier + "-data", tuple, null, fields, tupleValues);
                }
                long endTime = System.currentTimeMillis();

                collector.ack(tuple);
                throughputCurrentTimestamp = System.currentTimeMillis();
                if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
                    throughputPreviousTimestamp = throughputCurrentTimestamp;
                    throughput.setValue(temporaryThroughput);
                    inputRate.setValue(temporaryInputRate);
                    executeLatency.setValue((endTime - startTime));
                    stateSize.setValue(dispatcher.getStateSize());
                    temporaryThroughput = 0;
                    temporaryInputRate = 0;
                } else {
                    temporaryThroughput += numberOfTuplesDispatched;
                    temporaryInputRate++;
                }
                tupleCounter++;
                String command = "";
                if (!zookeeperClient.commands.isEmpty() && SCALE_ACTION_FLAG == false) {
                    command = zookeeperClient.commands.poll();
                    manageCommand(command);
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
        outputFieldsDeclarer.declareStream(streamIdentifier + "-data", true, new Fields(schema));
        outputFieldsDeclarer.declareStream(streamIdentifier + "-control", true, new Fields(schema));
    }

    public void manageCommand(String command) {
        logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + " received scale-command \"" + command + "\"");
        String[] scaleCommandTokens = command.split("[~:@]");
        String action = scaleCommandTokens[0];
        String taskWithAddress = scaleCommandTokens[1] + ":" + scaleCommandTokens[2] + "@" + scaleCommandTokens[3];
        String taskAddress = scaleCommandTokens[3];
        String task = scaleCommandTokens[1];
        Integer taskIdentifier = Integer.parseInt(scaleCommandTokens[2]);
        StringBuilder strBuild = new StringBuilder();
        strBuild.append(SynefoConstant.PUNCT_TUPLE_TAG + "/");
        /**
         * Set a watch on the /synefo/state/task and set the SCALE_ACTION_FLAG to true
         */
        zookeeperClient.getJoinState(action.toLowerCase(), task + ":" + taskIdentifier);
        SCALE_ACTION_FLAG = true;
        scaleAction = action.toLowerCase();
        elasticTask = task + ":" + taskIdentifier;
        /**
         * Initialize the scale-tuple-buffer to buffer tuples until scale-action is
         * over
         */
        scaleTupleBuffer = new LinkedList<>();
        if (action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {
            activeDownstreamTaskNames = new ArrayList<>(zookeeperClient.getActiveDownstreamTasks());
            activeDownstreamTaskIdentifiers = new ArrayList<>(zookeeperClient.getActiveDownstreamTaskIdentifiers());
        }else {
            if (action.toLowerCase().contains("add")) {
                activeDownstreamTaskNames = new ArrayList<>(zookeeperClient.getActiveDownstreamTasks());
                activeDownstreamTaskIdentifiers = new ArrayList<>(zookeeperClient.getActiveDownstreamTaskIdentifiers());
                strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION + "/");
            }else if (action.toLowerCase().contains("remove")) {
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
            for(int i = 0; i < dispatcher.getOutputSchema().size(); i++)
                punctValue.add(null);
            for(Integer d_task : activeDownstreamTaskIdentifiers)
                collector.emitDirect(d_task, punctValue);
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

    public void manageStateTuple(Tuple tuple) {
        if (SCALE_RECEIVE_STATE) {
            if (stateTaskIdentifier == taskIdentifier) {
                //Case where state is received by a newly-added Dispatcher
                List<Values> statePacket = (List<Values>) tuple.getValue(1);
                dispatcher.mergeState(statePacket);
                numberOfConnections++;
                if (numberOfConnections >= stateTaskNumber) {
                    logger.info(taskName + ":" + taskIdentifier + " completed the reception of state from (" + stateTaskNumber + ") tasks.");
                    activeDownstreamTaskIdentifiers = zookeeperClient.getActiveDownstreamTaskIdentifiers();
                    zookeeperClient.notifyActionComplete();
                    SCALE_RECEIVE_STATE = false;
                    numberOfConnections = -1;
                    stateTaskNumber = -1;
                    stateTaskIdentifier = -1;
                    long currentTimestamp = System.currentTimeMillis();
                    stateTransferTime.setValue((currentTimestamp - startTransferTimestamp));
                }
            }else {
                //Case where state is received by a remaining-node
                logger.info(taskName + ":" + taskIdentifier + " completed the reception of state from removed-task (" + tuple.getSourceTask() + ").");
                List<Values> state = (List<Values>) tuple.getValue(1);
                dispatcher.mergeState(state);
                SCALE_RECEIVE_STATE = false;
                numberOfConnections = -1;
                stateTaskNumber = -1;
                stateTaskIdentifier = -1;
            }
        }else if (SCALE_SEND_STATE) {
            //Case where state is send by a about-to-be-removed Dispatcher
            numberOfConnections++;
            List<Values> statePacket = dispatcher.getState();
            String stateHeader = SynefoConstant.STATE_PREFIX + "/" + SynefoConstant.COMP_TAG + ":" + taskIdentifier + "/";
            Values stateTuple = new Values();
            stateTuple.add(stateHeader);
            stateTuple.add(statePacket);
            Integer identifier = Integer.parseInt(((String) tuple.getValue(0)).split("[:/]")[2]);
            for (int i = 0; i < (dispatcher.getOutputSchema().size() - 1); i++) {
                stateTuple.add(null);
            }
            collector.emitDirect(identifier, stateTuple);
            if (numberOfConnections >= stateTaskNumber) {
                logger.info(taskName + ":" + taskIdentifier + " completed the transmission of state to (" + stateTaskNumber + ") tasks.");
                activeDownstreamTaskIdentifiers = zookeeperClient.getActiveDownstreamTaskIdentifiers();
                zookeeperClient.notifyActionComplete();
                SCALE_SEND_STATE = false;
                numberOfConnections = -1;
                stateTaskNumber = -1;
                stateTaskIdentifier = -1;
                long currentTimestamp = System.currentTimeMillis();
                stateTransferTime.setValue((currentTimestamp - startTransferTimestamp));
            }
        }
    }

    public void altManageScaleTuple(Tuple tuple) {
        String[] tokens = ((String) tuple.getValues().get(0)).split("[/:]");
        String scaleAction = tokens[2];
        String taskName = tokens[4];
        stateTaskIdentifier = Integer.parseInt(tokens[5]);
        stateTaskNumber = Integer.parseInt(tokens[7]);
        logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": received scale-command: " +
                scaleAction + "(" + System.currentTimeMillis() + ")");
        if (scaleAction.equals(SynefoConstant.ADD_ACTION)) {
            if ((this.taskName + ":" + this.taskIdentifier).equals(taskName + ":" + taskIdentifier)) {
                numberOfConnections = 0;
                SCALE_RECEIVE_STATE = true;
                this.startTransferTimestamp = System.currentTimeMillis();
            }else {
                List<Values> statePacket = dispatcher.getState();
                String stateHeader = SynefoConstant.STATE_PREFIX + "/" + SynefoConstant.COMP_TAG + ":" + taskIdentifier + "/";
                Values stateTuple = new Values();
                stateTuple.add(stateHeader);
                stateTuple.add(statePacket);
                for (int i = 0; i < (dispatcher.getOutputSchema().size() - 1); i++) {
                    stateTuple.add(null);
                }
                collector.emitDirect(stateTaskIdentifier, stateTuple);
                activeDownstreamTaskIdentifiers = zookeeperClient.getActiveDownstreamTaskIdentifiers();
                numberOfConnections = -1;
                stateTaskNumber = -1;
                stateTaskIdentifier = -1;
            }
        }else if (scaleAction.equals(SynefoConstant.REMOVE_ACTION)) {
            if ((this.taskName + ":" + this.taskIdentifier).equals(taskName + ":" + taskIdentifier)) {
                numberOfConnections = 0;
                SCALE_SEND_STATE = true;
                this.startTransferTimestamp = System.currentTimeMillis();
            }else {
                SCALE_RECEIVE_STATE = true;
                String stateHeader = SynefoConstant.STATE_PREFIX + "/" + SynefoConstant.COMP_TAG + ":" + taskIdentifier + "/";
                Values stateTuple = new Values();
                stateTuple.add(stateHeader);
                for (int i = 0; i < dispatcher.getOutputSchema().size(); i++) {
                    stateTuple.add(null);
                }
                collector.emitDirect(stateTaskIdentifier, stateTuple);
            }
        }
    }

}
