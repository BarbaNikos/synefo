package gr.katsip.synefo.storm.operators.joiner;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.ZookeeperClient;
import gr.katsip.synefo.utils.Pair;
import gr.katsip.synefo.utils.Util;
import gr.katsip.synefo.utils.SynefoMessage;
import gr.katsip.synefo.utils.SynefoConstant;
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
 * Created by katsip on 9/21/2015.
 */
public class JoinBolt extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(JoinBolt.class);

    private static final int METRIC_REPORT_FREQ_SEC = 1;

//    private static final int WARM_UP_THRESHOLD = 10000;

//    private boolean SYSTEM_WARM_FLAG;

    private OutputCollector collector;

    private String taskName;

    private String streamIdentifier;

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

    private NewJoinJoiner joiner;

    private int tupleCounter;

    private transient AssignableMetric throughput;

    private transient AssignableMetric executeLatency;

    private transient AssignableMetric stateSize;

    private transient AssignableMetric inputRate;

    private transient AssignableMetric stateTransferTime;

    private long startTransferTimestamp;

    private int temporaryInputRate;

    private int temporaryThroughput;

    private long inputRateCurrentTimestamp;

    private long inputRatePreviousTimestamp;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    private long lastExecuteLatencyMetric = 0L;

    private long lastStateSizeMetric = 0L;

    private int numberOfConnections;

    private int stateTaskNumber;

    private int stateTaskIdentifier;

    private boolean SCALE_RECEIVE_STATE;

    private boolean SCALE_SEND_STATE;

    private HashMap<String, ArrayList<Values>> state = null;

    private List<String> keys = null;

    public JoinBolt(String taskName, String synefoAddress, Integer synefoPort,
                    NewJoinJoiner joiner, String zookeeperAddress) {
        this.taskName = taskName;
        this.workerPort = -1;
        this.synefoAddress = synefoAddress;
        this.synefoPort = synefoPort;
        downstreamTaskNames = null;
        downstreamTaskIdentifiers = null;
        activeDownstreamTaskNames = null;
        activeDownstreamTaskIdentifiers = null;
        this.joiner = joiner;
        this.zookeeperAddress = zookeeperAddress;
//        SYSTEM_WARM_FLAG = false;
        tupleCounter = 0;
    }

    public void register() {
        Socket socket;
        ObjectOutputStream output;
        ObjectInputStream input;
        SynefoMessage message = new SynefoMessage();
        message._type = SynefoMessage.Type.REG;
        message._values.put("TASK_IP", taskAddress);
        message._values.put("TASK_TYPE", "JOIN_BOLT");
        message._values.put("JOIN_STEP", joiner.operatorStep());
        message._values.put("JOIN_RELATION", joiner.relationStorage());
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
            if(downstream.size() > 0) {
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
        strBuild.append("JOIN-BOLT-" + taskName + ":" + taskIdentifier + ": active tasks: ");
        for(String activeTask : activeDownstreamTaskNames) {
            strBuild.append(activeTask + " ");
        }
        logger.info(strBuild.toString());
        logger.info("JOIN-BOLT-" + taskName + ":" + taskIdentifier + " registered to load-balancer");
        downstreamIndex = 0;
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
        } catch(UnknownHostException e) {
            e.printStackTrace();
        }
        zookeeperClient = new ZookeeperClient(zookeeperAddress, taskName, taskIdentifier, taskAddress);
        if(downstreamTaskNames == null && activeDownstreamTaskNames == null)
            register();
        initMetrics(topologyContext);
//        SYSTEM_WARM_FLAG = false;
        tupleCounter = 0;
        SCALE_RECEIVE_STATE = false;
        SCALE_SEND_STATE = false;
    }

    private void initMetrics(TopologyContext context) {
        executeLatency = new AssignableMetric(null);
        stateSize = new AssignableMetric(null);
        inputRate = new AssignableMetric(null);
        throughput = new AssignableMetric(null);
        stateTransferTime = new AssignableMetric(null);
        context.registerMetric("execute-latency", executeLatency, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-size", stateSize, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("input-rate", inputRate, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("throughput", throughput, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-transfer", stateTransferTime, JoinBolt.METRIC_REPORT_FREQ_SEC);
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
            logger.error("JOIN-BOLT-" + taskName + ":" + taskIdentifier +
                    " missing synefo header (source: " +
                    tuple.getSourceTask() + ")");
            collector.fail(tuple);
            return;
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
        }
        inputRateCurrentTimestamp = System.currentTimeMillis();
        if ((inputRateCurrentTimestamp - inputRatePreviousTimestamp) >= 1000L) {
            inputRatePreviousTimestamp = inputRateCurrentTimestamp;
            inputRate.setValue(temporaryInputRate);
            executeLatency.setValue(lastExecuteLatencyMetric);
            stateSize.setValue(lastStateSizeMetric);
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
        long startTime = System.currentTimeMillis();
        Pair<Integer, Integer> pair = joiner.execute(tuple, collector, activeDownstreamTaskIdentifiers,
                downstreamIndex, fields, tupleValues);
        downstreamIndex = pair.first;
        temporaryThroughput += pair.second;
        collector.ack(tuple);
        long endTime = System.currentTimeMillis();
        lastExecuteLatencyMetric = endTime - startTime;
        lastStateSizeMetric = joiner.getStateSize();
        executeLatency.setValue(lastExecuteLatencyMetric);
        stateSize.setValue(lastStateSizeMetric);
        throughputCurrentTimestamp = System.currentTimeMillis();
        if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
            throughputPreviousTimestamp = throughputCurrentTimestamp;
            throughput.setValue(temporaryThroughput);
            //TODO: Change the following
//            zookeeperClient.addInputRateData((double) temporaryInputRate);
            temporaryThroughput = 0;
        }
        tupleCounter++;
//        if (tupleCounter >= WARM_UP_THRESHOLD && !SYSTEM_WARM_FLAG)
//            SYSTEM_WARM_FLAG = true;

        String command = "";
        if (!zookeeperClient.commands.isEmpty()) {
            command = zookeeperClient.commands.poll();
            manageCommand(command);
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

    public void manageCommand(String command) {
        //TODO: Not supported yet!
    }

    public void manageStateTuple(Tuple tuple) {
        if (SCALE_RECEIVE_STATE) {
            if (stateTaskIdentifier == taskIdentifier) {
                //Case where state is received by a newly-added Joiner
                HashMap<String, ArrayList<Values>> statePacket =
                        (HashMap<String, ArrayList<Values>>) tuple.getValue(1);
                Util.mergeState(state, statePacket);
                numberOfConnections++;
                if (numberOfConnections >= stateTaskNumber) {
                    logger.info(taskName + ":" + taskIdentifier + " completed the reception of state from (" + stateTaskNumber + ") tasks.");
                    keys = joiner.setState(state);
                    zookeeperClient.setJoinState(taskName, taskIdentifier, keys);
                    keys.clear();
                    state.clear();
                    SCALE_RECEIVE_STATE = false;
                    numberOfConnections = -1;
                    stateTaskNumber = -1;
                    stateTaskIdentifier = -1;
                    long currentTimestamp = System.currentTimeMillis();
                    stateTransferTime.setValue((currentTimestamp - startTransferTimestamp));
                }
            }else {
                //Case where state is received by a remaining-task (Joiner)
                logger.info(taskName + ":" + taskIdentifier + " completed the reception of state from removed-task (" + tuple.getSourceTask() + ").");
                HashMap<String, ArrayList<Values>> statePacket =
                        (HashMap<String, ArrayList<Values>>) tuple.getValue(1);
                joiner.addToState(statePacket);
                SCALE_RECEIVE_STATE = false;
                numberOfConnections = -1;
                stateTaskNumber = -1;
                stateTaskIdentifier = -1;
            }
        }else if (SCALE_SEND_STATE) {
            //Case where state is send by a about-to-be-removed Joiner
            numberOfConnections++;
            HashMap<String, ArrayList<Values>> statePacket = joiner.getStateToBeSent();
            String stateHeader = SynefoConstant.STATE_PREFIX + "/" + SynefoConstant.COMP_TAG + ":" +
                    taskIdentifier + "/";
            Integer identifier = Integer.parseInt(((String) tuple.getValue(0)).split("[:/]")[2]);
            Values stateTuple = new Values();
            stateTuple.add(stateHeader);
            stateTuple.add(statePacket);
            for (int i = 0; i < (joiner.getOutputSchema().size() - 1); i++)
                stateTuple.add(null);
            Iterator<Map.Entry<String, ArrayList<Values>>> iterator = statePacket.entrySet().iterator();
            StringBuilder stringBuilder = new StringBuilder();
            while (iterator.hasNext()) {
                stringBuilder.append(iterator.next().getKey() + ",");
            }
            if (stringBuilder.length() > 0 && stringBuilder.charAt(stringBuilder.length() - 1) == ',') {
                stringBuilder.setLength(stringBuilder.length() - 1);
            }
            collector.emitDirect(identifier, stateTuple);
            keys.add(tuple.getSourceTask() + "=" + stringBuilder.toString());
            if (numberOfConnections >= stateTaskNumber) {
                logger.info(taskName + ":" + taskIdentifier + " completed the transmission of state to (" + stateTaskNumber + ") tasks.");
                zookeeperClient.setJoinState(taskName, taskIdentifier, keys);
                keys.clear();
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
        logger.info("JOIN-BOLT-" + taskName + ":" + taskIdentifier + ": received scale-command: " +
                scaleAction + "(" + System.currentTimeMillis() + ")");
        if (scaleAction.equals(SynefoConstant.ADD_ACTION)) {
            if ((this.taskName + ":" + this.taskIdentifier).equals(taskName + ":" + taskIdentifier)) {
                numberOfConnections = 0;
                SCALE_RECEIVE_STATE = true;
                state = new HashMap<>();
                startTransferTimestamp = System.currentTimeMillis();
            }else {
                HashMap<String, ArrayList<Values>> statePacket = joiner.getStateToBeSent();
                String stateHeader = SynefoConstant.STATE_PREFIX + "/" + SynefoConstant.COMP_TAG + ":" +
                        taskIdentifier + "/";
                Values stateTuple = new Values();
                stateTuple.add(stateHeader);
                stateTuple.add(statePacket);
                for (int i = 0; i < (joiner.getOutputSchema().size() - 1); i++)
                    stateTuple.add(null);
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
                keys = new ArrayList<>();
                startTransferTimestamp = System.currentTimeMillis();
            }else {
                SCALE_RECEIVE_STATE = true;
                String stateHeader = SynefoConstant.STATE_PREFIX + "/" + SynefoConstant.COMP_TAG + ":" +
                        taskIdentifier + "/";
                Values stateTuple = new Values();
                stateTuple.add(stateHeader);
                for (int i = 0; i < (joiner.getOutputSchema().size()); i++)
                    stateTuple.add(null);
                collector.emitDirect(stateTaskIdentifier, stateTuple);
            }
        }
    }

}