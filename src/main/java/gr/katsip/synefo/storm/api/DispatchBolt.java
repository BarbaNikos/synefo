package gr.katsip.synefo.storm.api;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.lib.SynefoMessage;
import gr.katsip.synefo.storm.operators.relational.elastic.NewJoinDispatcher;
import gr.katsip.synefo.utils.SynefoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by katsip on 9/14/2015.
 */
public class DispatchBolt extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(DispatchBolt.class);

    private static final int METRIC_REPORT_FREQ_SEC = 5;

    private static final int WARM_UP_THRESHOLD = 10000;

    private OutputCollector collector;

    private String taskName;

    private int workerPort;

    private String taskAddress;

    private String synefoAddress;

    private int synefoPort;

    private ZookeeperClient zookeeperClient;

    private List<String> downstreamTaskNames;

    private List<Integer> downstreamTaskIdentifiers;

    private List<String> activeDownstreamTaskNames;

    private List<Integer> activeDownstreamTaskIdentifiers;

    private NewJoinDispatcher dispatcher;

    private List<Values> state;

    private String zookeeperAddress;

    private HashMap<String, List<Integer>> relationTaskIndex;

    private boolean AUTO_SCALE_FLAG;

    private boolean SYSTEM_WARM_FLAG;

    private int taskIdentifier;

    private int tupleCounter;

    private transient AssignableMetric latency;

    private transient AssignableMetric throughput;

    private transient AssignableMetric executeLatency;

    private transient AssignableMetric stateSize;

    private transient AssignableMetric inputRate;

    private int temporaryInputRate;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    public DispatchBolt(String taskName, String synefoAddress, Integer synefoPort,
                        NewJoinDispatcher dispatcher, String zookeeperAddress, boolean autoScale) {
        this.taskName = taskName;
        this.workerPort = -1;
        this.synefoAddress = synefoAddress;
        this.synefoPort = synefoPort;
        downstreamTaskNames = null;
        downstreamTaskIdentifiers = null;
        activeDownstreamTaskNames = null;
        activeDownstreamTaskIdentifiers = null;
        this.dispatcher = dispatcher;
        state = new ArrayList<Values>();
        this.dispatcher.initializeState(state);
        this.zookeeperAddress = zookeeperAddress;
        AUTO_SCALE_FLAG = autoScale;
        SYSTEM_WARM_FLAG = false;
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
                downstreamTaskNames = new ArrayList<String>(downstream);
                downstreamTaskIdentifiers = new ArrayList<Integer>();
                Iterator<String> itr = downstreamTaskNames.iterator();
                while (itr.hasNext()) {
                    String[] tokens = itr.next().split("[:@]");
                    Integer task = Integer.parseInt(tokens[1]);
                    downstreamTaskIdentifiers.add(task);
                }
            }else {
                downstreamTaskIdentifiers = new ArrayList<Integer>();
            }
            ArrayList<String> activeDownstream = (ArrayList<String>) input.readObject();
            if (activeDownstream.size() > 0) {
                activeDownstreamTaskNames = new ArrayList<String>(activeDownstream);
                activeDownstreamTaskIdentifiers = new ArrayList<Integer>();
                Iterator<String> itr = activeDownstreamTaskNames.iterator();
                while (itr.hasNext()) {
                    String[] tokens = itr.next().split("[:@]");
                    Integer task = Integer.parseInt(tokens[1]);
                    activeDownstreamTaskIdentifiers.add(task);
                }
            }else {
                activeDownstreamTaskNames = new ArrayList<String>();
                activeDownstreamTaskIdentifiers = new ArrayList<Integer>();
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
                    if (activeDownstreamTaskIdentifiers.lastIndexOf(task) >= 0)
                        tasks.add(identifier);
                }
                activeRelationToTaskIndex.put(relation, identifiers);
                relationTaskIndex.put(relation, identifiers);
            }
            logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": task-to-relation index after processing: " +
                    relationTaskIndex);
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
        /**
         * Handshake with ZooKeeper
         */
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
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        taskIdentifier = topologyContext.getThisTaskId();
        workerPort = topologyContext.getThisWorkerPort();
        /**
         * Update the taskName and extend it with the task-id (support for multi-core)
         */
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
    }

    private void initMetrics(TopologyContext context) {
        latency = new AssignableMetric(null);
        throughput = new AssignableMetric(null);
        executeLatency = new AssignableMetric(null);
        stateSize = new AssignableMetric(null);
        inputRate = new AssignableMetric(null);
        context.registerMetric("latency", latency, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("execute-latency", executeLatency, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("throughput", throughput, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-size", stateSize, DispatchBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("input-rate", inputRate, DispatchBolt.METRIC_REPORT_FREQ_SEC);
    }

    private boolean isScaleHeader(String header) {
        return (header.contains(SynefoConstant.PUNCT_TUPLE_TAG) == true &&
                header.contains(SynefoConstant.ACTION_PREFIX) == true &&
                header.contains(SynefoConstant.COMP_IP_TAG) == true &&
                header.split("/")[0].equals(SynefoConstant.PUNCT_TUPLE_TAG));
    }

    @Override
    public void execute(Tuple tuple) {
        String header = "";
        if (!tuple.getFields().contains("SYNEFO_HEADER")) {
            logger.error("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier +
                    " missing synefo header (source: " +
                    tuple.getSourceTask() + ")");
            collector.fail(tuple);
            return;
        }else {
            header = tuple.getString(tuple.getFields()
                    .fieldIndex("SYNEFO_HEADER"));
            if (header != null && !header.equals("") && header.contains("/") &&
                    isScaleHeader(header)) {
                /**
                 * TODO: Revisit this when I ensure proper execution
                 */
                manageScaleCommand(tuple);
                collector.ack(tuple);
                return;
            }
        }
        throughputCurrentTimestamp = System.currentTimeMillis();
        if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
            throughputPreviousTimestamp = throughputCurrentTimestamp;
            inputRate.setValue(temporaryInputRate);
            /**
             * Set the data values on the zookeeper server
             */
            temporaryInputRate = 0;
        }else {
            temporaryInputRate++;
        }
        /**
         * Remove from both values and fields SYNEFO_HEADER (SYNEFO_TIMESTAMP)
         */
        Values values = new Values(tuple.getValues().toArray());
        values.remove(0);
        List<String> fieldList = tuple.getFields().toList();
        fieldList.remove(0);
        Fields fields = new Fields(fieldList);
        long startTime = System.currentTimeMillis();
        if (activeDownstreamTaskIdentifiers != null && activeDownstreamTaskIdentifiers.size() > 0) {
            dispatcher.execute(tuple, collector, fields, values);
            collector.ack(tuple);
        }else {
            dispatcher.execute(tuple, null, fields, values);
            collector.ack(tuple);
        }
        long endTime = System.currentTimeMillis();
        executeLatency.setValue((endTime - startTime));

        tupleCounter++;
        if (tupleCounter >= WARM_UP_THRESHOLD && !SYSTEM_WARM_FLAG)
            SYSTEM_WARM_FLAG = true;

        String command = "";
        if (!zookeeperClient.commands.isEmpty())
            command = zookeeperClient.commands.poll();
        manageCommand(command);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> producerSchema = new ArrayList<String>();
        producerSchema.add("SYNEFO_HEADER");
        producerSchema.addAll(dispatcher.getOutputSchema().toList());
        outputFieldsDeclarer.declare(new Fields(producerSchema));
    }

    public void manageCommand(String command) {
        String[] scaleCommandTokens = command.split("[~:@]");
        String action = scaleCommandTokens[0];
        String taskWithAddress = scaleCommandTokens[1] + ":" + scaleCommandTokens[2] + "@" + scaleCommandTokens[3];
        String taskAddress = scaleCommandTokens[3];
        String task = scaleCommandTokens[1];
        Integer taskIdentifier = Integer.parseInt(scaleCommandTokens[2]);
        StringBuilder strBuild = new StringBuilder();
        strBuild.append(SynefoConstant.PUNCT_TUPLE_TAG + "/");
        if (action.toLowerCase().contains("activate") || action.toLowerCase().contains("deactivate")) {

        }else {
            if (action.toLowerCase().contains("add")) {
                strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.ADD_ACTION + "/");
                activeDownstreamTaskNames.add(taskWithAddress);
                activeDownstreamTaskIdentifiers.add(taskIdentifier);
                /**
                 * TODO: Here I need to update the taskToRelationIndex accordingly
                 */
            }else if (action.toLowerCase().contains("remove")) {
                strBuild.append(SynefoConstant.ACTION_PREFIX + ":" + SynefoConstant.REMOVE_ACTION + "/");
                /**
                 * TODO: Here I need to update the taskToRelationIndex accordingly
                 */
            }
            strBuild.append(SynefoConstant.COMP_TAG + ":" + task + ":" + taskIdentifier + "/");
            strBuild.append(SynefoConstant.COMP_NUM_TAG + ":" + activeDownstreamTaskNames.size() + "/");
            strBuild.append(SynefoConstant.COMP_IP_TAG + ":" + taskIdentifier + "/");
            Values punctValue = new Values();
            punctValue.add(strBuild.toString());
            for(int i = 0; i < dispatcher.getOutputSchema().size(); i++) {
                punctValue.add(null);
            }
            for(Integer d_task : activeDownstreamTaskIdentifiers) {
                collector.emitDirect(d_task, punctValue);
            }
            if(action.toLowerCase().contains("remove") && activeDownstreamTaskNames.indexOf(taskWithAddress) >= 0) {
                activeDownstreamTaskNames.remove(activeDownstreamTaskNames.indexOf(taskWithAddress));
                activeDownstreamTaskIdentifiers.remove(activeDownstreamTaskIdentifiers.indexOf(taskWithAddress));
            }
        }
    }

    public void manageScaleCommand(Tuple tuple) {
        String[] tokens = ((String) tuple.getValues().get(0)).split("[/:]");
        String scaleAction = tokens[2];
        String taskName = tokens[4];
        String taskIdentifier = tokens[5];
        Integer taskNumber = Integer.parseInt(tokens[7]);
        String taskAddress = tokens[9];
        if (SYSTEM_WARM_FLAG)
            SYSTEM_WARM_FLAG = false;
        logger.info("DISPATCH-BOLT-" + taskName + ":" + taskIdentifier + ": received scale-command: " +
                scaleAction + "(" + System.currentTimeMillis() + ")");
        if (scaleAction != null && scaleAction.equals(SynefoConstant.ADD_ACTION)) {
            if ((this.taskName + ":" + this.taskIdentifier).equals(taskName + ":" + taskIdentifier)) {
                /**
                 * Case where this node is added. Nothing needs to be done for
                 * notifying the dispatchers.
                 */
                logger.info("");
                try {
                    ServerSocket socket = new ServerSocket(6000 + this.taskIdentifier);
                    int numberOfConnections = 0;
                    while (numberOfConnections < (taskNumber)) {
                        Socket client = socket.accept();
                        ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
                        ObjectInputStream input = new ObjectInputStream(client.getInputStream());
                        Object response = input.readObject();
                        if (state instanceof List) {
                            List<Values> receivedState = (List<Values>) response;
                            dispatcher.mergeState(receivedState);
                        }
                        output.flush();
                        input.close();
                        output.close();
                        client.close();
                        numberOfConnections++;
                    }
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                logger.info("");
            }else {
                Socket client = null;
                boolean ATTEMPT = true;
                while (ATTEMPT) {
                    try {
                        client = new Socket(taskAddress, 6000 + Integer.parseInt(taskIdentifier));
                        ATTEMPT = false;
                    } catch (IOException e) {
                        logger.error("");
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
                logger.info("");
                try {
                    ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
                    ObjectInputStream input = new ObjectInputStream(client.getInputStream());
                    output.writeObject(dispatcher.getState());
                    Object response = input.readObject();
                    if (response instanceof String) {
                        input.close();
                        output.close();
                        client.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                logger.info("");
            }
            activeDownstreamTaskIdentifiers = zookeeperClient.getActiveDownstreamTaskIdentifiers();
        }else if (scaleAction != null && scaleAction.equals(SynefoConstant.REMOVE_ACTION)) {
            if ((this.taskName + ":" + this.taskIdentifier).equals(taskName + ":" + taskIdentifier)) {
                try {
                    ServerSocket socket = new ServerSocket(6000 + this.taskIdentifier);
                    int numberOfConnections = 0;
                    while (numberOfConnections < (taskNumber)) {
                        Socket client = socket.accept();
                        ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
                        ObjectInputStream input = new ObjectInputStream(client.getInputStream());
                        output.writeObject(dispatcher.getState());
                        Object response = input.readObject();
                        input.close();
                        output.close();
                        client.close();
                        numberOfConnections++;
                    }
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                logger.info("");
            }else {
                Socket client = null;
                boolean ATTEMPT = true;
                while (ATTEMPT) {
                    try {
                        client = new Socket(taskAddress, 6000 + Integer.parseInt(taskIdentifier));
                        ATTEMPT = false;
                    } catch (IOException e) {
                        logger.error("");
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ei) {
                            ei.printStackTrace();
                        }
                    }
                }
                try {
                    ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
                    ObjectInputStream input = new ObjectInputStream(client.getInputStream());
                    Object response = input.readObject();
                    if (response instanceof List) {
                        List<Values> state = (List<Values>) response;
                        dispatcher.mergeState(state);
                    }
                    output.writeObject("OK");
                    input.close();
                    output.close();
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                logger.info("");
            }
            activeDownstreamTaskIdentifiers = zookeeperClient.getActiveDownstreamTaskIdentifiers();
        }
    }
}
