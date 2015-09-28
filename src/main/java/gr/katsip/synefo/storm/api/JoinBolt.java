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
import gr.katsip.synefo.storm.operators.relational.elastic.NewJoinJoiner;
import gr.katsip.synefo.utils.SynefoConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by katsip on 9/21/2015.
 */
public class JoinBolt extends BaseRichBolt {

    Logger logger = LoggerFactory.getLogger(JoinBolt.class);

    private static final int METRIC_REPORT_FREQ_SEC = 5;

    private static final int WARM_UP_THRESHOLD = 10000;

    private String taskName;

    private String synefoAddress;

    private Integer synefoPort;

    private NewJoinJoiner joiner;

    private String zookeeperAddress;

    private boolean AUTO_SCALE;

    private String taskAddress;

    private OutputCollector outputCollector;

    private Integer taskIdentifier;

    private Integer workerPort;

    private ZookeeperClient zookeeperClient;

    private List<String> downstreamTaskNames;

    private List<Integer> downstreamTaskIdentifiers;

    private List<String> activeDownstreamTaskNames;

    private List<Integer> activeDownstreamTaskIdentifiers;

    private int tupleCounter;

    private boolean SYSTEM_WARM_FLAG;

    private Integer downstreamIndex;

    private transient AssignableMetric latency;

    private transient AssignableMetric throughput;

    private transient AssignableMetric executeLatency;

    private transient AssignableMetric stateSize;

    private transient AssignableMetric inputRate;

    private int temporaryInputRate;

    private long throughputCurrentTimestamp;

    private long throughputPreviousTimestamp;

    public JoinBolt(String taskName, String synefoAddress, Integer synefoPort,
                    NewJoinJoiner joiner, String zookeeperAddress, boolean AUTO_SCALE) {
        this.taskName = taskName;
        this.synefoAddress = synefoAddress;
        this.synefoPort = synefoPort;
        this.joiner = joiner;
        this.zookeeperAddress = zookeeperAddress;
        this.AUTO_SCALE = AUTO_SCALE;
        downstreamTaskNames = null;
        activeDownstreamTaskNames = null;
        tupleCounter = 0;
        SYSTEM_WARM_FLAG = false;
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
            message = null;
            ArrayList<String> downstream = null;
            try {
                downstream = (ArrayList<String>) input.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            if(downstream != null && downstream.size() > 0) {
                downstreamTaskNames = new ArrayList<String>(downstream);
                downstreamTaskIdentifiers = new ArrayList<Integer>();
                Iterator<String> itr = downstreamTaskNames.iterator();
                while(itr.hasNext()) {
                    String[] tokens = itr.next().split("[:@]");
                    Integer task = Integer.parseInt(tokens[1]);
                    downstreamTaskIdentifiers.add(task);
                }
            }else {
                downstreamTaskNames = new ArrayList<String>();
                downstreamTaskIdentifiers = new ArrayList<Integer>();
            }
            ArrayList<String> activeDownstream = null;
            try {
                activeDownstream = (ArrayList<String>) input.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            if(activeDownstream != null && activeDownstream.size() > 0) {
                activeDownstreamTaskNames = new ArrayList<String>(activeDownstream);
                activeDownstreamTaskIdentifiers = new ArrayList<Integer>();
                Iterator<String> itr = activeDownstreamTaskNames.iterator();
                while(itr.hasNext()) {
                    String[] tokens = itr.next().split("[:@]");
                    Integer task = Integer.parseInt(tokens[1]);
                    activeDownstreamTaskIdentifiers.add(task);
                }
            }else {
                activeDownstreamTaskNames = new ArrayList<String>();
                activeDownstreamTaskIdentifiers = new ArrayList<Integer>();
            }
            output.close();
            input.close();
            socket.close();
        } catch (IOException e) {
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
        downstreamIndex = new Integer(0);
        throughputPreviousTimestamp = System.currentTimeMillis();
        temporaryInputRate = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
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
        SYSTEM_WARM_FLAG = false;
        tupleCounter = 0;
    }

    private void initMetrics(TopologyContext context) {
        latency = new AssignableMetric(null);
        throughput = new AssignableMetric(null);
        executeLatency = new AssignableMetric(null);
        stateSize = new AssignableMetric(null);
        inputRate = new AssignableMetric(null);
        context.registerMetric("latency", latency, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("execute-latency", executeLatency, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("throughput", throughput, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("state-size", stateSize, JoinBolt.METRIC_REPORT_FREQ_SEC);
        context.registerMetric("input-rate", inputRate, JoinBolt.METRIC_REPORT_FREQ_SEC);
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
            logger.error("JOIN-BOLT-" + taskName + ":" + taskIdentifier +
                    " missing synefo header (source: " +
                    tuple.getSourceTask() + ")");
            outputCollector.fail(tuple);
            return;
        }else {
            header = tuple.getString(tuple.getFields().fieldIndex("SYNEFO_HEADER"));
            if (header != null && !header.equals("") && header.contains("/") && isScaleHeader(header)) {
                /**
                 * TODO: Finish up the handle punctuation tuple
                 */
                outputCollector.ack(tuple);
                return;
            }
        }
        throughputCurrentTimestamp = System.currentTimeMillis();
        if ((throughputCurrentTimestamp - throughputPreviousTimestamp) >= 1000L) {
            throughputPreviousTimestamp = throughputCurrentTimestamp;
            inputRate.setValue(temporaryInputRate);
            temporaryInputRate = 0;
        }else {
            temporaryInputRate++;
        }
        Values values = new Values(tuple.getValues().toArray());
        values.remove(0);
        List<String> fieldList = tuple.getFields().toList();
        fieldList.remove(0);
        Fields fields = new Fields(fieldList);
        long startTime = System.currentTimeMillis();
        if (activeDownstreamTaskIdentifiers.size() > 0) {
            joiner.execute(tuple, outputCollector, activeDownstreamTaskIdentifiers,
                    downstreamIndex, fields, values, null);
            outputCollector.ack(tuple);
        }else {
            joiner.execute(tuple, outputCollector, activeDownstreamTaskIdentifiers,
                    downstreamIndex, fields, values, null);
            outputCollector.ack(tuple);
        }
        long endTime = System.currentTimeMillis();

        tupleCounter++;
        if (tupleCounter >= WARM_UP_THRESHOLD && !SYSTEM_WARM_FLAG)
            SYSTEM_WARM_FLAG = true;
        String command = "";
        if (!zookeeperClient.commands.isEmpty()) {
            command = zookeeperClient.commands.poll();
            //TODO: Populate the following
//            manageCommand(command);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> producerSchema = new ArrayList<String>();
        producerSchema.add("SYNEFO_HEADER");
        producerSchema.addAll(joiner.getOutputSchema().toList());
        outputFieldsDeclarer.declare(new Fields(producerSchema));
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
        if (scaleAction != null && scaleAction.equals(SynefoConstant.ADD_ACTION)) {
            if ((this.taskName + ":" + this.taskIdentifier).equals(taskName + ":" + taskIdentifier)) {
                try {
                    /**
                     * First clear-out the contents of
                     */
                    ServerSocket socket = new ServerSocket(6000 + this.taskIdentifier);
                    int numberOfConnections = 0;
                    while (numberOfConnections < (taskNumber)) {
                        Socket client = socket.accept();
                        ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
                        ObjectInputStream input = new ObjectInputStream(client.getInputStream());
                        Object response = input.readObject();
                        if (response instanceof List) {
                            List<Values> receivedState = (List<Values>) response;
//                            dispatcher.mergeState(receivedState);
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
            }else {
                /**
                 * Other node is added. Required Actions
                 * 1) Get a list of the keys from the state that is going to be sent
                 */
                HashMap<String, ArrayList<Values>> state = joiner.getStateToBeSent();
                Iterator<Map.Entry<String, ArrayList<Values>>> iterator = state.entrySet().iterator();
                List<String> keys = new ArrayList<String>();
                while (iterator.hasNext()) {
                    keys.add(iterator.next().getKey());
                }
                Socket client = null;
                boolean ATTEMPT = true;
                while (ATTEMPT) {
                    try {
                        client = new Socket(taskAddress, 6000 + Integer.parseInt(taskIdentifier));
                        ATTEMPT = false;
                    } catch (IOException e) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
                /**
                 * 2) Create a z-node under /synefo/join-state/taskName:taskIdentifier/x where x is a sequence number
                 *    and set the data to as a comma-separated list of the keys
                 */
//                zookeeperClient.createChildJoinStateDataScaleOut(taskName, taskNumber, keys);
                try {
                    ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
                    ObjectInputStream input = new ObjectInputStream(client.getInputStream());
                    output.writeObject(state);
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
            }
//            List<Integer> activeDownstreamTasks =
//                    zookeeperClient.getActiveTopology(this.taskName, this.taskIdentifier, this.taskAddress);
//            this.activeDownstreamTaskIdentifiers = new ArrayList<Integer>(activeDownstreamTasks);
        }else if (scaleAction != null && scaleAction.equals(SynefoConstant.REMOVE_ACTION)) {
            if ((this.taskName + ":" + this.taskIdentifier).equals(taskName + ":" + taskIdentifier)) {
                try {
                    ServerSocket socket = new ServerSocket(6000 + this.taskIdentifier);
                    int numberOfConnections = 0;
                    while (numberOfConnections < (taskNumber)) {
                        Socket client = socket.accept();
                        ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
                        ObjectInputStream input = new ObjectInputStream(client.getInputStream());
//                        output.writeObject(dispatcher.getState());
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
            }else {
                Socket client = null;
                boolean ATTEMPT = true;
                while (ATTEMPT) {
                    try {
                        client = new Socket(taskAddress, 6000 + Integer.parseInt(taskIdentifier));
                        ATTEMPT = false;
                    } catch (IOException e) {
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
//                        dispatcher.mergeState(state);
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
            }
//            List<Integer> activeDownstreamTasks =
//                    zookeeperClient.getActiveTopology(this.taskName, this.taskIdentifier, this.taskAddress);
//            this.activeDownstreamTaskIdentifiers = new ArrayList<Integer>(activeDownstreamTasks);
        }
    }
}
