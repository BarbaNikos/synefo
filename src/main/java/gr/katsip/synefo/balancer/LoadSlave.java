package gr.katsip.synefo.balancer;

import gr.katsip.synefo.server2.JoinOperator;
import gr.katsip.synefo.storm.lib.SynefoMessage;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by katsip on 9/23/2015.
 */
public class LoadSlave implements Runnable {

    private InputStream in;

    private OutputStream out;

    private ObjectInputStream input;

    private ObjectOutputStream output;

    private ConcurrentHashMap<String, ArrayList<String>> topology;

    private ConcurrentHashMap<String, ArrayList<String>> activeTopology;

    private ConcurrentHashMap<String, Integer> taskIdentifierIndex;

    private ConcurrentHashMap<String, Integer> taskWorkerPortIndex;

    private Integer identifier;

    private String taskName;

    private String taskAddress;

    private Integer workerPort;

    private ConcurrentHashMap<String, String> taskAddressIndex;

    private AtomicInteger taskNumber = null;

    private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = null;

    public LoadSlave(ConcurrentHashMap<String, ArrayList<String>> topology,
                     ConcurrentHashMap<String, ArrayList<String>> activeTopology,
                     ConcurrentHashMap<String, Integer> taskIdentifierIndex,
                     ConcurrentHashMap<String, Integer> taskWorkerPortIndex,
                     InputStream in, OutputStream out,
                     ConcurrentHashMap<String, String> taskAddressIndex,
                     AtomicInteger taskNumber,
                     ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation) {
        this.in = in;
        this.out = out;
        this.taskIdentifierIndex = taskIdentifierIndex;
        this.taskAddressIndex = taskAddressIndex;
        this.taskWorkerPortIndex = taskWorkerPortIndex;
        try {
            output = new ObjectOutputStream(this.out);
            input = new ObjectInputStream(this.in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.topology = this.topology;
        this.activeTopology = activeTopology;
        this.taskNumber = taskNumber;
        this.taskToJoinRelation = taskToJoinRelation;
    }

    public void run() {
        SynefoMessage msg = null;
        try {
            msg = (SynefoMessage) input.readObject();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        if (msg != null) {
            String _componentType = ((SynefoMessage) msg)._values.get("TASK_TYPE");
            switch (_componentType) {
                case "SPOUT":
                    spout(msg._values);
                    break;
                case "JOIN_BOLT":
                    bolt(msg._values);
                    break;
                case "TOPOLOGY":
                    topology(msg._values);
                    break;
                default:
            }
        }
    }

    public void spout(HashMap<String, String> values) {
        identifier = Integer.parseInt(values.get("TASK_ID"));
        taskName = values.get("TASK_NAME");
        taskAddress = values.get("TASK_IP");
        workerPort = Integer.parseInt(values.get("WORKER_PORT"));
        taskAddressIndex.putIfAbsent(taskName + ":" + identifier, taskAddress);
        taskWorkerPortIndex.putIfAbsent(taskName + ":" + identifier, workerPort);
        taskIdentifierIndex.putIfAbsent(taskName, identifier);
        /**
         * Effective wait until the LoadMaster updates
         * the topology with the correct information
         */
        while (taskIdentifierIndex.size() > 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e2) {
                e2.printStackTrace();
            }
        }
        ArrayList<String> downstream = null;
        ArrayList<String> activeDownstream = null;
        if (topology.containsKey(taskName + ":" + identifier + "@" + taskAddress)) {
            downstream = new ArrayList<>(topology.get(taskName + ":" + identifier + "@" + taskAddress));
            if (activeTopology.containsKey(taskName + ":" + identifier + "@" + taskAddress)) {
                activeDownstream = new ArrayList<>(activeTopology.get(taskName + ":" + identifier + "@" + taskAddress));
            }else {
                activeDownstream = new ArrayList<>();
            }
        }else {
            downstream = new ArrayList<>();
            activeDownstream = new ArrayList<>();
        }
        try {
            output.writeObject(downstream);
            output.writeObject(activeDownstream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            output.flush();
            output.close();
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void bolt(HashMap<String, String> values) {
        String joinStep = values.get("JOIN_STEP");
        String relation = values.get("JOIN_RELATION");
        identifier = Integer.parseInt(values.get("TASK_ID"));
        JoinOperator operator = new JoinOperator(identifier,
                (joinStep.equals("DISPATCH") ? JoinOperator.Step.DISPATCH : JoinOperator.Step.JOIN),
                relation);
        this.taskToJoinRelation.putIfAbsent(identifier, operator);
        identifier = Integer.parseInt(values.get("TASK_ID"));
        taskName = values.get("TASK_NAME");
        taskAddress = values.get("TASK_IP");
        workerPort = Integer.parseInt(values.get("WORKER_PORT"));
        taskAddressIndex.putIfAbsent(taskName + ":" + identifier, taskAddress);
        taskWorkerPortIndex.putIfAbsent(taskName + ":" + identifier, workerPort);
        taskIdentifierIndex.putIfAbsent(taskName, identifier);
        while (taskIdentifierIndex.size() > 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e2) {
                e2.printStackTrace();
            }
        }
        HashMap<String, ArrayList<String>> relationTaskIndex = null;
        if (operator.getStep() == JoinOperator.Step.DISPATCH) {
            relationTaskIndex = new HashMap<String, ArrayList<String>>();
            for (String task : topology.get(taskName + ":" + identifier + "@" + taskAddress)) {
                Integer identifier = Integer.parseInt(task.split("[:@]")[1]);
                JoinOperator op = taskToJoinRelation.get(identifier);
                if (relationTaskIndex.containsKey(op.getRelation())) {
                    ArrayList<String> ops = relationTaskIndex.get(op.getRelation());
                    ops.add(task);
                    relationTaskIndex.put(op.getRelation(), ops);
                }else {
                    ArrayList<String> ops = new ArrayList<String>();
                    ops.add(task);
                    relationTaskIndex.put(op.getRelation(), ops);
                }
            }
        }
        ArrayList<String> downstream = null;
        ArrayList<String> activeDownstream = null;
        if (topology.containsKey(taskName + ":" + identifier + "@" + taskAddress)) {
            downstream = new ArrayList<>(topology.get(taskName + ":" + identifier + "@" + taskAddress));
            if (activeTopology.containsKey(taskName + ":" + identifier + "@" + taskAddress)) {
                activeDownstream = new ArrayList<>(activeTopology.get(taskName + ":" + identifier + "@" + taskAddress));
            }else {
                activeDownstream = new ArrayList<>();
            }
        }else {
            downstream = new ArrayList<>();
            activeDownstream = new ArrayList<>();
        }
        try {
            output.writeObject(downstream);
            output.writeObject(activeDownstream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (operator.getStep() == JoinOperator.Step.DISPATCH) {
            try {
                output.writeObject(relationTaskIndex);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            output.flush();
            output.close();
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void topology(HashMap<String, String> values) {
        /**
         * This is the total number of tasks (threads) that will be spawned in the
         * given topology. It is set atomically by the topology-process thread.
         */
        HashMap<String, ArrayList<String>> topology = null;
        try {
            topology = (HashMap<String, ArrayList<String>>) input.readObject();
        } catch (ClassNotFoundException | IOException e1) {
            e1.printStackTrace();
        }
        this.topology.clear();
        this.topology.putAll(topology);
        Integer providedTaskNumber = Integer.parseInt(values.get("TASK_NUM"));
        this.taskNumber.compareAndSet(-1, providedTaskNumber);
        String _ack = "+EFO_ACK";
        try {
            output.writeObject(_ack);
            output.flush();
            output.close();
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
