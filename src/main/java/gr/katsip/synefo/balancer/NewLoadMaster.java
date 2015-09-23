package gr.katsip.synefo.balancer;

import gr.katsip.synefo.server2.JoinOperator;
import gr.katsip.synefo.server2.ScaleFunction;
import gr.katsip.synefo.storm.api.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by nick on 9/22/15.
 */
public class NewLoadMaster implements Runnable {

    private ConcurrentHashMap<String, ArrayList<String>> topology;

    private ConcurrentHashMap<String, ArrayList<String>> activeTopology;

    private ConcurrentHashMap<String, Integer> taskIdentifierIndex;

    private ConcurrentHashMap<String, String> taskAddressIndex;

    private LoadBalancer balancer;

    private HashMap<String, Pair<Number, Number>> resourceThresholds;

    private String zookeeperAddress;

    private AtomicBoolean operationFlag;

    private AtomicInteger taskNumber = null;

    private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = null;

    public NewLoadMaster(String zookeeperAddress,
                         HashMap<String, Pair<Number, Number>> resourceThresholds,
                         ConcurrentHashMap<String, ArrayList<String>> topology,
                         ConcurrentHashMap<String, ArrayList<String>> activeTopology,
                         ConcurrentHashMap<String, Integer> taskIdentifierIndex,
                         ConcurrentHashMap<String, String> taskAddressIndex,
                         AtomicBoolean operationFlag,
                         AtomicInteger taskNumber,
                         ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation) {
        this.topology = topology;
        this.activeTopology = activeTopology;
        this.taskIdentifierIndex = taskIdentifierIndex;
        this.taskAddressIndex = taskAddressIndex;
        this.zookeeperAddress = zookeeperAddress;
        this.operationFlag = operationFlag;
        this.taskNumber = taskNumber;
        this.taskToJoinRelation = taskToJoinRelation;
    }


    @Override
    public void run() {
        while (this.taskNumber.get() == -1) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (taskIdentifierIndex.size() < taskNumber.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        balancer = new LoadBalancer(zookeeperAddress, taskToJoinRelation, resourceThresholds);
        balancer.start();
        ConcurrentHashMap<String, ArrayList<String>> expandedTopology = topologyTaskExpand(taskIdentifierIndex,
                topology);
        ConcurrentHashMap<String, ArrayList<String>> finalTopology = updateTopology(taskAddressIndex, taskIdentifierIndex,
                topology);
        topology.clear();
        topology.putAll(finalTopology);
        activeTopology.clear();
        activeTopology.putAll(getInitialActiveTopologyWithJoinOperators(topology,
                getInverseTopology(topology), taskToJoinRelation, true));
        operationFlag.set(true);
        taskIdentifierIndex.clear();
        balancer.setTopology(topology);
        balancer.setTopology(activeTopology);
        while (operationFlag.get()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static ConcurrentHashMap<String, ArrayList<String>> topologyTaskExpand(ConcurrentHashMap<String, Integer> taskIdentifierIndex,
                                                                                  ConcurrentHashMap<String, ArrayList<String>> topology) {
        ConcurrentHashMap<String, ArrayList<String>> physicalTopologyWithIds = new ConcurrentHashMap<String, ArrayList<String>>();
        Iterator<Map.Entry<String, Integer>> taskNameIterator = taskIdentifierIndex.entrySet().iterator();
        while(taskNameIterator.hasNext()) {
            Map.Entry<String, Integer> pair = taskNameIterator.next();
            String taskName = pair.getKey();
            String taskNameWithoutId = taskName.split("_")[0];
            ArrayList<String> downstreamTaskList = topology.get(taskNameWithoutId);
            if (downstreamTaskList != null && downstreamTaskList.size() > 0) {
                ArrayList<String> downstreamTaskWithIdList = new ArrayList<String>();
                for(String downstreamTask : downstreamTaskList) {
                    Iterator<Map.Entry<String, Integer>> downstreamTaskIterator = taskIdentifierIndex.entrySet().iterator();
                    while(downstreamTaskIterator.hasNext()) {
                        Map.Entry<String, Integer> downstreamPair = downstreamTaskIterator.next();
                        if(downstreamPair.getKey().split("_")[0].equals(downstreamTask)) {
                            downstreamTaskWithIdList.add(downstreamPair.getKey());
                        }
                    }
                }
                physicalTopologyWithIds.put(taskName, downstreamTaskWithIdList);
            }
        }
        return physicalTopologyWithIds;
    }

    public static ConcurrentHashMap<String, ArrayList<String>> updateTopology(ConcurrentHashMap<String, String> taskAddressIndex,
                                                                              ConcurrentHashMap<String, Integer> taskIdentifierIndex,
                                                                              ConcurrentHashMap<String, ArrayList<String>> topology) {
        ConcurrentHashMap<String, ArrayList<String>> updatedTopology = new ConcurrentHashMap<String, ArrayList<String>>();
        Iterator<Map.Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            String taskName = pair.getKey();
            ArrayList<String> downStreamNames = pair.getValue();
            String parentTask = taskName + ":" + Integer.toString(taskIdentifierIndex.get(taskName)) + "@" +
                    taskAddressIndex.get(taskName + ":" + Integer.toString(taskIdentifierIndex.get(taskName)));
            if(downStreamNames != null && downStreamNames.size() > 0) {
                ArrayList<String> downStreamIds = new ArrayList<String>();
                for(String name : downStreamNames) {
                    if(taskIdentifierIndex.containsKey(name) == false) {
                        assert taskIdentifierIndex.containsKey(name) == true;
                    }
                    String childTask = name + ":" + Integer.toString(taskIdentifierIndex.get(name)) + "@" +
                            taskAddressIndex.get(name + ":" + Integer.toString(taskIdentifierIndex.get(name)));
                    downStreamIds.add(childTask);
                }
                updatedTopology.put(parentTask, downStreamIds);
            }else {
                updatedTopology.put(parentTask, new ArrayList<String>());
            }
        }
        return updatedTopology;
    }

    public static ConcurrentHashMap<String, ArrayList<String>> getInitialActiveTopologyWithJoinOperators(
            ConcurrentHashMap<String, ArrayList<String>> topology,
            ConcurrentHashMap<String, ArrayList<String>> inverseTopology,
            ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation, boolean minimalFlag) {
        ConcurrentHashMap<String, ArrayList<String>> activeTopology = new ConcurrentHashMap<String, ArrayList<String>>();
        ArrayList<String> activeTasks = new ArrayList<String>();
        ConcurrentHashMap<String, ArrayList<String>> layerTopology = ScaleFunction.produceTopologyLayers(
                topology, inverseTopology);
        /**
         * If minimal flag is set to false, then all existing nodes
         * will be set as active
         */
        if(minimalFlag == false) {
            activeTopology.putAll(topology);
            return activeTopology;
        }
        /**
         * Add all source operators first
         */
        Iterator<Map.Entry<String, ArrayList<String>>> itr = inverseTopology.entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            String taskName = pair.getKey();
            ArrayList<String> parentTasks = pair.getValue();
            if(parentTasks == null) {
                activeTasks.add(taskName);
            }else if(parentTasks != null && parentTasks.size() == 0) {
                activeTasks.add(taskName);
            }
        }
        /**
         * Add all drain operators second
         */
        itr = topology.entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            String taskName = pair.getKey();
            ArrayList<String> childTasks = pair.getValue();
            if(childTasks == null || childTasks.size() == 0) {
                activeTasks.add(taskName);
            }
        }
        /**
         * From each operator layer (stage of computation) add one node.
         * If the layer consists of JOIN operators, add one for each relation
         */
        itr = layerTopology.entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            ArrayList<String> layerTasks = pair.getValue();
            /**
             * Check if this is a layer of Join operators (dispatchers)
             */
            Integer candidateTask = Integer.parseInt(layerTasks.get(0).split("[:@]")[1]);
            if(taskToJoinRelation.containsKey(candidateTask) &&
                    taskToJoinRelation.get(candidateTask).getStep().equals(JoinOperator.Step.JOIN)) {
                String relation = taskToJoinRelation.get(candidateTask).getRelation();
                activeTasks.add(layerTasks.get(0));
                for(int i = 1; i < layerTasks.size(); i++) {
                    Integer otherCandidateTask = Integer.parseInt(layerTasks.get(i).split("[:@]")[1]);
                    if(taskToJoinRelation.containsKey(otherCandidateTask) &&
                            taskToJoinRelation.get(otherCandidateTask).getStep().equals(JoinOperator.Step.JOIN) &&
                            taskToJoinRelation.get(otherCandidateTask).getRelation().equals(relation) == false) {
                        activeTasks.add(layerTasks.get(i));
                        break;
                    }
                }
            }else {
                activeTasks.add(layerTasks.get(0));
            }
        }
        /**
         * Now create the activeTopology by adding each node
         * in the activeNodes list, along with its active downstream
         * operators (also in the activeNodes list)
         */
        for(String activeTask : activeTasks) {
            ArrayList<String> children = topology.get(activeTask);
            ArrayList<String> activeChildren = new ArrayList<String>();
            for(String childTask : children) {
                if(activeTasks.indexOf(childTask) >= 0) {
                    activeChildren.add(childTask);
                }
            }
            activeTopology.put(activeTask, activeChildren);
        }
        return activeTopology;
    }

    public static ConcurrentHashMap<String, ArrayList<String>> getInverseTopology(ConcurrentHashMap<String, ArrayList<String>> topology) {
        ConcurrentHashMap<String, ArrayList<String>> inverseTopology = new ConcurrentHashMap<String, ArrayList<String>>();
        if(topology == null || topology.size() == 0)
            return null;
        Iterator<Map.Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> entry = itr.next();
            String taskName = entry.getKey();
            ArrayList<String> downStreamNames = entry.getValue();
            if(downStreamNames != null && downStreamNames.size() > 0) {
                for(String downStreamTask : downStreamNames) {
                    if(inverseTopology.containsKey(downStreamTask)) {
                        ArrayList<String> parentList = inverseTopology.get(downStreamTask);
                        if(parentList.indexOf(taskName) < 0) {
                            parentList.add(taskName);
                            inverseTopology.put(downStreamTask, parentList);
                        }
                    }else {
                        ArrayList<String> parentList = new ArrayList<String>();
                        parentList.add(taskName);
                        inverseTopology.put(downStreamTask, parentList);
                    }
                }
                if(inverseTopology.containsKey(taskName) == false)
                    inverseTopology.put(taskName, new ArrayList<String>());
            }
        }
        return inverseTopology;
    }

}
