package gr.katsip.synefo.utils;

import backtype.storm.tuple.Values;
import gr.katsip.synefo.server2.JoinOperator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by katsip on 9/24/2015.
 */
public class Util {

    public static int BOLT_STAT_BATCH_SIZE = 5;

    public static String serializeTopology(ConcurrentHashMap<String, ArrayList<String>> topology) {
        StringBuilder strBuild = new StringBuilder();
        Iterator<Map.Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> entry = itr.next();
            String task = entry.getKey();
            strBuild.append(task + "=");
            for(String downstreamTask : entry.getValue()) {
                strBuild.append(downstreamTask + ",");
            }
            if(strBuild.length() > 0 && strBuild.charAt(strBuild.length() - 1) == ',') {
                strBuild.setLength(strBuild.length() - 1);
            }
            strBuild.append("&");
        }
        if(strBuild.length() > 0 && strBuild.charAt(strBuild.length() - 1) == '&') {
            strBuild.setLength(strBuild.length() - 1);
        }
        return strBuild.toString();
    }

    public static ConcurrentHashMap<String, ArrayList<String>> deserializeTopology(String serializedTopology) {
        ConcurrentHashMap<String, ArrayList<String>> topology = new ConcurrentHashMap<String, ArrayList<String>>();
        String[] pairs = serializedTopology.split("&");
        for (String pair : pairs) {
            if (pair != "") {
                String[] tokens = pair.split("=");
                String key = tokens[0];
                if (tokens.length > 1) {
                    String[] values = tokens[1].split(",");
                    ArrayList<String> tasks = new ArrayList<String>();
                    for (int i = 0; i < values.length; i++) {
                        tasks.add(values[i]);
                    }
                    topology.put(key, tasks);
                }else {
                    topology.put(key, new ArrayList<String>());
                }
            }
        }
        return topology;
    }

    public static ConcurrentHashMap<String, ArrayList<String>> getInverseTopology(ConcurrentHashMap<String, ArrayList<String>> topology) {
        ConcurrentHashMap<String, ArrayList<String>> inverseTopology = new ConcurrentHashMap<>();
        Iterator<Map.Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> entry = itr.next();
            String taskName = entry.getKey();
            ArrayList<String> downStreamNames = entry.getValue();
            if(downStreamNames.size() > 0) {
                for (String downStreamTask : downStreamNames) {
                    if (inverseTopology.containsKey(downStreamTask)) {
                        ArrayList<String> parentList = inverseTopology.get(downStreamTask);
                        if (parentList.indexOf(taskName) < 0) {
                            parentList.add(taskName);
                            inverseTopology.put(downStreamTask, parentList);
                        }
                    }else {
                        ArrayList<String> parentList = new ArrayList<String>();
                        parentList.add(taskName);
                        inverseTopology.put(downStreamTask, parentList);
                    }
                }
                if (inverseTopology.containsKey(taskName) == false)
                    inverseTopology.put(taskName, new ArrayList<String>());
            }
        }
        return inverseTopology;
    }

    /**
     * This function separates the topology operators into different layers (stages) of computation. In each layer,
     * the source operators (nodes with no upstream operators) and the drain operators (operators with no downstream operators) are
     * not included. For each operator, a signature-key is created by appending all parent operators of that node, followed by
     * all child operators. For instance, if operator X has operators {A, B} as parents, and operators {Z, Y} as children, the
     * signature-key is created as : "A,B,Z,Y". After a signature-key is created, the operator is stored in a HashMap with key
     * its own signature and its name is added in a list. If two or more operators have the same signature-key they are added
     * in the same bucket in the HashMap.
     * @param physicalTopology The physical topology of operators in synefo
     * @param inverseTopology The map that contains the parent operators (upstream) of each operator
     * @return a HashMap with the the operators separated in different buckets, according to their parent-operators list and children-operator lists.
     */
    public static ConcurrentHashMap<String, ArrayList<String>> produceTopologyLayers(
            ConcurrentHashMap<String, ArrayList<String>> physicalTopology,
            ConcurrentHashMap<String, ArrayList<String>> inverseTopology) {
        ConcurrentHashMap<String, ArrayList<String>> operatorLayers = new ConcurrentHashMap<String, ArrayList<String>>();
        Iterator<Map.Entry<String, ArrayList<String>>> itr = physicalTopology.entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            String taskName = pair.getKey();
            ArrayList<String> childOperators = pair.getValue();
            ArrayList<String> parentOperators = inverseTopology.get(taskName);
            if(childOperators != null && childOperators.size() > 0 &&
                    parentOperators != null && parentOperators.size() > 0) {
                StringBuilder strBuild = new StringBuilder();
                for(String parent : parentOperators) {
                    strBuild.append(parent + ",");
                }
                for(String child : childOperators) {
                    strBuild.append(child + ",");
                }
                strBuild.setLength(strBuild.length() - 1);
                String key = strBuild.toString();
                if(operatorLayers.containsKey(key)) {
                    ArrayList<String> layerOperators = operatorLayers.get(key);
                    layerOperators.add(taskName);
                    operatorLayers.put(key, layerOperators);
                }else {
                    ArrayList<String> layerOperators = new ArrayList<String>();
                    layerOperators.add(taskName);
                    operatorLayers.put(key, layerOperators);
                }
            }
        }
        return operatorLayers;
    }

    public static ConcurrentHashMap<String, ArrayList<String>> getInitialActiveTopologyWithJoinOperators(
            ConcurrentHashMap<String, ArrayList<String>> topology,
            ConcurrentHashMap<String, ArrayList<String>> inverseTopology,
            ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation, boolean minimalFlag) {
        ConcurrentHashMap<String, ArrayList<String>> activeTopology = new ConcurrentHashMap<String, ArrayList<String>>();
        ArrayList<String> activeTasks = new ArrayList<String>();
        ConcurrentHashMap<String, ArrayList<String>> layerTopology = produceTopologyLayers(
                topology, inverseTopology);
        /**
         * If minimal flag is set to false, then all existing nodes
         * will be set as active
         */
        if (!minimalFlag) {
            activeTopology.putAll(topology);
            return activeTopology;
        }
        /**
         * Add all source operators first
         */
        Iterator<Map.Entry<String, ArrayList<String>>> itr = inverseTopology.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            String taskName = pair.getKey();
            ArrayList<String> parentTasks = pair.getValue();
            if (parentTasks == null) {
                activeTasks.add(taskName);
            }else if (parentTasks != null && parentTasks.size() == 0) {
                activeTasks.add(taskName);
            }
        }
        /**
         * Add all drain operators (operators with no down-stream tasks) second
         */
        itr = topology.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            String taskName = pair.getKey();
            Integer identifier = Integer.parseInt(taskName.split(":")[1]);
            ArrayList<String> childTasks = pair.getValue();
            if (childTasks == null || childTasks.size() == 0 && !taskToJoinRelation.containsKey(identifier)) {
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
            Integer candidateTask = Integer.parseInt(layerTasks.get(0).split("[:]")[1]);
            if (taskToJoinRelation.containsKey(candidateTask) &&
                    taskToJoinRelation.get(candidateTask).getStep().equals(JoinOperator.Step.JOIN)) {
                String relation = taskToJoinRelation.get(candidateTask).getRelation();
                activeTasks.add(layerTasks.get(0));
                for (int i = 1; i < layerTasks.size(); i++) {
                    Integer otherCandidateTask = Integer.parseInt(layerTasks.get(i).split("[:]")[1]);
                    if (taskToJoinRelation.containsKey(otherCandidateTask) &&
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

    public static ConcurrentHashMap<String, ArrayList<String>> topologyTaskExpand(final ConcurrentHashMap<String, Integer> taskIdentifierIndex,
                                                                                  final ConcurrentHashMap<String, ArrayList<String>> topology) {
        ConcurrentHashMap<String, ArrayList<String>> physicalTopologyWithIds = new ConcurrentHashMap<String, ArrayList<String>>();
        Iterator<Map.Entry<String, Integer>> taskNameIterator = taskIdentifierIndex.entrySet().iterator();
        while(taskNameIterator.hasNext()) {
            Map.Entry<String, Integer> pair = taskNameIterator.next();
            String taskName = pair.getKey();
            String taskNameWithoutId = taskName.split("_")[0];
            ArrayList<String> downstreamTaskList = new ArrayList<>(topology.get(taskNameWithoutId));
            if (downstreamTaskList != null && downstreamTaskList.size() > 0) {
                ArrayList<String> downstreamTaskWithIdList = new ArrayList<String>();
                for (String downstreamTask : downstreamTaskList) {
                    Iterator<Map.Entry<String, Integer>> downstreamTaskIterator = taskIdentifierIndex.entrySet().iterator();
                    while (downstreamTaskIterator.hasNext()) {
                        Map.Entry<String, Integer> downstreamPair = downstreamTaskIterator.next();
                        if(downstreamPair.getKey().split("_")[0].equals(downstreamTask)) {
                            downstreamTaskWithIdList.add(downstreamPair.getKey());
                        }
                    }
                }
                physicalTopologyWithIds.put(taskName, downstreamTaskWithIdList);
            } else {
                physicalTopologyWithIds.put(taskName, downstreamTaskList);
            }
        }
        return physicalTopologyWithIds;
    }

//    public static ConcurrentHashMap<String, ArrayList<String>> updateTopology(final ConcurrentHashMap<String, String> taskAddressIndex,
//                                                                              final ConcurrentHashMap<String, Integer> taskIdentifierIndex,
//                                                                              final ConcurrentHashMap<String, ArrayList<String>> topology) {
//        ConcurrentHashMap<String, ArrayList<String>> updatedTopology = new ConcurrentHashMap<String, ArrayList<String>>();
//        Iterator<Map.Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
//        while(itr.hasNext()) {
//            Map.Entry<String, ArrayList<String>> pair = itr.next();
//            String taskName = pair.getKey();
//            ArrayList<String> downStreamNames = pair.getValue();
//            String parentTask = taskName + ":" + Integer.toString(taskIdentifierIndex.get(taskName)) + "@" +
//                    taskAddressIndex.get(taskName + ":" + Integer.toString(taskIdentifierIndex.get(taskName)));
//            if(downStreamNames != null && downStreamNames.size() > 0) {
//                ArrayList<String> downStreamIds = new ArrayList<String>();
//                for(String name : downStreamNames) {
//                    if(taskIdentifierIndex.containsKey(name) == false) {
//                        assert taskIdentifierIndex.containsKey(name) == true;
//                    }
//                    String childTask = name + ":" + Integer.toString(taskIdentifierIndex.get(name)) + "@" +
//                            taskAddressIndex.get(name + ":" + Integer.toString(taskIdentifierIndex.get(name)));
//                    downStreamIds.add(childTask);
//                }
//                updatedTopology.put(parentTask, downStreamIds);
//            }else {
//                updatedTopology.put(parentTask, new ArrayList<String>());
//            }
//        }
//        return updatedTopology;
//    }

    public static ConcurrentHashMap<String, ArrayList<String>> updateTopology(final ConcurrentHashMap<String, Integer> taskIdentifierIndex,
                                                                              final ConcurrentHashMap<String, ArrayList<String>> topology) {
        ConcurrentHashMap<String, ArrayList<String>> updatedTopology = new ConcurrentHashMap<String, ArrayList<String>>();
        Iterator<Map.Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            String taskName = pair.getKey();
            ArrayList<String> downStreamNames = pair.getValue();
            String parentTask = taskName + ":" + Integer.toString(taskIdentifierIndex.get(taskName));
            if (downStreamNames != null && downStreamNames.size() > 0) {
                ArrayList<String> downStreamIds = new ArrayList<String>();
                for(String name : downStreamNames) {
                    String childTask = name + ":" + Integer.toString(taskIdentifierIndex.get(name));
                    downStreamIds.add(childTask);
                }
                updatedTopology.put(parentTask, downStreamIds);
            }else {
                updatedTopology.put(parentTask, new ArrayList<String>());
            }
        }
        return updatedTopology;
    }

    public static List<String> getActiveJoinNodes(final Map<String, ArrayList<String>> activeTopology,
                                                  final String upstreamTask, final String slacker,
                                                  final Map<Integer, JoinOperator> taskToJoinRelation) {
        if(activeTopology.containsKey(upstreamTask) == false)
            return new ArrayList<>();
        List<String> activeNodes = new ArrayList<String>(activeTopology.get(upstreamTask));
        List<String> sameRelationActiveNodes = new ArrayList<String>();
        Integer slackerIdentifier = Integer.parseInt(slacker.split("[:]")[1]);
        for(String task : activeNodes) {
            Integer identifier = Integer.parseInt(task.split("[:]")[1]);
            if(taskToJoinRelation.containsKey(identifier) &&
                    taskToJoinRelation.get(identifier).getRelation()
                            .equals(taskToJoinRelation.get(slackerIdentifier).getRelation()) &&
                    identifier != slackerIdentifier) {
                sameRelationActiveNodes.add(task);
            }
        }
        return sameRelationActiveNodes;
    }

    public static ArrayList<String> getAvailableTasks(final Map<String, ArrayList<String>> topology,
                                                      final Map<String, ArrayList<String>> activeTopology,
                                                      final String upstreamTask, final Integer strugglerIdentifier,
                                                      Map<Integer, JoinOperator> taskToJoinRelation) {
        ArrayList<String> availableTasks = new ArrayList<String>();
        ArrayList<String> allTasks = new ArrayList<String>(topology.get(upstreamTask));
        allTasks.removeAll(activeTopology.get(upstreamTask));
        for(String task : allTasks) {
            Integer identifier = Integer.parseInt(task.split("[:]")[1]);
            if (taskToJoinRelation.get(identifier).getStep().equals(taskToJoinRelation.get(strugglerIdentifier).getStep()) &&
                    taskToJoinRelation.get(identifier).getRelation().equals(
                            taskToJoinRelation.get(strugglerIdentifier).getRelation())) {
                availableTasks.add(task);
            }
        }
        return availableTasks;
    }

    public static String randomElementSelection(final List<String> availableNodes) {
        Random random = new Random();
        return availableNodes.get(random.nextInt(availableNodes.size()));
    }

    public static void mergeState(HashMap<String, ArrayList<Values>> state, final HashMap<String, ArrayList<Values>> statePacket) {
        Iterator<Map.Entry<String, ArrayList<Values>>> packetIterator = statePacket.entrySet().iterator();
        while (packetIterator.hasNext()) {
            Map.Entry<String, ArrayList<Values>> entry = packetIterator.next();
            if (state.containsKey(entry.getKey())) {
                //Merge two lists together
                ArrayList<Values> current = new ArrayList<>(state.get(entry.getKey()));
                for (Values tuple : entry.getValue()) {
                    if (current.lastIndexOf(tuple) < 0) {
                        current.add(tuple);
                    }
                }
                state.put(entry.getKey(), current);
            }else {
                state.put(entry.getKey(), new ArrayList<Values>(entry.getValue()));
            }
        }
    }

    public static void mergeDispatcherState(HashMap<String, List<Integer>> state, HashMap<String, List<Integer>> statePacket) {
        Iterator<Map.Entry<String, List<Integer>>> packetIterator = statePacket.entrySet().iterator();
        while (packetIterator.hasNext()) {
            Map.Entry<String, List<Integer>> entry = packetIterator.next();
            if (state.containsKey(entry.getKey())) {
                //Merge two lists together
                ArrayList<Integer> current = new ArrayList<>(state.get(entry.getKey()));
                for (Integer task : entry.getValue()) {
                    if (current.lastIndexOf(task) < 0) {
                        current.add(task);
                    }
                }
                state.put(entry.getKey(), current);
            }else {
                state.put(entry.getKey(), new ArrayList<Integer>(entry.getValue()));
            }
        }
    }

    public static String serializeList(final List<String> list) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String element : list) {
            stringBuilder.append(element + ",");
        }
        if (stringBuilder.length() > 0 && stringBuilder.charAt(stringBuilder.length() - 1) == ',') {
            stringBuilder.setLength(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }

    public static List<String> deserializeList(final String serializedList) {
        List<String> list = new ArrayList<>();
        String[] elements = serializedList.split(",");
        for (String element : elements) {
            list.add(element);
        }
        return list;
    }

}
