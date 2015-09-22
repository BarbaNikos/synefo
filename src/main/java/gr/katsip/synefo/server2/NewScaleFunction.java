package gr.katsip.synefo.server2;

import gr.katsip.synefo.storm.api.GenericPair;
import gr.katsip.synefo.storm.api.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by katsip on 9/22/2015.
 */
public class NewScaleFunction {

    private static final int SCALE_EPOCH = 1000;

    private int counter;

    private HashMap<String, List<Double>> processor;

    private HashMap<String, List<Double>> memory;

    private HashMap<String, List<Double>> latency;

    private HashMap<String, List<Double>> inputRate;

    private HashMap<String, List<Double>> state;

    private Map<String, ArrayList<String>> topology;

    private Map<String, ArrayList<String>> activeTopology;

    private Map<Integer, JoinOperator> taskToJoinRelation;

    private Map<String, Pair<Number, Number>> thresholds;

    public NewScaleFunction(Map<Integer, JoinOperator> taskToJoinRelation,
                            Map<String, Pair<Number, Number>> thresholds) {
        processor = new HashMap<>();
        memory = new HashMap<>();
        latency = new HashMap<>();
        inputRate = new HashMap<>();
        state = new HashMap<>();
        this.taskToJoinRelation = new HashMap<>(taskToJoinRelation);
        counter = 0;
        if (thresholds != null) {
            this.thresholds = thresholds;
        }
    }

    public void updateTopology(Map<String, ArrayList<String>> topology) {
        this.topology = new HashMap<>(topology);
    }

    public void updateActiveTopology(Map<String, ArrayList<String>> activeTopology) {
        this.activeTopology = new HashMap<>(activeTopology);
    }

    public GenericPair<String, String> addData(String taskName, Integer taskIdentifier, double processor,
                        double memory, double latency, double inputRate, double state) {
        String identifier = taskName + ":" + taskIdentifier;
        add(this.processor, identifier, processor);
        add(this.memory, identifier, memory);
        add(this.latency, identifier, latency);
        add(this.inputRate, identifier, inputRate);
        add(this.state, identifier, state);
        counter++;
        if (counter >= SCALE_EPOCH) {
            /**
             * Check if scale is needed
             */
            counter = 0;
            return scaleCheck();
        }else {
            return new GenericPair<>();
        }
    }

    public GenericPair<String, String> scaleCheck() {
        GenericPair<String, String> scaleAction = null;
        /**
         * First create a list with the overloaded workers
         */
        List<String> overloadedWorkers = new ArrayList<>();
        String struggler = "";
        Double bottleneck = -1.0;
        Iterator<Map.Entry<String, List<Double>>> iterator = inputRate.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<Double>> pair = iterator.next();
            Double average = 0.0;
            for (int i = pair.getValue().size() - 1; i >= (pair.getValue().size() - 4); i--) {
                average += pair.getValue().get(i);
            }
            average = average / 3.0;
            if (average >= thresholds.get("input-rate").upperBound.doubleValue() &&
                    overloadedWorkers.indexOf(pair.getKey()) < 0) {
                overloadedWorkers.add(pair.getKey());
                if (average > bottleneck) {
                    struggler = pair.getKey();
                    bottleneck = average;
                }
            }
        }
        String upstreamTask = getParentNode(topology, struggler);
        ArrayList<String> availableNodes = null;
        Integer identifier = Integer.parseInt(struggler.split("[:@]")[1]);
        if (upstreamTask != null && taskToJoinRelation.containsKey(identifier)) {
            availableNodes = getAvailableTasks(topology, activeTopology, upstreamTask, identifier, taskToJoinRelation);
            if (availableNodes.size() > 0) {
                String chosenTask = randomChoice(availableNodes);
                /**
                 * Update active-topology
                 */
                scaleAction = new GenericPair<String, String>(upstreamTask, chosenTask);
            }
        }
        /**
         * Check for scale-in action
         */
        if (scaleAction == null) {
            List<String> underloadedWorkers = new ArrayList<>();
            String slacker = "";
            Double opening = thresholds.get("input-rate").upperBound.doubleValue();
            iterator = inputRate.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, List<Double>> pair = iterator.next();
                Double average = 0.0;
                for (int i = pair.getValue().size() - 1; i >= (pair.getValue().size() - 4); i--) {
                    average += pair.getValue().get(i);
                }
                if (average <= thresholds.get("input-rate").lowerBound.doubleValue() &&
                        underloadedWorkers.indexOf(pair.getKey()) < 0) {
                    underloadedWorkers.add(pair.getKey());
                    if (average < opening) {
                        slacker = pair.getKey();
                        opening = average;
                    }
                }
            }
            upstreamTask = getParentNode(topology, slacker);
            identifier = Integer.parseInt(slacker.split("[:@]")[1]);
            availableNodes = null;
            if (activeTopology.containsKey(slacker) == false) {

            }
        }
        return null;
    }

    private static String randomChoice(List<String> availableNodes) {
        Random random = new Random();
        return availableNodes.get(random.nextInt(availableNodes.size()));
    }

    private void add(HashMap<String, List<Double>> storage, String identifier, Double data) {
        if (storage.containsKey(identifier)) {
            List<Double> points = storage.get(identifier);
            points.add(data);
            storage.put(identifier, points);
        }else {
            List<Double> points = new ArrayList<>();
            points.add(data);
            storage.put(identifier, points);
        }
    }

    public static ArrayList<String> getAvailableTasks(Map<String, ArrayList<String>> topology,
                                                         Map<String, ArrayList<String>> activeTopology,
                                                         String upstreamTask, Integer strugglerIdentifier,
                                                         Map<Integer, JoinOperator> taskToJoinRelation) {
        ArrayList<String> availableNodes = new ArrayList<String>();
        ArrayList<String> physicalNodes = topology.get(upstreamTask);
        physicalNodes.removeAll(activeTopology.get(upstreamTask));
        for(String task : physicalNodes) {
            Integer identifier = Integer.parseInt(task.split("[:@]")[1]);
            if(taskToJoinRelation.get(identifier).getRelation().equals(
                    taskToJoinRelation.get(strugglerIdentifier).getRelation())) {
                availableNodes.add(task);
            }
        }
        return availableNodes;
    }

    public static String getParentNode(Map<String, ArrayList<String>> topology, String taskName) {
        Iterator<Map.Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
        while(itr.hasNext()) {
            Map.Entry<String, ArrayList<String>> pair = itr.next();
            ArrayList<String> tasks = pair.getValue();
            for(String t : tasks) {
                if(t.equals(taskName))
                    return pair.getKey();
            }
        }
        return null;
    }
}
