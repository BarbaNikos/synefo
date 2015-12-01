package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.*;

/**
 * Created by katsip on 10/21/2015.
 */
public class CollocatedWindowDispatcher implements Serializable {

    Logger logger = LoggerFactory.getLogger(CollocatedWindowDispatcher.class);

    private long window;

    private long slide;

    private String outerRelationName;

    private String innerRelationName;

    private Fields outerRelationSchema;

    private Fields innerRelationSchema;

    private String outerRelationKey;

    private String innerRelationKey;

    private Fields outputSchema;

    private LinkedList<CollocatedDispatchWindow> ringBuffer;

    private int bufferSize;

    private HashMap<String, List<Integer>> taskToRelationIndex;

    private int innerRelationCardinality;

    private int outerRelationCardinality;

    private int index;

    private HashMap<Integer, Long> numberOfTuplesPerTask;

    public CollocatedWindowDispatcher(String outerRelationName, Fields outerRelationSchema, String outerRelationKey,
                                      String innerRelationName, Fields innerRelationSchema, String innerRelationKey,
                                      Fields outputSchema, long window, long slide) {
        this.window = window;
        this.slide = slide;
        bufferSize = (int) Math.ceil(this.window / this.slide);
        ringBuffer = new LinkedList<>();
        this.outerRelationName = outerRelationName;
        this.innerRelationName = innerRelationName;
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        this.innerRelationKey = innerRelationKey;
        this.outerRelationKey = outerRelationKey;
        this.outputSchema = new Fields(outputSchema.toList());
        innerRelationCardinality = 0;
        outerRelationCardinality = 0;
        index = 0;
        numberOfTuplesPerTask = new HashMap<>();
    }

    public void setTaskToRelationIndex(List<Integer> activeDownstreamTaskIdentifiers) {
        taskToRelationIndex = new HashMap<>();
        taskToRelationIndex.put(innerRelationName, new ArrayList<>(activeDownstreamTaskIdentifiers));
        taskToRelationIndex.put(outerRelationName, new ArrayList<>(activeDownstreamTaskIdentifiers));
        index = 0;
    }

    public void setOutputSchema(Fields outputSchema) {
        this.outputSchema = new Fields(outputSchema.toList());
    }

    public int locateTask(Long timestamp, String relation, String key) {
        int task = -1;
        for (int i = 0; i < ringBuffer.size(); i++) {
            CollocatedDispatchWindow window = ringBuffer.get(i);
            if ((window.start + this.window) >= timestamp) {
                if (relation.equals(innerRelationName)) {
                    if (window.innerRelationIndex.containsKey(key)) {
                        task = window.innerRelationIndex.get(key).get(0);
                        break;
                    }
                } else if (relation.equals(outerRelationName)) {
                    if (window.outerRelationIndex.containsKey(key)) {
                        task = window.outerRelationIndex.get(key).get(0);
                        break;
                    }
                }
            }else {
                break;
            }
        }
        return task;
    }

    public int pickTaskForNewKey() {
        int victim = taskToRelationIndex.get(innerRelationName).get(index);
        if (index == taskToRelationIndex.get(innerRelationName).size() - 1)
            index = 0;
        else
            index++;
        return victim;
    }

    public int execute(String streamId, Tuple anchor, OutputCollector collector, Fields fields, Values values, List<String> migratedKeys,
                       int scaledTask, int candidateTask, String action) {
        long currentTimestamp = System.currentTimeMillis();
        int numberOfDispatchedTuples = 0;
        int victimTask;
        String key = null;
        String relationName = null;
        Values tuple = new Values();
        tuple.add("0");
        tuple.add(fields);
        tuple.add(values);
        if (fields.toList().toString().equals(innerRelationSchema.toList().toString())) {
            relationName = innerRelationName;
            key = (String) values.get(innerRelationSchema.fieldIndex(innerRelationKey));
        }else if (fields.toList().toString().equals(outerRelationSchema.toList().toString())) {
            relationName = outerRelationName;
            key = (String) values.get(outerRelationSchema.fieldIndex(outerRelationKey));
        }
        victimTask = locateTask(currentTimestamp, relationName, key);
        if (migratedKeys.size() > 0 && migratedKeys.indexOf(key) >= 0 && scaledTask != -1 && candidateTask != -1) {
            updateCurrentWindow(currentTimestamp, relationName, key, candidateTask);
            if (victimTask >= 0 && victimTask != scaledTask && victimTask != candidateTask) {
                logger.error("inconsistency located. victim-task: " + victimTask + " is neither equal to scaled-task (" +
                        scaledTask + ") nor candidate-task (" + candidateTask + ")");
                throw new RuntimeException("Candidate inconsistency");
            }
            if (collector != null) {
                collector.emitDirect(candidateTask, streamId, anchor, tuple);
                collector.emitDirect(scaledTask, streamId, anchor, tuple);
                numberOfDispatchedTuples += 2;
            }
        }else {
            if (victimTask < 0)
                victimTask = pickTaskForNewKey();
            updateCurrentWindow(currentTimestamp, relationName, key, victimTask);
            if (collector != null && victimTask >= 0) {
                collector.emitDirect(victimTask, streamId, anchor, tuple);
                numberOfDispatchedTuples++;
            }
        }
        return numberOfDispatchedTuples;
    }

    public void garbageCollect(Long timestamp) {
        if (ringBuffer.size() >= bufferSize && (ringBuffer.getLast().start + this.window) <= timestamp) {
            CollocatedDispatchWindow window = ringBuffer.removeLast();
            innerRelationCardinality -= (window.innerRelationCardinality);
            outerRelationCardinality -= (window.outerRelationCardinality);
            Iterator<Map.Entry<Integer, Integer>> iterator = window.numberOfTuplesPerTask.entrySet().iterator();
            //Decrement statistics
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                numberOfTuplesPerTask.put(entry.getKey(),
                        new Long(numberOfTuplesPerTask.get(entry.getKey()) - entry.getValue()));
            }
        }
    }

    public void updateCurrentWindow(Long timestamp, String relation, String key, Integer victimTask) {
        garbageCollect(timestamp);
        if (ringBuffer.size() == 0) {
            CollocatedDispatchWindow window = new CollocatedDispatchWindow();
            window.start = timestamp;
            window.end = window.start + slide;
            if (relation.equals(innerRelationName)) {
                List<Integer> tasks = new ArrayList<>();
                tasks.add(victimTask);
                window.innerRelationIndex.put(key, tasks);
                window.innerRelationCardinality += 1;
                innerRelationCardinality += 1;
            }else if (relation.equals(outerRelationName)) {
                List<Integer> tasks = new ArrayList<>();
                tasks.add(victimTask);
                window.outerRelationIndex.put(key, tasks);
                window.outerRelationCardinality += 1;
                outerRelationCardinality += 1;
            }
            window.numberOfTuplesPerTask.put(victimTask, new Integer(1));
            if (numberOfTuplesPerTask.containsKey(victimTask))
                numberOfTuplesPerTask.put(victimTask, new Long(numberOfTuplesPerTask.get(victimTask) + 1L));
            else
                numberOfTuplesPerTask.put(victimTask, new Long(1L));
            window.keyToTaskMapping.put(key, victimTask);
            window.stateSize += (key.length() + 4);
            ringBuffer.addFirst(window);
        }else if (ringBuffer.getFirst().end < timestamp && ringBuffer.size() < bufferSize) {
            CollocatedDispatchWindow window = new CollocatedDispatchWindow();
            window.start = ringBuffer.getFirst().end + 1;
            window.end = window.start + slide;
            List<Integer> tasks = new ArrayList<>();
            tasks.add(victimTask);
            if (relation.equals(innerRelationName)) {
                window.innerRelationIndex.put(key, tasks);
                window.innerRelationCardinality += 1;
                innerRelationCardinality += 1;
            }else if (relation.equals(outerRelationName)) {
                window.outerRelationIndex.put(key, tasks);
                window.outerRelationCardinality += 1;
                outerRelationCardinality += 1;
            }
            window.numberOfTuplesPerTask.put(victimTask, new Integer(1));
            if (numberOfTuplesPerTask.containsKey(victimTask))
                numberOfTuplesPerTask.put(victimTask, new Long(numberOfTuplesPerTask.get(victimTask) + 1L));
            else
                numberOfTuplesPerTask.put(victimTask, new Long(1L));
            window.keyToTaskMapping.put(key, victimTask);
            window.stateSize += (key.length() + 4);
            ringBuffer.addFirst(window);
        }else {
            List<Integer> tasks = new ArrayList<>();
            tasks.add(victimTask);
            if (relation.equals(innerRelationName)) {
                if (!ringBuffer.getFirst().innerRelationIndex.containsKey(key)) {
                    ringBuffer.getFirst().innerRelationIndex.put(key, tasks);
                    ringBuffer.getFirst().stateSize += (key.length() + 4);
                }
                ringBuffer.getFirst().innerRelationCardinality += 1;
                innerRelationCardinality += 1;
            }else if (relation.equals(outerRelationName)) {
                if (!ringBuffer.getFirst().outerRelationIndex.containsKey(key)) {
                    ringBuffer.getFirst().outerRelationIndex.put(key, tasks);
                    ringBuffer.getFirst().stateSize += (key.length() + 4);
                }
                ringBuffer.getFirst().outerRelationCardinality += 1;
                outerRelationCardinality += 1;
            }
            Integer count;
            if (ringBuffer.getFirst().numberOfTuplesPerTask.containsKey(victimTask))
                count = ringBuffer.getFirst().numberOfTuplesPerTask.get(victimTask);
            else
                count = new Integer(0);
            if (numberOfTuplesPerTask.containsKey(victimTask))
                numberOfTuplesPerTask.put(victimTask, new Long(numberOfTuplesPerTask.get(victimTask) + 1L));
            else
                numberOfTuplesPerTask.put(victimTask, new Long(1L));
            ringBuffer.getFirst().numberOfTuplesPerTask.put(victimTask, ++count);
            ringBuffer.getFirst().keyToTaskMapping.put(key, victimTask);
        }
    }

    public List<String> getKeysForATask(Integer task) {
        if (task < 0)
            return null;
        List<String> keys = new ArrayList<>();
        Long timestamp = System.currentTimeMillis();
        for (int i = 0; i < ringBuffer.size(); i++) {
            CollocatedDispatchWindow window = ringBuffer.get(i);
            if ((window.start + this.window) > timestamp) {
                for (String key : window.keyToTaskMapping.keySet()) {
                    if (window.keyToTaskMapping.get(key) == task && !keys.contains(key)) {
                        keys.add(key);
                    }
                }
            }else {
                break;
            }
        }
        return keys;
    }

    public Fields getOutputSchema() {
        return outputSchema;
    }

    public HashMap<Integer, Long> getNumberOfTuplesPerTask() {
        return numberOfTuplesPerTask;
    }

    public void mergeState(List<Values> state) {
        //Single Dispatcher version - Not supported
    }

    public List<Values> getState() {
        //Single Dispatcher version - Not supported
        return null;
    }

    public long getStateSize() {
        return 0;
    }

    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {
        //Single Dispatcher version - Not supported
    }

    public void reinitializeBuffer() {
        ringBuffer.clear();
        numberOfTuplesPerTask.clear();
    }

    public void reassignKeys(List<String> migratedKeys, int scaledTask, int candidateTask) {
        Long timestamp = System.currentTimeMillis();
        for (int i = 0; i < ringBuffer.size(); i++) {
            CollocatedDispatchWindow window = ringBuffer.get(i);
            if ((window.start + this.window) >= timestamp) {
                for (String key : migratedKeys) {
                    //The following assert is for debug purposes
                    if (window.keyToTaskMapping.containsKey(key))
                        assert window.keyToTaskMapping.get(key) == scaledTask;
                    if (window.keyToTaskMapping.containsKey(key) && window.keyToTaskMapping.get(key) == scaledTask)
                        window.keyToTaskMapping.put(key, candidateTask);
                    if (window.innerRelationIndex.containsKey(key) && window.innerRelationIndex.get(key).get(0) == scaledTask)
                        window.innerRelationIndex.get(key).set(0, candidateTask);
                    if (window.outerRelationIndex.containsKey(key)&& window.outerRelationIndex.get(key).get(0) == scaledTask)
                        window.outerRelationIndex.get(key).set(0, candidateTask);
                }
            }
        }
    }

}
