package gr.katsip.synefo.storm.operators.dispatcher.collocated;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Nick R. Katsipoulakis on 10/21/2015.
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

    private ArrayList<Integer> joinerTasks;

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

    /**
     * TODO: Check if taskToRelationIndex can be simplified
     * @param activeDownstreamTaskIdentifiers
     */
    public void setTaskToRelationIndex(List<Integer> activeDownstreamTaskIdentifiers) {
        joinerTasks = new ArrayList<>(activeDownstreamTaskIdentifiers);
        index = 0;
    }

    public void setOutputSchema(Fields outputSchema) {
        this.outputSchema = new Fields(outputSchema.toList());
    }

    public int locateTask(Long timestamp, String key) {
        int task = -1;
        for (int i = 0; i < ringBuffer.size(); i++) {
            CollocatedDispatchWindow window = ringBuffer.get(i);
            /**
             * If window is valid, get the task that currently has the tuple
             */
            if ((window.start + this.window) >= timestamp) {
                if (window.keyIndex.containsKey(key)) {
                    task = window.keyIndex.get(key);
//                    logger.info("found task: " + task + " for key: " + key);
                    break;
                }
            }else {
                break;
            }
        }
        return task;
    }

    public int pickTaskForNewKey() {
        int victim = joinerTasks.get(index);
        if (index == joinerTasks.size() - 1)
            index = 0;
        else
            index++;
        return victim;
    }

    public int execute(String streamId, Tuple anchor, OutputCollector collector, Fields fields, Values values,
                       List<String> migratedKeys, int scaledTask, int candidateTask, String action) {
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
        victimTask = locateTask(currentTimestamp, key);
        if (migratedKeys.size() > 0 && migratedKeys.indexOf(key) >= 0 && scaledTask != -1 && candidateTask != -1) {
//            logger.info("ongoing migration for received key[" + key + "], scaledTask: " + scaledTask + ", candidate: " +
//                    candidateTask + ", victim-task: " + victimTask + ", will update current window.");
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
//            String logInfo = "no migration for received key[" + key + "], victim-task: " + victimTask;
            if (victimTask < 0) {
                victimTask = pickTaskForNewKey();
                logger.info("picked task " + victimTask + " for key " + key);
//                logInfo = logInfo + "~~picked new task: " + victimTask + "";
            }
            updateCurrentWindow(currentTimestamp, relationName, key, victimTask);
            if (collector != null && victimTask >= 0) {
                collector.emitDirect(victimTask, streamId, anchor, tuple);
                numberOfDispatchedTuples++;
            }
//            logger.info(logInfo);
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
        if (ringBuffer.size() == 0 || (ringBuffer.getFirst().end < timestamp && ringBuffer.size() < bufferSize)) {
            CollocatedDispatchWindow window = new CollocatedDispatchWindow();
            if (ringBuffer.size() == 0)
                window.start = timestamp;
            else
                window.start = ringBuffer.getFirst().end + 1;
            window.end = window.start + slide;
            window.keyIndex.put(key, victimTask);
            if (relation.equals(innerRelationName)) {
                window.innerRelationCardinality += 1;
                innerRelationCardinality += 1;
            } else if (relation.equals(outerRelationName)) {
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
            if (!ringBuffer.getFirst().keyIndex.containsKey(key))
                ringBuffer.getFirst().keyIndex.put(key, victimTask);
            if (relation.equals(innerRelationName)) {
                ringBuffer.getFirst().stateSize += (key.length() + 4);
                ringBuffer.getFirst().innerRelationCardinality += 1;
                innerRelationCardinality += 1;
            }else if (relation.equals(outerRelationName)) {
                ringBuffer.getFirst().stateSize += (key.length() + 4);
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
                    if (window.keyIndex.containsKey(key) && window.keyIndex.get(key) == scaledTask)
                        window.keyIndex.put(key, candidateTask);
                }
            } else {
                break;
            }
        }
    }

}
