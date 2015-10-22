package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

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
    }

    public void setTaskToRelationIndex(List<Integer> activeDownstreamTaskIdentifiers) {
        this.taskToRelationIndex = new HashMap<>();
        this.taskToRelationIndex.put(innerRelationName, new ArrayList<>(activeDownstreamTaskIdentifiers));
        this.taskToRelationIndex.put(outerRelationName, new ArrayList<>(activeDownstreamTaskIdentifiers));
    }

    public void setOutputSchema(Fields outputSchema) {
        this.outputSchema = new Fields(outputSchema.toList());
    }

    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
        long currentTimestamp = System.currentTimeMillis();
        int numberOfDispatchedTuples = 0;
        Values tuple = new Values();
        tuple.add("0");
        tuple.add(fields);
        tuple.add(values);
        if (fields.toList().toString().equals(innerRelationSchema.toList().toString())) {
            String key = (String) values.get(innerRelationSchema.fieldIndex(innerRelationKey));
            int victimTaskIndex = key.hashCode() % taskToRelationIndex.get(innerRelationName).size();
            Integer victimTask = taskToRelationIndex.get(innerRelationName).get(victimTaskIndex);
            if (collector != null)
                collector.emitDirect(victimTask, anchor, tuple);
            updateStatistic(currentTimestamp, innerRelationName, victimTask, key);
            numberOfDispatchedTuples++;
            return numberOfDispatchedTuples;
        }else if (fields.toList().toString().equals(outerRelationSchema.toList().toString())) {
            String key = (String) values.get(outerRelationSchema.fieldIndex(outerRelationKey));
            int victimTaskIndex = key.hashCode() % taskToRelationIndex.get(outerRelationName).size();
            Integer victimTask = taskToRelationIndex.get(outerRelationName).get(victimTaskIndex);
            if (collector != null)
                collector.emitDirect(victimTask, anchor, tuple);
            updateStatistic(currentTimestamp, outerRelationName, victimTask, key);
            numberOfDispatchedTuples++;
            return numberOfDispatchedTuples;
        }
        return 0;
    }

    public void updateStatistic(Long timestamp, String relation, Integer victimTask, String key) {
        if (ringBuffer.size() >= bufferSize && (ringBuffer.getLast().start + this.window) <= timestamp) {
            CollocatedDispatchWindow window = ringBuffer.removeLast();
            innerRelationCardinality -= (window.innerRelationCardinality);
            outerRelationCardinality -= (window.outerRelationCardinality);
        }
        if (ringBuffer.size() == 0) {
            CollocatedDispatchWindow window = new CollocatedDispatchWindow();
            window.start = timestamp;
            window.end = window.start + slide;
            ringBuffer.addFirst(window);
        }else if (ringBuffer.getFirst().end < timestamp && ringBuffer.size() < bufferSize) {
            CollocatedDispatchWindow window = new CollocatedDispatchWindow();
            window.start = ringBuffer.getFirst().end + 1;
            window.end = window.start + slide;
            ringBuffer.addFirst(window);
        }
        if (relation.equals(innerRelationName)) {
            if (ringBuffer.getFirst().numberOfTuplesPerTask.containsKey(victimTask)) {
                Integer counter = ringBuffer.getFirst().numberOfTuplesPerTask.get(victimTask);
                ringBuffer.getFirst().numberOfTuplesPerTask.put(victimTask, ++counter);
            }else {
                ringBuffer.getFirst().numberOfTuplesPerTask.put(victimTask, new Integer(1));
            }
            if (!ringBuffer.getFirst().keyToTaskMapping.containsKey(key))
                ringBuffer.getFirst().keyToTaskMapping.put(key, new Integer(victimTask));
            ringBuffer.getFirst().innerRelationCardinality += 1L;
        }else if (relation.equals(outerRelationName)) {
            if (ringBuffer.getFirst().numberOfTuplesPerTask.containsKey(victimTask)) {
                Integer counter = ringBuffer.getFirst().numberOfTuplesPerTask.get(victimTask);
                ringBuffer.getFirst().numberOfTuplesPerTask.put(victimTask, ++counter);
            }else {
                ringBuffer.getFirst().numberOfTuplesPerTask.put(victimTask, new Integer(1));
            }
            if (!ringBuffer.getFirst().keyToTaskMapping.containsKey(key))
                ringBuffer.getFirst().keyToTaskMapping.put(key, new Integer(victimTask));
            ringBuffer.getFirst().outerRelationCardinality += 1L;
        }
    }

    public Fields getOutputSchema() {
        return outputSchema;
    }

    public void mergeState(List<Values> state) {
        //TODO: Leave blank for now. One dispatcher example
    }

    public List<Values> getState() {
        //TODO: Leave blank for now. One dispatcher example
        return null;
    }

    public long getStateSize() {
        return 0;
    }

    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {
        //TODO: Leave blank for now. One dispatcher and state is only the statistics of dispatched tuples
    }
}
