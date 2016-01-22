package gr.katsip.synefo.storm.operators.joiner.collocated;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by katsip on 1/22/2016.
 */
public class CollocatedWindowGroupByCount implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(CollocatedGroupByCounter.class);

    private long windowSize;

    private long slide;

    private LinkedList<OptimizedCollocatedEquiWindow> ringBuffer;

    private int bufferSize;

    private Fields relationSchema;

    private String groupbyAttribute;

    public long byteStateSize;

    public long mirrorBufferByteStateSize;

    public long cardinality;

    private String relation;

    private LinkedList<OptimizedCollocatedEquiWindow> mirrorBuffer;

    private List<String> migratedKeys;

    public CollocatedWindowGroupByCount(long windowSize, long slide, Fields relationSchema,
                                        String groupbyAttribute, String relation) {
        this.windowSize = windowSize;
        this.slide = slide;
        this.ringBuffer = new LinkedList<>();
        this.bufferSize = (int) Math.ceil(this.windowSize / this.slide);
        this.relationSchema = new Fields(relationSchema.toList());
        this.groupbyAttribute = groupbyAttribute;
        this.byteStateSize = 0L;
        this.mirrorBufferByteStateSize = 0L;
        cardinality = 0L;
        this.relation = relation;
        migratedKeys = new ArrayList<>();
    }

    public void store(Long timestamp, Fields schema, Values tuple) {
        String key = (String) tuple.get(relationSchema.fieldIndex(groupbyAttribute));
        boolean addedTuple = false;
        Values groupByTuple = new Values();
        groupByTuple.add(key);
        if (migratedKeys.size() > 0 && migratedKeys.indexOf(key) > 0)
            return;
        if (ringBuffer.size() == 0) {
            OptimizedCollocatedEquiWindow window = new OptimizedCollocatedEquiWindow();
            window.start = timestamp;
            window.end = window.start + slide;
            LinkedList<Values> relation = new LinkedList<>();
            groupByTuple.add(new Integer(1));
            relation.add(groupByTuple);
            HashMap<String, LinkedList<Values>> keyIndex = new HashMap<>();
            keyIndex.put(this.relation, relation);
            window.tupleIndex.put(key, keyIndex);
            window.byteStateSize = groupByTuple.toArray().toString().getBytes().length;
            byteStateSize += window.byteStateSize;
            window.innerRelationCardinality = 1;
            cardinality++;
            ringBuffer.addFirst(window);
        } else {
            if (ringBuffer.getFirst().start <= timestamp && ringBuffer.getFirst().end >= timestamp) {
                HashMap<String, LinkedList<Values>> tupleIndex;
                if (ringBuffer.getFirst().tupleIndex.containsKey(key)) {
                    tupleIndex = ringBuffer.getFirst().tupleIndex.get(key);
                    groupByTuple = tupleIndex.get(relation).get(0);
                    groupByTuple.set(1, new Integer((Integer) groupByTuple.get(1) + 1));
                    tupleIndex.get(relation).set(0, groupByTuple);
                    ringBuffer.getFirst().tupleIndex.put(key, tupleIndex);
                    ringBuffer.getFirst().innerRelationCardinality += 1;
                    cardinality++;
                } else {
                    groupByTuple.add(new Integer(1));
                    tupleIndex = new HashMap<>();
                    LinkedList<Values> relation = new LinkedList<>();
                    relation.add(groupByTuple);
                    tupleIndex.put(this.relation, relation);
                    ringBuffer.getFirst().tupleIndex.put(key, tupleIndex);
                    ringBuffer.getFirst().byteStateSize += groupByTuple.toArray().toString().getBytes().length;
                    byteStateSize += groupByTuple.toArray().toString().getBytes().length;
                    ringBuffer.getFirst().innerRelationCardinality += 1;
                    cardinality++;
                }
            } else {
                if (ringBuffer.size() >= bufferSize) {
                    OptimizedCollocatedEquiWindow victim = ringBuffer.removeLast();
                    byteStateSize -= victim.byteStateSize;
                    cardinality -= victim.innerRelationCardinality;
                }
                OptimizedCollocatedEquiWindow window = new OptimizedCollocatedEquiWindow();
                window.start = ringBuffer.getFirst().end + 1;
                window.end = window.start + slide;
                LinkedList<Values> relation = new LinkedList<>();
                groupByTuple.add(new Integer(1));
                relation.add(groupByTuple);
                HashMap<String, LinkedList<Values>> keyIndex = new HashMap<>();
                window.innerRelationCardinality = 1;
                cardinality++;
                keyIndex.put(this.relation, relation);
                window.tupleIndex.put(key, keyIndex);
                window.byteStateSize = groupByTuple.toArray().toString().getBytes().length;
                byteStateSize += window.byteStateSize;
                ringBuffer.addFirst(window);
            }
        }
    }

    public ArrayList<Values> groupbyCount(Long timestamp, Fields schema, Values tuple) {
        return groupbyCount(timestamp, ringBuffer, schema, tuple);
    }

    public ArrayList<Values> groupbyCount(Long timestamp, LinkedList<OptimizedCollocatedEquiWindow> buffer,
                                          Fields schema, Values tuple) {
        ArrayList<Values> result = new ArrayList<>();
        String key = (String) tuple.get(relationSchema.fieldIndex(groupbyAttribute));
        int totalCount = 0;
        for (int i = 0; i < buffer.size(); i++) {
            OptimizedCollocatedEquiWindow window = buffer.get(i);
            if ((window.start + windowSize) > timestamp) {
                if (window.tupleIndex.containsKey(key)) {
                    LinkedList<Values> tuples = window.tupleIndex.get(key).get(this.relation);
                    totalCount += (Integer) tuples.get(0).get(1);
                }
            } else {
                break;
            }
        }
        if (totalCount > 0) {
            Values countTuple = new Values();
            countTuple.add(key);
            countTuple.add(new Integer(totalCount));
            result.add(countTuple);
        }
        return result;
    }

}
