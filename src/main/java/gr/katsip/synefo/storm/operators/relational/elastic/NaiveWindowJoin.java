package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.*;

/**
 * Created by katsip on 9/10/2015.
 */
public class NaiveWindowJoin implements Serializable {

    private Integer windowSize;

    private Integer slide;

    private TreeMap<Long, Values> tuples;

    private HashMap<String, List<Long>> hashIndex;

    private Integer tupleCounter;

    private Fields tupleSchema;

    private String joinAttribute;

    private Long stateSizeInBytes;

    private String storedRelation;

    private String otherRelation;

    private Long windowStartTimestamp;

    private Long windowEndTimestamp;

    public NaiveWindowJoin(int windowSize, int slide, Fields tupleSchema, String joinAttribute,
                           String storedRelation, String otherRelation) {
        this.windowSize = windowSize;
        this.slide = slide;
        this.tupleSchema = new Fields(tupleSchema.toList());
        this.joinAttribute = joinAttribute;
        this.storedRelation = storedRelation;
        this.otherRelation = otherRelation;
        this.stateSizeInBytes = 0L;
        this.tupleCounter = 0;
        tuples = new TreeMap<>();
        hashIndex = new HashMap<>();
        windowStartTimestamp = -1L;
        windowEndTimestamp = -1L;
    }

    public void insert(Long currentTimestamp, Values tuple) {
        /**
         * Need to slide
         */
        if(windowStartTimestamp != -1L && currentTimestamp >= (windowStartTimestamp + (slide * 1000L))) {
            slide();
        }
        if(windowEndTimestamp == -1L && windowStartTimestamp == -1L) {
            windowStartTimestamp = currentTimestamp;
            windowEndTimestamp = currentTimestamp + (windowSize * 1000L);
        }
        tuples.put(currentTimestamp, tuple);
        List<Long> bucket = null;
        if(hashIndex.containsKey(tuple.get(tupleSchema.fieldIndex(joinAttribute)))) {
            bucket = hashIndex.get(tuple.get(tupleSchema.fieldIndex(joinAttribute)));
        }else {
            bucket = new ArrayList<>();
        }
        bucket.add(currentTimestamp);
        hashIndex.put((String) tuple.get(tupleSchema.fieldIndex(joinAttribute)), bucket);
        tupleCounter += 1;
        stateSizeInBytes += tuple.toArray().toString().length();
    }

    public List<Values> join(Long currentTimestamp, Values tuple, Fields otherSchema, String otherRelationJoinAttribute) {
        if(windowStartTimestamp != -1L && currentTimestamp >= (windowStartTimestamp + (slide * 1000L))) {
            slide();
        }
        if(windowEndTimestamp == -1L && windowStartTimestamp == -1L) {
            windowStartTimestamp = currentTimestamp;
            windowEndTimestamp = currentTimestamp + (windowSize * 1000L);
        }
        List<Long> bucket = null;
        if(hashIndex.containsKey(tuple.get(otherSchema.fieldIndex(otherRelationJoinAttribute)))) {
            bucket = hashIndex.get(tuple.get(otherSchema.fieldIndex(otherRelationJoinAttribute)));
        }
        if(bucket != null && bucket.size() > 0) {
            List<Values> result = new ArrayList<>();
            for(Long timestamp : bucket) {
                Values storedTuple = tuples.get(timestamp);
                Values joinedTuple = null;
                if(storedRelation.compareTo(otherRelation) <= 0) {
                    joinedTuple = new Values(storedTuple.toArray());
                    joinedTuple.addAll(tuple);
                }else {
                    joinedTuple = new Values(tuple.toArray());
                    joinedTuple.addAll(storedTuple);
                }
                if(result.indexOf(joinedTuple) == -1)
                    result.add(joinedTuple);
            }
            return result;
        }
        return new ArrayList<Values>();
    }

    private void slide() {
        windowStartTimestamp += slide;
        Iterator timestampIterator = tuples.keySet().iterator();
        while(timestampIterator.hasNext()) {
            Long tupleTimestamp = (Long) timestampIterator.next();
            Values tuple = tuples.get(tupleTimestamp);
            if(tupleTimestamp < windowStartTimestamp) {
                String value = (String) tuple.get(this.tupleSchema.fieldIndex(joinAttribute));
                List<Long> bucket = hashIndex.get(value);
                bucket.remove(bucket.indexOf(tupleTimestamp));
                tupleCounter -= 1;
                stateSizeInBytes -= tuple.toArray().toString().length();
                hashIndex.put(value, bucket);
            }else {
                break;
            }
        }
        windowEndTimestamp += slide;
    }

    private void cleanup() {
        tuples.clear();
        hashIndex.clear();
        stateSizeInBytes = 0L;
        tupleCounter = 0;
    }

    public long getStateSize() {
        return stateSizeInBytes;
    }

}
