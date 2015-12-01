package gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated;

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
 * Created by katsip on 10/21/2015.
 */
public class CollocatedWindowEquiJoin implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(CollocatedWindowEquiJoin.class);

    private long windowSize;

    private long slide;

    private LinkedList<BasicCollocatedEquiWindow> ringBuffer;

    private int bufferSize;

    private Fields innerRelationSchema;

    private Fields outerRelationSchema;

    private String innerRelationJoinAttribute;

    private String outerRelationJoinAttribute;

    private long byteStateSize;

    private long mirrorBufferByteStateSize;

    private long innerRelationCardinality;

    private long outerRelationCardinality;

    private String innerRelation;

    private String outerRelation;

    private LinkedList<BasicCollocatedEquiWindow> mirrorBuffer;

    private List<String> migratedKeys;

    public CollocatedWindowEquiJoin(long windowSize, long slide, Fields innerRelationSchema,
                                    Fields outerRelationSchema,
                                    String innerRelationJoinAttribute,
                                    String outerRelationJoinAttribute,
                                    String innerRelation,
                                    String outerRelation) {
        this.windowSize = windowSize;
        this.slide = slide;
        this.ringBuffer = new LinkedList<>();
        this.bufferSize = (int) Math.ceil(this.windowSize / this.slide);
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.innerRelationJoinAttribute = innerRelationJoinAttribute;
        this.outerRelationJoinAttribute = outerRelationJoinAttribute;
        this.byteStateSize = 0L;
        this.mirrorBufferByteStateSize = 0L;
        innerRelationCardinality = 0L;
        outerRelationCardinality = 0L;
        this.innerRelation = innerRelation;
        this.outerRelation = outerRelation;
        migratedKeys = new ArrayList<>();
    }

    public void store(Long currentTimestamp, Fields schema, Values tuple) {
        String key = null;
        int fieldIndex = -1;
        if (schema.toList().toString().equals(innerRelationSchema.toList().toString())) {
            key = (String) tuple.get(innerRelationSchema.fieldIndex(innerRelationJoinAttribute));
            fieldIndex = innerRelationSchema.fieldIndex(innerRelationJoinAttribute);
        } else if (schema.toList().toString().equals(outerRelationSchema.toList().toString())) {
            key = (String) tuple.get(outerRelationSchema.fieldIndex(outerRelationJoinAttribute));
            fieldIndex = outerRelationSchema.fieldIndex(outerRelationJoinAttribute);
        }
        if (migratedKeys.size() > 0 && migratedKeys.indexOf(key) >= 0)
            return;
        if (schema.toList().toString().equals(innerRelationSchema.toList().toString())) {
            if (ringBuffer.size() == 0) {
                BasicCollocatedEquiWindow window = new BasicCollocatedEquiWindow();
                window.start = currentTimestamp;
                window.end = window.start + slide;
                ArrayList<Values> tuples = new ArrayList<>();
                tuples.add(tuple);
                window.innerRelation.put((String) tuple.get(fieldIndex), tuples);
                window.innerRelationCardinality = 1;
                innerRelationCardinality += 1;
                window.byteStateSize = tuple.toArray().toString().length();
                ringBuffer.addFirst(window);
                byteStateSize += (tuple.toArray().toString().length());
            }else if (ringBuffer.size() > 0 && ringBuffer.getFirst().start <= currentTimestamp &&
                    ringBuffer.getFirst().end >= currentTimestamp) {
                ArrayList<Values> tuples;
                if (ringBuffer.getFirst().innerRelation.containsKey(tuple.get(fieldIndex)))
                    tuples = ringBuffer.getFirst().innerRelation.get(tuple.get(fieldIndex));
                else
                    tuples = new ArrayList<>();
                tuples.add(tuple);
                ringBuffer.getFirst().innerRelation.put((String) tuple.get(fieldIndex), tuples);
                ringBuffer.getFirst().byteStateSize += (tuple.toArray().toString().length());
                ringBuffer.getFirst().innerRelationCardinality += 1;
                innerRelationCardinality += 1;
                byteStateSize += (tuple.toArray().toString().length());
            }else {
                if (ringBuffer.size() >= bufferSize) {
                    BasicCollocatedEquiWindow victimWindow = ringBuffer.removeLast();
                    byteStateSize -= (victimWindow.byteStateSize);
                    innerRelationCardinality -= (victimWindow.innerRelationCardinality);
                    outerRelationCardinality -= (victimWindow.outerRelationCardinality);
                }
                BasicCollocatedEquiWindow window = new BasicCollocatedEquiWindow();
                window.start = ringBuffer.getFirst().end + 1;
                window.end = window.start + slide;
                ArrayList<Values> tuples = new ArrayList<>();
                tuples.add(tuple);
                window.innerRelation.put((String) tuple.get(fieldIndex), tuples);
                window.byteStateSize = tuple.toArray().toString().length();
                window.innerRelationCardinality = 1;
                ringBuffer.addFirst(window);
                byteStateSize += (tuple.toArray().toString().length());
                innerRelationCardinality += 1;
            }
        }else if (schema.toList().toString().equals(outerRelationSchema.toList().toString())) {
            if (ringBuffer.size() == 0) {
                BasicCollocatedEquiWindow window = new BasicCollocatedEquiWindow();
                window.start = currentTimestamp;
                window.end = window.start + slide;
                ArrayList<Values> tuples = new ArrayList<>();
                tuples.add(tuple);
                window.outerRelation.put((String) tuple.get(fieldIndex), tuples);
                window.byteStateSize = tuple.toArray().toString().length();
                window.outerRelationCardinality = 1;
                ringBuffer.addFirst(window);
                byteStateSize += (tuple.toArray().toString().length());
                outerRelationCardinality += 1;
            }else if (ringBuffer.size() > 0 && ringBuffer.getFirst().start <= currentTimestamp &&
                    ringBuffer.getFirst().end >= currentTimestamp) {
                ArrayList<Values> tuples;
                if (ringBuffer.getFirst().outerRelation.containsKey(tuple.get(fieldIndex)))
                    tuples = ringBuffer.getFirst().outerRelation.get(tuple.get(fieldIndex));
                else
                    tuples = new ArrayList<>();
                tuples.add(tuple);
                ringBuffer.getFirst().outerRelation.put((String) tuple.get(fieldIndex), tuples);
                ringBuffer.getFirst().byteStateSize += (tuple.toArray().toString().length());
                ringBuffer.getFirst().outerRelationCardinality += 1;
                outerRelationCardinality += 1;
                byteStateSize += (tuple.toArray().toString().length());
            }else {
                if (ringBuffer.size() >= bufferSize) {
                    BasicCollocatedEquiWindow victimWindow = ringBuffer.removeLast();
                    byteStateSize -= (victimWindow.byteStateSize);
                    innerRelationCardinality -= (victimWindow.innerRelationCardinality);
                    outerRelationCardinality -= (victimWindow.outerRelationCardinality);
                }
                BasicCollocatedEquiWindow window = new BasicCollocatedEquiWindow();
                window.start = ringBuffer.getFirst().end + 1;
                window.end = window.start + slide;
                ArrayList<Values> tuples = new ArrayList<>();
                tuples.add(tuple);
                window.outerRelation.put((String) tuple.get(fieldIndex), tuples);
                window.byteStateSize = tuple.toArray().toString().length();
                window.outerRelationCardinality = 1;
                ringBuffer.addFirst(window);
                byteStateSize += (tuple.toArray().toString().length());
                outerRelationCardinality += 1;
            }
        }
    }

    public ArrayList<Values> join(Long currentTimestamp, Fields schema, Values tuple) {
        ArrayList<Values> result = new ArrayList<>();
        if (schema.toList().toString().equals(innerRelationSchema.toList().toString())) {
            String value = (String) tuple.get(innerRelationSchema.fieldIndex(innerRelationJoinAttribute));
            for (int i = 0; i < ringBuffer.size(); i++) {
                BasicCollocatedEquiWindow window = ringBuffer.get(i);
                if ((window.start + windowSize) > currentTimestamp) {
                    if (window.outerRelation.containsKey(value)) {
                        ArrayList<Values> outerTuples = window.outerRelation.get(value);
                        for (Values t : outerTuples) {
                            Values joinResult;
                            if (innerRelation.compareTo(outerRelation) <= 0) {
                                joinResult = new Values(tuple.toArray());
                                joinResult.addAll(t);
                            }else {
                                joinResult = new Values(t.toArray());
                                joinResult.addAll(tuple);
                            }
                            if (result.indexOf(joinResult) < 0)
                                result.add(joinResult);
                        }
                    }
                }else {
                    break;
                }
            }
        }else if (schema.toList().toString().equals(outerRelationSchema.toList().toString())) {
            String value = (String) tuple.get(outerRelationSchema.fieldIndex(outerRelationJoinAttribute));
            for (int i = 0; i < ringBuffer.size(); i++) {
                BasicCollocatedEquiWindow window = ringBuffer.get(i);
                if ((window.start + windowSize) > currentTimestamp) {
                    if (window.innerRelation.containsKey(value)) {
                        ArrayList<Values> innerTuples = window.innerRelation.get(value);
                        for (Values t : innerTuples) {
                            Values joinResult;
                            if (innerRelation.compareTo(outerRelation) <= 0) {
                                joinResult = new Values(t.toArray());
                                joinResult.addAll(tuple);
                            }else {
                                joinResult = new Values(tuple.toArray());
                                joinResult.addAll(t);
                            }
                            if (result.indexOf(joinResult) < 0)
                                result.add(joinResult);
                        }
                    }
                }else {
                    break;
                }
            }
        }
        return result;
    }

    public void mirrorBufferGarbageCollect(long timestamp) {
        while(mirrorBuffer.size() > 0) {
            BasicCollocatedEquiWindow window = mirrorBuffer.get(0);
            if ((window.start + windowSize) < timestamp) {
                mirrorBufferByteStateSize -= mirrorBuffer.get(0).byteStateSize;
                mirrorBuffer.removeFirst();
                continue;
            }else {
                break;
            }
        }
        if (mirrorBuffer.size() == 0) {
            migratedKeys.clear();
            mirrorBufferByteStateSize = 0;
        }
    }

    public ArrayList<Values> mirrorJoin(Long currentTimestamp, Fields schema, Values tuple) {
        mirrorBufferGarbageCollect(currentTimestamp);
        ArrayList<Values> result = new ArrayList<>();
        if (schema.toList().toString().equals(innerRelationSchema.toList().toString())) {
            String value = (String) tuple.get(innerRelationSchema.fieldIndex(innerRelationJoinAttribute));
            for (int i = 0; i < mirrorBuffer.size(); i++) {
                BasicCollocatedEquiWindow window = mirrorBuffer.get(i);
                if ((window.start + windowSize) > currentTimestamp) {
                    if (window.outerRelation.containsKey(value)) {
                        ArrayList<Values> outerTuples = window.outerRelation.get(value);
                        for (Values t : outerTuples) {
                            Values joinResult;
                            if (innerRelation.compareTo(outerRelation) <= 0) {
                                joinResult = new Values(tuple.toArray());
                                joinResult.addAll(t);
                            }else {
                                joinResult = new Values(t.toArray());
                                joinResult.addAll(tuple);
                            }
                            if (result.indexOf(joinResult) < 0)
                                result.add(joinResult);
                        }
                    }
                }else {
                    break;
                }
            }
        }else if (schema.toList().toString().equals(outerRelationSchema.toList().toString())) {
            String value = (String) tuple.get(outerRelationSchema.fieldIndex(outerRelationJoinAttribute));
            for (int i = 0; i < mirrorBuffer.size(); i++) {
                BasicCollocatedEquiWindow window = mirrorBuffer.get(i);
                if ((window.start + windowSize) > currentTimestamp) {
                    if (window.innerRelation.containsKey(value)) {
                        ArrayList<Values> innerTuples = window.innerRelation.get(value);
                        for (Values t : innerTuples) {
                            Values joinResult;
                            if (innerRelation.compareTo(outerRelation) <= 0) {
                                joinResult = new Values(t.toArray());
                                joinResult.addAll(tuple);
                            }else {
                                joinResult = new Values(tuple.toArray());
                                joinResult.addAll(t);
                            }
                            if (result.indexOf(joinResult) < 0)
                                result.add(joinResult);
                        }
                    }
                }else {
                    break;
                }
            }
        }
        return result;
    }

    public long getStateSize() {
        return byteStateSize + mirrorBufferByteStateSize;
    }

    public void initializeBuffer() {
        ringBuffer.clear();
        byteStateSize = 0L;
        innerRelationCardinality = 0L;
        outerRelationCardinality = 0L;
        mirrorBuffer.clear();
    }

    public void initializeScaleOut(List<String> migratedKeys) {
        long timestamp = System.currentTimeMillis();
        /**
         * Just for experimental purposed to remove all the state
         * and see if load-shedding helps the situation
         */
        initializeBuffer();
        this.migratedKeys = migratedKeys;
        mirrorBuffer = new LinkedList<>();
        if (migratedKeys.size() == 0)
            return;
        for (int i = 0; i < ringBuffer.size(); i++) {
            BasicCollocatedEquiWindow window = ringBuffer.get(i);
            if ((window.start + this.windowSize) > timestamp) {
                BasicCollocatedEquiWindow mirrorWindow = new BasicCollocatedEquiWindow();
                mirrorWindow.start = window.start;
                mirrorWindow.end = window.end;
                mirrorWindow.byteStateSize = 0L;
                HashMap<String, ArrayList<Values>> temporaryMap = new HashMap<>();
                for (String key : window.innerRelation.keySet()) {
                    if (migratedKeys.indexOf(key) >= 0) {
                        ArrayList<Values> migratedTuples = window.innerRelation.get(key);
                        mirrorWindow.innerRelation.put(key, migratedTuples);
                        int size = migratedTuples.toString().length();
                        mirrorWindow.byteStateSize += size;
                        mirrorBufferByteStateSize += size;
                        byteStateSize -= size;
                        window.byteStateSize -= size;
                    }else {
                        temporaryMap.put(key, new ArrayList<>(window.innerRelation.get(key)));
                    }
                }
                //remove migratedTuples from inner relation
                if (mirrorWindow.innerRelation.size() > 0) {
                    window.innerRelation.clear();
                    window.innerRelation.putAll(temporaryMap);
                }
                temporaryMap = new HashMap<>();
                for (String key : window.outerRelation.keySet()) {
                    if (migratedKeys.indexOf(key) >= 0) {
                        ArrayList<Values> migratedTuples = window.outerRelation.get(key);
                        mirrorWindow.outerRelation.put(key, migratedTuples);
                        int size = migratedTuples.toString().length();
                        mirrorWindow.byteStateSize += size;
                        mirrorBufferByteStateSize += size;
                        byteStateSize -= size;
                        window.byteStateSize -= size;
                    }else {
                        temporaryMap.put(key, new ArrayList<>(window.outerRelation.get(key)));
                    }
                }
                //remove migratedTuples from outer relation
                if (mirrorWindow.outerRelation.size() > 0) {
                    window.outerRelation.clear();
                    window.outerRelation.putAll(temporaryMap);
                }
                //Add mirrorWindow to mirror-buffer
                mirrorBuffer.addLast(mirrorWindow);
            } else {
                break;
            }
        }
    }

    public void initializeScaleIn(List<String> migratedKeys) {
        long timestamp = System.currentTimeMillis();
        this.migratedKeys = migratedKeys;
        mirrorBuffer = new LinkedList<>();
        for (int i = 0; i < ringBuffer.size(); i++) {
            BasicCollocatedEquiWindow window = ringBuffer.get(i);
            if ((window.start + windowSize) > timestamp) {
                BasicCollocatedEquiWindow mirrorWindow = new BasicCollocatedEquiWindow();
                mirrorWindow.start = window.start;
                mirrorWindow.end = window.end;
                HashMap<String, ArrayList<Values>> temporaryMap = new HashMap<>();
                for (String key : window.innerRelation.keySet()) {
                    if (migratedKeys.indexOf(key) >= 0) {
                        ArrayList<Values> migratedTuples = window.innerRelation.get(key);
                        mirrorWindow.innerRelation.put(key, migratedTuples);
                        int size = migratedTuples.toString().length();
                        mirrorWindow.byteStateSize += size;
                        mirrorBufferByteStateSize += size;
                        byteStateSize -= size;
                        window.byteStateSize -= size;
                    }else {
                        temporaryMap.put(key, new ArrayList<Values>(window.innerRelation.get(key)));
                    }
                }
                //remove migratedTuples from inner relation
                if (mirrorWindow.innerRelation.size() > 0) {
                    window.innerRelation.clear();
                    window.innerRelation.putAll(temporaryMap);
                }
                temporaryMap = new HashMap<>();
                for (String key : window.outerRelation.keySet()) {
                    if (migratedKeys.indexOf(key) >= 0) {
                        ArrayList<Values> migratedTuples = window.outerRelation.get(key);
                        mirrorWindow.outerRelation.put(key, migratedTuples);
                        int size = migratedTuples.toString().length();
                        mirrorWindow.byteStateSize += size;
                        mirrorBufferByteStateSize += size;
                        byteStateSize -= size;
                        window.byteStateSize -= size;
                    }else {
                        temporaryMap.put(key, new ArrayList<Values>(window.outerRelation.get(key)));
                    }
                }
                //remove migratedTuples from outer relation
                if (mirrorWindow.outerRelation.size() > 0) {
                    window.outerRelation.clear();
                    window.outerRelation.putAll(temporaryMap);
                }
                //Add mirrorWindow to mirror-buffer
                mirrorBuffer.addLast(mirrorWindow);
            }else {
                break;
            }
        }
    }

    public long getNumberOfTuples() { return innerRelationCardinality + outerRelationCardinality; }
}
