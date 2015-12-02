package gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by katsip on 12/2/2015.
 */
public class OptimizedWindowEquiJoin {

    private long windowSize;

    private long slide;

    private LinkedList<OptimizedCollocatedEquiWindow> ringBuffer;

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

    private LinkedList<OptimizedCollocatedEquiWindow> mirrorBuffer;

    private List<String> migratedKeys;

    public OptimizedWindowEquiJoin(long windowSize, long slide, Fields innerRelationSchema,
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

    public void store(Long timestamp, Fields schema, Values tuple) {
        String key, relationName;
        boolean addedTuple = false;
        if (schema.toList().toString().equals(innerRelationSchema.toList().toString())) {
            key = (String) tuple.get(innerRelationSchema.fieldIndex(innerRelationJoinAttribute));
            relationName = innerRelation;
        } else if (schema.toList().toString().equals(outerRelationSchema.toList().toString())) {
            key = (String) tuple.get(outerRelationSchema.fieldIndex(outerRelationJoinAttribute));
            relationName = outerRelation;
        } else {
            return;
        }
        if (migratedKeys.size() > 0 && migratedKeys.indexOf(key) > 0)
            return;
        if (ringBuffer.size() == 0) {
            OptimizedCollocatedEquiWindow window = new OptimizedCollocatedEquiWindow();
            window.start = timestamp;
            window.end = window.start + slide;
            LinkedList<Values> relation = new LinkedList<>();
            relation.add(tuple);
            HashMap<String, LinkedList<Values>> keyIndex = new HashMap<>();
            augmentStatistics(relationName, keyIndex, window);
            keyIndex.put(relationName, relation);
            window.tupleIndex.put(key, keyIndex);
            window.byteStateSize = tuple.toArray().toString().length();
            byteStateSize += tuple.toArray().toString().length();
            ringBuffer.addFirst(window);
        } else {
            if (ringBuffer.getFirst().start <= timestamp &&
                    ringBuffer.getFirst().end >= timestamp) {
                HashMap<String, LinkedList<Values>> tupleIndex;
                if (ringBuffer.getFirst().tupleIndex.containsKey(key)) {
                    tupleIndex = ringBuffer.getFirst().tupleIndex.get(key);
                    if (tupleIndex.get(relationName).indexOf(tuple) < 0) {
                        tupleIndex.get(relationName).add(tuple);
                        addedTuple = true;
                        ringBuffer.getFirst().tupleIndex.put(key, tupleIndex);
                    }
                } else {
                    tupleIndex = new HashMap<>();
                    LinkedList<Values> tuples = new LinkedList<>();
                    tuples.add(tuple);
                    addedTuple = true;
                    tupleIndex.put(relationName, tuples);
                    ringBuffer.getFirst().tupleIndex.put(key, tupleIndex);
                }
                if (relationName.equals(innerRelation)) {
                    if (addedTuple) {
                        ringBuffer.getFirst().byteStateSize += tuple.toArray().toString().length();
                        ringBuffer.getFirst().innerRelationCardinality += 1;
                        byteStateSize += tuple.toArray().toString().length();
                        innerRelationCardinality += 1;
                    }
                } else {
                    if (addedTuple) {
                        ringBuffer.getFirst().byteStateSize += tuple.toArray().toString().length();
                        ringBuffer.getFirst().outerRelationCardinality += 1;
                        byteStateSize += tuple.toArray().toString().length();
                        outerRelationCardinality += 1;
                    }
                }
            } else {
                if (ringBuffer.size() >= bufferSize) {
                    OptimizedCollocatedEquiWindow victim = ringBuffer.removeLast();
                    byteStateSize -= victim.byteStateSize;
                    innerRelationCardinality -= victim.innerRelationCardinality;
                    outerRelationCardinality -= victim.outerRelationCardinality;
                }
                OptimizedCollocatedEquiWindow window = new OptimizedCollocatedEquiWindow();
                window.start = ringBuffer.getFirst().end + 1;
                window.end = window.start + slide;
                LinkedList<Values> relation = new LinkedList<>();
                relation.add(tuple);
                HashMap<String, LinkedList<Values>> keyIndex = new HashMap<>();
                augmentStatistics(relationName, keyIndex, window);
                keyIndex.put(relationName, relation);
                window.tupleIndex.put(key, keyIndex);
                window.byteStateSize = tuple.toArray().toString().length();
                byteStateSize += tuple.toArray().toString().length();
                ringBuffer.addFirst(window);
            }
        }
    }

    private void augmentStatistics(String relationName, HashMap<String, LinkedList<Values>> keyIndex,
                                   OptimizedCollocatedEquiWindow window) {
        if (relationName.equals(innerRelation)) {
            keyIndex.put(outerRelation, new LinkedList<Values>());
            window.innerRelationCardinality = 1;
            innerRelationCardinality += 1;
            window.outerRelationCardinality = 0;
        } else {
            keyIndex.put(innerRelation, new LinkedList<Values>());
            window.outerRelationCardinality = 1;
            outerRelationCardinality += 1;
            window.innerRelationCardinality = 0;
        }
    }

    public ArrayList<Values> join(Long timestamp, Fields schema, Values tuple) {
        ArrayList<Values> result = join(timestamp, ringBuffer, schema, tuple);
        if (migratedKeys.size() > 0) {
            mirrorBufferGarbageCollect(timestamp);
            ArrayList<Values> mirrorResult = join(timestamp, mirrorBuffer, schema, tuple);
            mirrorResult.removeAll(result);
            result.addAll(mirrorResult);
        }
        return result;
    }

    public void mirrorBufferGarbageCollect(long timestamp) {
        while (mirrorBuffer.size() > 0) {
            OptimizedCollocatedEquiWindow window = mirrorBuffer.getFirst();
            if ((window.start + windowSize) < timestamp) {
                mirrorBufferByteStateSize -= window.byteStateSize;
                mirrorBuffer.removeFirst();
            } else {
                break;
            }
        }
        if (mirrorBuffer.isEmpty()) {
            migratedKeys.clear();
            mirrorBufferByteStateSize = 0;
        }
    }

    public ArrayList<Values> join(Long timestamp, LinkedList<OptimizedCollocatedEquiWindow> buffer,
                                  Fields schema, Values tuple) {
        ArrayList<Values> result = new ArrayList<>();
        String key, joinRelationName;
        /**
         * joinRelationName gets the name of the relation that is not the same as the received tuple.
         */
        if (schema.toList().toString().equals(innerRelationSchema.toList().toString())) {
            key = (String) tuple.get(innerRelationSchema.fieldIndex(innerRelationJoinAttribute));
            joinRelationName = outerRelation;
        } else if (schema.toList().toString().equals(outerRelationSchema.toList().toString())) {
            key = (String) tuple.get(outerRelationSchema.fieldIndex(outerRelationJoinAttribute));
            joinRelationName = innerRelation;
        } else {
            return result;
        }
        for (int i = 0; i < buffer.size(); i++) {
            OptimizedCollocatedEquiWindow window = buffer.get(i);
            if ((window.start + windowSize) > timestamp) {
                if (window.tupleIndex.containsKey(key)) {
                    LinkedList<Values> tuples = window.tupleIndex.get(key).get(joinRelationName);
                    for (Values t : tuples) {
                        Values joinResult;
                        if (innerRelation.compareTo(outerRelation) <= 0) {
                            joinResult = new Values(tuple.toArray());
                            joinResult.addAll(t);
                        } else {
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
        return result;
    }

    public long getStateSize() {
        return byteStateSize + mirrorBufferByteStateSize;
    }

    public void initBuffer() {
        ringBuffer.clear();
        if (mirrorBuffer != null)
            mirrorBuffer.clear();
        byteStateSize = 0L;
        innerRelationCardinality = 0L;
        outerRelationCardinality = 0L;
    }

    public void initScaleBuffer(Long timestamp, List<String> migratedKeys) {
        /**
         * Just for experimental purposes: remove all the state and
         * see if load-shedding helps the situation
         */
        this.migratedKeys = migratedKeys;
        if (migratedKeys.size() == 0)
            return;
        else {
            for (int i = 0; i < ringBuffer.size(); i++) {
                OptimizedCollocatedEquiWindow window = ringBuffer.get(i);
                if ((window.start + this.windowSize) > timestamp) {
                    OptimizedCollocatedEquiWindow mirrorWindow = new OptimizedCollocatedEquiWindow();
                    mirrorWindow.start = window.start;
                    mirrorWindow.end = window.end;
                    Set<String> keySet = new LinkedHashSet<>(window.tupleIndex.keySet());
                    for (String key : keySet) {
                        if (migratedKeys.indexOf(key) >= 0) {
                            HashMap<String, LinkedList<Values>> tupleIndex = window.tupleIndex.remove(key);
                            mirrorWindow.tupleIndex.put(key, tupleIndex);
                            /**
                             * TODO: Check the validity of the statistics (the cardinality of a relation increments when a new tuple is added)
                             */
                            mirrorWindow.innerRelationCardinality += mirrorWindow.tupleIndex.get(key).get(innerRelation).size();
                            mirrorWindow.outerRelationCardinality += mirrorWindow.tupleIndex.get(key).get(outerRelation).size();
                            window.innerRelationCardinality -= mirrorWindow.tupleIndex.get(key).get(innerRelation).size();
                            innerRelationCardinality -= mirrorWindow.tupleIndex.get(key).get(innerRelation).size();
                            window.outerRelationCardinality -= mirrorWindow.tupleIndex.get(key).get(outerRelation).size();
                            outerRelationCardinality -= mirrorWindow.tupleIndex.get(key).get(outerRelation).size();
                            long sizeDiff = (mirrorWindow.tupleIndex.get(key).get(innerRelation).toArray().toString().length() +
                                    mirrorWindow.tupleIndex.get(key).get(outerRelation).toArray().toString().length());
                            mirrorBufferByteStateSize += sizeDiff;
                            mirrorWindow.byteStateSize += sizeDiff;
                            window.byteStateSize -= sizeDiff;
                            byteStateSize -= sizeDiff;
                        }
                    }
                    mirrorBuffer.addFirst(mirrorWindow);
                } else {
                    break;
                }
            }
        }
    }

    public long getNumberOfTuples() {
        return innerRelationCardinality + outerRelationCardinality;
    }
}
