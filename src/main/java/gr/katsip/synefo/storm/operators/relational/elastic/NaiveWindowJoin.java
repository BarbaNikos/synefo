package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by katsip on 9/10/2015.
 */
public class NaiveWindowJoin {

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
    }

    public void insert(Long currentTimestamp, Values tuple) {

    }

    public void join(Long currentTimestamp, Values tuple, Fields otherSchema, String otherRelationJoinAttribute) {

    }

    private void slide() {

    }

    private void cleanup() {
        tuples.clear();
        hashIndex.clear();
        stateSizeInBytes = 0L;
        tupleCounter = 0;
    }

}
