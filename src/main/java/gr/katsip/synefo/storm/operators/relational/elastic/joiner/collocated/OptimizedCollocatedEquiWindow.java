package gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by katsip on 12/2/2015.
 */
public class OptimizedCollocatedEquiWindow implements Serializable {

    public Long start;

    public Long end;

    public HashMap<String, HashMap<String, LinkedList<Values>>> tupleIndex;

    public long byteStateSize;

    public long innerRelationCardinality;

    public long outerRelationCardinality;

    public OptimizedCollocatedEquiWindow() {
        start = 0L;
        end = 0L;
        tupleIndex = new HashMap<>();
        byteStateSize = 0L;
        innerRelationCardinality = 0L;
        outerRelationCardinality = 0L;
    }

}
