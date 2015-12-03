package gr.katsip.synefo.storm.operators.joiner.collocated;

import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by katsip on 10/21/2015.
 */
public class BasicCollocatedEquiWindow implements Serializable {

    public Long start;

    public Long end;

    public HashMap<String, ArrayList<Values>> innerRelation;

    public HashMap<String, ArrayList<Values>> outerRelation;

    public long byteStateSize;

    public long innerRelationCardinality;

    public long outerRelationCardinality;

    public BasicCollocatedEquiWindow() {
        start = 0L;
        end = 0L;
        innerRelation = new HashMap<>();
        outerRelation = new HashMap<>();
        byteStateSize = 0L;
        innerRelationCardinality = 0L;
        outerRelationCardinality = 0L;
    }
}
