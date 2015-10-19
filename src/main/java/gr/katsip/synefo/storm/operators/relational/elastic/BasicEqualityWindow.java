package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by katsip on 10/19/2015.
 */
public class BasicEqualityWindow implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 687583325823717593L;

    public Long startingTimestamp;

    public Long endingTimestamp;

    public HashMap<String, ArrayList<Values>> tuples;

    public long basicWindowStateSize;

    public long numberOfTuples;

    public BasicEqualityWindow() {
        basicWindowStateSize = 0L;
        numberOfTuples = 0L;
        tuples = new HashMap<>();
        startingTimestamp = 0L;
        endingTimestamp = 0L;
    }

}
