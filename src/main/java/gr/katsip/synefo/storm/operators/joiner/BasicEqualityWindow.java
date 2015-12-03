package gr.katsip.synefo.storm.operators.joiner;

import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by katsip on 10/19/2015.
 */
public class BasicEqualityWindow implements Serializable {

    public Long start;

    public Long end;

    public HashMap<String, ArrayList<Values>> tuples;

    public long basicWindowStateSize;

    public long numberOfTuples;

    public BasicEqualityWindow() {
        basicWindowStateSize = 0L;
        numberOfTuples = 0L;
        tuples = new HashMap<>();
        start = 0L;
        end = 0L;
    }

}
