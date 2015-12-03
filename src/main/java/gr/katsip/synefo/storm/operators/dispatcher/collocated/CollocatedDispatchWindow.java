package gr.katsip.synefo.storm.operators.dispatcher.collocated;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Nick R. Katsipoulakis on 10/21/2015.
 */
public class CollocatedDispatchWindow implements Serializable {

    public long start;

    public long end;

    public long stateSize = 0L;

    public HashMap<String, Integer> keyIndex;

    public HashMap<Integer, Integer> numberOfTuplesPerTask;

    public HashMap<String, Integer> keyToTaskMapping;

    public Long innerRelationCardinality;

    public Long outerRelationCardinality;

    public CollocatedDispatchWindow() {
        start = 0L;
        end = 0L;
        stateSize = 0L;
        innerRelationCardinality = 0L;
        outerRelationCardinality = 0L;
        numberOfTuplesPerTask = new HashMap<>();
        keyToTaskMapping = new HashMap<>();
        keyIndex = new HashMap<>();
    }

}
