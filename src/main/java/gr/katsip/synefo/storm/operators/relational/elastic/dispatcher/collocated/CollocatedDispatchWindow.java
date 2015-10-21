package gr.katsip.synefo.storm.operators.relational.elastic.dispatcher.collocated;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Created by katsip on 10/21/2015.
 */
public class CollocatedDispatchWindow implements Serializable {

    public long start;

    public long end;

    public long stateSize = 0L;

    public HashMap<String, List<Integer>> outerRelationIndex;

    public HashMap<String, List<Integer>> innerRelationIndex;

    public long innerRelationCardinality;

    public long outerRelationCardinality;

    public CollocatedDispatchWindow() {
        start = 0L;
        end = 0L;
        stateSize = 0L;
        outerRelationIndex = new HashMap<>();
        innerRelationIndex = new HashMap<>();
        innerRelationCardinality = 0L;
        outerRelationCardinality = 0L;
    }

}
