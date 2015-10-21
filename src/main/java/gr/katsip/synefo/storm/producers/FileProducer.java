package gr.katsip.synefo.storm.producers;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;

/**
 * Created by katsip on 10/14/2015.
 */
public interface FileProducer {
    public void setSchema(Fields fields);

    public Fields getSchema();

    public int nextTuple(SpoutOutputCollector spoutOutputCollector, Integer taskIdentifier,
                         HashMap<Values, Long> tupleStatistics);

    public void init();
}
