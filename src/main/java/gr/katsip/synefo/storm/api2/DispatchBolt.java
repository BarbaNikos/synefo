package gr.katsip.synefo.storm.api2;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import gr.katsip.synefo.storm.operators.AbstractJoinOperator;

/**
 * Created by katsip on 9/2/2015.
 */
public class DispatchBolt extends BaseBasicBolt {

    private OutputCollector collector;

    private AbstractJoinOperator operator;

    public DispatchBolt(AbstractJoinOperator operator) {
        this.operator = operator;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
