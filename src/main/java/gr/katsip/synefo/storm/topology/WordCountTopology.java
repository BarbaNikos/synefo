package gr.katsip.synefo.storm.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordCountTopology {
	
	public static class TestWordSpout extends BaseRichSpout {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6198546189871230979L;
		
		boolean _isDistributed;
	    SpoutOutputCollector _collector;
	    
	    public TestWordSpout() {
	    	this(true);
	    }
	    
	    public TestWordSpout(boolean isDistributed) {
	        _isDistributed = isDistributed;
	    }
	        
	    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
	        _collector = collector;
	    }
	    
	    public void close() {
	        
	    }
	        
	    public void nextTuple() {
	        Utils.sleep(10);
	        final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
	        final Random rand = new Random();
	        final String word = words[rand.nextInt(words.length)];
	        _collector.emit(new Values(word));
	    }
	    
	    public void ack(Object msgId) {

	    }

	    public void fail(Object msgId) {
	        
	    }
	    
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("word"));
	    }

	    @Override
	    public Map<String, Object> getComponentConfiguration() {
	        if(!_isDistributed) {
	            Map<String, Object> ret = new HashMap<String, Object>();
	            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
	            return ret;
	        } else {
	            return null;
	        }
	    }    
	}
	
	public static class ExclamationBolt extends BaseRichBolt {
	    /**
		 * 
		 */
		private static final long serialVersionUID = -4704330343507226230L;
		OutputCollector _collector;
	    @Override
	    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
	      _collector = collector;
	    }
	    @Override
	    public void execute(Tuple tuple) {
	      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
	      _collector.ack(tuple);
	    }
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("word"));
	    }
	  }

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
	    builder.setSpout("word", new TestWordSpout(), 30);
	    builder.setBolt("exclaim1", new ExclamationBolt(), 10).shuffleGrouping("word");
	    builder.setBolt("exclaim2", new ExclamationBolt(), 10).shuffleGrouping("exclaim1");

	    Config conf = new Config();
	    conf.setDebug(false);
	    conf.setNumWorkers(8);
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
	    StormSubmitter.submitTopology("multi-core-word-count", conf, builder.createTopology());
	}
}
