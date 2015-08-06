package gr.katsip.synefo.storm.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
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
			final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
			final Random rand = new Random();
			final String word = words[rand.nextInt(words.length)];
			Values tuple = new Values();
			tuple.add((new Long(System.currentTimeMillis())).toString());
			tuple.add(word);
			_collector.emit(tuple);
		}

		public void ack(Object msgId) {

		}

		public void fail(Object msgId) {

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			String[] schema = { "timestamp", "word" };
			declarer.declare(new Fields(schema));
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

	public static class IntermediateBolt extends BaseRichBolt {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = -3473126381154414151L;
		
		private transient ReducedMetric aggregateLatencyMetric;
		
		private transient AssignableMetric latencyMetric;
		
		private Long latestTimestamp;
		
		OutputCollector _collector;
		
		private boolean isTickTuple(Tuple tuple) {
			String sourceComponent = tuple.getSourceComponent();
			String sourceStreamId = tuple.getSourceStreamId();
			return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID) && 
					sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
		}
		
		private void initMetrics(TopologyContext context) {
			latencyMetric = new AssignableMetric(null);
			aggregateLatencyMetric = new ReducedMetric(new MeanReducer());
			context.registerMetric("latency", latencyMetric, 5);
			context.registerMetric("aggr-latency", aggregateLatencyMetric, 5);
		}

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			latestTimestamp = -1L;
			initMetrics(context);
		}
		@Override
		public void execute(Tuple tuple) {
			Long currentTimestamp = System.currentTimeMillis();
			if(isTickTuple(tuple)) {
				if(latestTimestamp == -1L) {
					latestTimestamp = currentTimestamp;
				}else {
					Long timeDifference = currentTimestamp - latestTimestamp;
					if(timeDifference >= 5000) {
						aggregateLatencyMetric.update(timeDifference);
						latencyMetric.setValue(timeDifference);
					}
				}
				_collector.ack(tuple);
			}else {
				Values v = new Values();
				v.add(tuple.getString(0));
				v.add(tuple.getString(1));
				_collector.emit(v);
				_collector.ack(tuple);
			}
		}
		
		@Override
		public Map<String, Object> getComponentConfiguration() {
			Config conf = new Config();
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
			return conf;
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			String[] schema = { "timestamp", "word" };
			declarer.declare(new Fields(schema));
		}
	}
	
	public static class SinkBolt extends BaseRichBolt {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = -3064223929882288608L;

		Logger logger = LoggerFactory.getLogger(SinkBolt.class);
		
		OutputCollector _collector;

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}
		@Override
		public void execute(Tuple tuple) {
			Long currentTimestamp = System.currentTimeMillis();
			Long tupleTimestamp = Long.parseLong(tuple.getString(0));
			logger.info("latency: " + (currentTimestamp - tupleTimestamp));
			_collector.ack(tuple);
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			String[] schema = { "timestamp", "word" };
			declarer.declare(new Fields(schema));
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
		builder.setSpout("word", new TestWordSpout(), 4).setMaxSpoutPending(300);
		builder.setBolt("step1", new IntermediateBolt(), 4).shuffleGrouping("word");
		builder.setBolt("step2", new IntermediateBolt(), 4).shuffleGrouping("step1");
		builder.setBolt("step3", new IntermediateBolt(), 4).shuffleGrouping("step2");
		builder.setBolt("step4", new IntermediateBolt(), 4).shuffleGrouping("step3");
		builder.setBolt("sink", new SinkBolt(), 4).shuffleGrouping("step4");

		Config conf = new Config();
		conf.setDebug(false);
		conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);
//		conf.setNumWorkers(20);
		conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, 
				"-Xmx4096m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true");
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		StormSubmitter.submitTopology("benchmark-topology", conf, builder.createTopology());
	}
}
