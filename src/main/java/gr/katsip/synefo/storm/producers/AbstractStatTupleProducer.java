package gr.katsip.synefo.storm.producers;

import gr.katsip.synefo.metric.TaskStatistics;
import backtype.storm.tuple.Values;

public interface AbstractStatTupleProducer extends AbstractTupleProducer {

	public Values nextTuple(TaskStatistics statistics);
	
	public void updateProducerName(String producerName);
	
}
