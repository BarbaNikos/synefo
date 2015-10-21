package gr.katsip.deprecated.deprecated;

import gr.katsip.synefo.metric.TaskStatistics;
import backtype.storm.tuple.Values;

/**
 * @deprecated
 */
public interface AbstractStatTupleProducer extends AbstractTupleProducer {

	public Values nextTuple(TaskStatistics statistics);
	
	public void updateProducerName(String producerName);
	
}
