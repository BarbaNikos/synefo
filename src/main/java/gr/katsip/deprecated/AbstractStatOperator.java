package gr.katsip.deprecated;

import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;

/**
 * @deprecated
 */
public interface AbstractStatOperator extends AbstractOperator {

	public List<Values> execute(TaskStatistics statistics, Fields fields, Values values);
	
	public void updateOperatorName(String operatorName);
	
	public void reportStatisticBeforeScaleOut();
	
	public void reportStatisticBeforeScaleIn();
	
}
