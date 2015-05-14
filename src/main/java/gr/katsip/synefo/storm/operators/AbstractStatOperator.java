package gr.katsip.synefo.storm.operators;

import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;

public interface AbstractStatOperator extends AbstractOperator {

	public List<Values> execute(TaskStatistics statistics, Fields fields, Values values);
	
	public void updateOperatorName(String operatorName);
	
}
