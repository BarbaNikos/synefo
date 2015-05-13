package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractOperator;

public interface AbstractCrypefoOperator extends AbstractOperator {

	public List<Values> execute(TaskStatistics statistics, Fields fields, Values values);
	
	public void updateOperatorName(String operatorName);
	
}
