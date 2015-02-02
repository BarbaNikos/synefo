package gr.katsip.synefo.metric;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.TopologyContext;

public class SynEFOMetric {

	private TaskStatistics _usage;
	
	private transient AssignableMetric _latencyMetric;
	
	private transient AssignableMetric _throughputMetric;
	
	private transient AssignableMetric _cpuMetric;
	
	private transient AssignableMetric _memoryMetric;
	
	public SynEFOMetric() {
		_usage = new TaskStatistics();
	}
	
	public void initMetrics(TopologyContext context, String taskName, String task_id) {
		_latencyMetric = new AssignableMetric(_usage.get_latency());
		_throughputMetric = new AssignableMetric(_usage.get_throughput());
		_cpuMetric = new AssignableMetric(_usage.get_cpu_load());
		_memoryMetric = new AssignableMetric(_usage.get_memory());
		
		context.registerMetric("latency_metric_" + taskName + ":" + task_id, _latencyMetric, 1);
		context.registerMetric("throughput_" + taskName + ":" + task_id, _throughputMetric, 1);
		context.registerMetric("cpu_load_metric_" + taskName + ":" + task_id, _cpuMetric, 1);
		context.registerMetric("memory_load_metric" + taskName + ":" + task_id, _memoryMetric, 1);
	}
	
	public void updateMetrics(long _thrpt_current_tuple_num) {
		updateCPU();
		updateMemory();
		updateThroughput(_thrpt_current_tuple_num);
		updateLatency();
	}
	
	public void updateCPU() {
		_usage.update_cpu_load();
		_cpuMetric.setValue(_usage.get_cpu_load());
	}
	
	public void updateMemory() {
		_usage.update_memory();
		_memoryMetric.setValue(_usage.get_memory());
	}
	
	public void updateThroughput(long _thrpt_current_tuple_num) {
		_usage.update_throughput(_thrpt_current_tuple_num);
		_throughputMetric.setValue(_usage.get_throughput());
	}
	
	public void updateLatency() {
		_usage.update_latency();
		_latencyMetric.setValue(_usage.get_latency());
	}
	
	public TaskStatistics getStats() {
		return _usage;
	}
	
}
