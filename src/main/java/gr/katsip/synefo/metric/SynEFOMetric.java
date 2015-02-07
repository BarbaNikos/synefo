package gr.katsip.synefo.metric;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.TopologyContext;

public class SynEFOMetric {

	private TaskStatistics usage;
	
	private transient AssignableMetric latencyMetric;
	
	private transient AssignableMetric throughputMetric;
	
	private transient AssignableMetric cpuMetric;
	
	private transient AssignableMetric memoryMetric;
	
	public SynEFOMetric() {
		usage = new TaskStatistics();
	}
	
	public void initMetrics(TopologyContext context, String taskName, String taskId) {
		latencyMetric = new AssignableMetric(usage.get_latency());
		throughputMetric = new AssignableMetric(usage.get_throughput());
		cpuMetric = new AssignableMetric(usage.get_cpu_load());
		memoryMetric = new AssignableMetric(usage.get_memory());
		
		context.registerMetric("latency_metric_" + taskName + ":" + taskId, latencyMetric, 1);
		context.registerMetric("throughput_" + taskName + ":" + taskId, throughputMetric, 1);
		context.registerMetric("cpu_load_metric_" + taskName + ":" + taskId, cpuMetric, 1);
		context.registerMetric("memory_load_metric" + taskName + ":" + taskId, memoryMetric, 1);
	}
	
	public void updateMetrics(long _thrpt_current_tuple_num) {
		updateCPU();
		updateMemory();
		updateThroughput(_thrpt_current_tuple_num);
		updateLatency();
	}
	
	public void updateCPU() {
		usage.update_cpu_load();
		cpuMetric.setValue(usage.get_cpu_load());
	}
	
	public void updateMemory() {
		usage.update_memory();
		memoryMetric.setValue(usage.get_memory());
	}
	
	public void updateThroughput(long _thrpt_current_tuple_num) {
		usage.update_throughput(_thrpt_current_tuple_num);
		throughputMetric.setValue(usage.get_throughput());
	}
	
	public void updateLatency() {
		usage.update_latency();
		latencyMetric.setValue(usage.get_latency());
	}
	
	public TaskStatistics getStats() {
		return usage;
	}
	
}
