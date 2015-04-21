package gr.katsip.synefo.metric;

import backtype.storm.metric.api.AssignableMetric;
import backtype.storm.task.TopologyContext;

public class SynefoMetric {

	private TaskStatistics usage;
	
	private transient AssignableMetric latencyMetric;
	
	private transient AssignableMetric throughputMetric;
	
	private transient AssignableMetric cpuMetric;
	
	private transient AssignableMetric memoryMetric;
	
	public SynefoMetric() {
		usage = new TaskStatistics();
	}
	
	public void initMetrics(TopologyContext context, String taskName, String taskId) {
		latencyMetric = new AssignableMetric(usage.getLatency());
		throughputMetric = new AssignableMetric(usage.getThroughput());
		cpuMetric = new AssignableMetric(usage.getCpuLoad());
		memoryMetric = new AssignableMetric(usage.getMemory());
		
		context.registerMetric("latency_metric_" + taskName + ":" + taskId, latencyMetric, 1);
		context.registerMetric("throughput_" + taskName + ":" + taskId, throughputMetric, 1);
		context.registerMetric("cpu_load_metric_" + taskName + ":" + taskId, cpuMetric, 1);
		context.registerMetric("memory_load_metric" + taskName + ":" + taskId, memoryMetric, 1);
	}
	
	public void updateMetrics() {
		updateCPU();
		updateMemory();
		updateThroughput();
		updateLatency();
	}
	
	public void updateCPU() {
		usage.updateCpuLoad();
		cpuMetric.setValue(usage.getCpuLoad());
	}
	
	public void updateMemory() {
		usage.updateMemory();
		memoryMetric.setValue(usage.getMemory());
	}
	
	public void updateThroughput() {
		usage.updateThroughput();
		throughputMetric.setValue(usage.getThroughput());
	}
	
	public void updateLatency() {
		usage.updateLatency();
		latencyMetric.setValue(usage.getLatency());
	}
	
	public TaskStatistics getStats() {
		return usage;
	}
	
}
