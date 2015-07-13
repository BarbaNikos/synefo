package gr.katsip.synefo.metric;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;

import com.sun.management.OperatingSystemMXBean;

public class TaskStatistics implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2552450405227774625L;

	private double selectivity;

	private long selectivitySamples;

	private long latency;
	
	private long operationalLatency;

	private double throughput;

	private long throughputSamples;

	private long throughputPreviousTimestamp;

	private long throughputPreviousTupleNumber;

	private LinkedList<Double> throughputSampleWindow;

	private Double runningThroughputWindowSum;

	private final int sampleWindowSize = 100;

	private LinkedList<Long> latencySampleWindow;
	
	private LinkedList<Long> operationalLatencySampleWindow;

	private Long runningLatencyWindowSum;
	
	private Long runningOperationalLatencyWindowSum;

	private double memory;
	
	private LinkedList<Double> memorySampleWindow;
	
	private Double runningMemoryWindowSum;

	private double inputRate;

	private long inputRateSamples;
	
	private LinkedList<Double> cpuSampleWindow;
	
	private Double runningCpuWindowSum;

	private double cpuLoad;
	
	private ControlBasedStatistics throughputSlope;
	
	public ControlBasedStatistics getThroughputSlope() {
		return throughputSlope;
	}

	public ControlBasedStatistics getLatencySlope() {
		return latencySlope;
	}

	private ControlBasedStatistics latencySlope;

	public TaskStatistics() {
		selectivity = 0.0;
		selectivitySamples = 0;
		latency = 0;
		operationalLatency = 0;
		throughput = 0;
		throughputSamples = 0;
		memory = 0.0;
		inputRate = 0.0;
		inputRateSamples = 0;
		cpuLoad = 0.0;
		throughputSampleWindow = new LinkedList<Double>();
		runningThroughputWindowSum = 0.0;
		latencySampleWindow = new LinkedList<Long>();
		operationalLatencySampleWindow = new LinkedList<Long>();
		runningLatencyWindowSum = 0L;
		runningOperationalLatencyWindowSum = 0L;
		cpuSampleWindow = new LinkedList<Double>();
		runningCpuWindowSum = 0.0;
		memorySampleWindow = new LinkedList<Double>();
		runningMemoryWindowSum = 0.0;
		throughputSlope = new ControlBasedStatistics();
		latencySlope = new ControlBasedStatistics();
	}

	public void updateSelectivity(double selectivity) {
		if(selectivitySamples == 0) {
			this.selectivity = selectivity;
			selectivitySamples += 1;
		}else {
			this.selectivity = this.selectivity + (selectivity - this.selectivity)/(selectivitySamples + 1);
			selectivitySamples += 1;
		}
	}

	public double getSelectivity() {
		return selectivity;
	}

	public void updateWindowThroughput() {
		if(throughputSamples == 0) {
			this.throughput = 0;
			runningThroughputWindowSum = 0.0;
			throughputPreviousTupleNumber = 1;
			throughputPreviousTimestamp = System.currentTimeMillis();
			throughputSamples += 1;
		}else {
			long currTimestamp = System.currentTimeMillis();
			long timeDelta = Math.abs(currTimestamp - throughputPreviousTimestamp);
			if(timeDelta >= 1000L) {
				throughput = throughputPreviousTupleNumber + 1;
				throughputPreviousTupleNumber = 0;
				throughputPreviousTimestamp = currTimestamp;
				throughputSamples += 1;
			}else {
				throughputPreviousTupleNumber += 1;
			}
		}
		if(throughputSampleWindow.size() >= sampleWindowSize) {
			Double removedValue = throughputSampleWindow.poll();
			runningThroughputWindowSum -= removedValue;
		}
		throughputSampleWindow.offer(throughput);
		runningThroughputWindowSum += throughput;
		throughputSlope.updateSlope(throughput);
	}

	public void updateWindowLatency(long latency) {
		this.latency = latency;
		if(latencySampleWindow.size() >= sampleWindowSize) {
			Long removedValue = latencySampleWindow.poll();
			runningLatencyWindowSum -= removedValue;
		}
		latencySampleWindow.offer(this.latency);
		runningLatencyWindowSum += this.latency;
		latencySlope.updateSlope(latency);
	}
	
	public void updateWindowOperationalLatency(long operationalLatency) {
		this.operationalLatency = operationalLatency;
		if(operationalLatencySampleWindow.size() >= sampleWindowSize) {
			Long removedValue = operationalLatencySampleWindow.poll();
			runningOperationalLatencyWindowSum -= removedValue;
		}
		operationalLatencySampleWindow.offer(this.operationalLatency);
		runningOperationalLatencyWindowSum += this.operationalLatency;
	}
	
	public void updateCpuLoad() {
		OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
				.getOperatingSystemMXBean();
		this.cpuLoad = bean.getSystemCpuLoad();
		if(cpuSampleWindow.size() >= sampleWindowSize) {
			Double removedValue = cpuSampleWindow.poll();
			runningCpuWindowSum -= removedValue;
		}
		cpuSampleWindow.offer(cpuLoad);
		runningCpuWindowSum += cpuLoad;
	}

	public double getWindowThroughput() {
		return throughputSampleWindow.size() > 0 ? (runningThroughputWindowSum / throughputSampleWindow.size()) : 0;
	}
	
	public long getWindowLatency() {
		return latencySampleWindow.size() > 0 ? (runningLatencyWindowSum / latencySampleWindow.size()) : 0;
	}
	
	public long getWindowOperationalLatency() {
		return operationalLatencySampleWindow.size() > 0 ? (runningOperationalLatencyWindowSum / operationalLatencySampleWindow.size()) : 0;
	}

	public double getCpuLoad() {
		return cpuSampleWindow.size() > 0 ? (runningCpuWindowSum / cpuSampleWindow.size()) : 0.0;
	}

	public void updateMemory() {
		Runtime runtime = Runtime.getRuntime();
		/**
		 * The formula below gives the percent of the maximum available memory to the JVM
		 */
		memory = (double) runtime.totalMemory() / runtime.maxMemory();
		if(memorySampleWindow.size() >= sampleWindowSize) {
			Double removedValue = memorySampleWindow.poll();
			runningMemoryWindowSum -= removedValue;
		}
		memorySampleWindow.offer(memory);
		runningMemoryWindowSum += memory;
	}

	public double getMemory() {
		return memorySampleWindow.size() > 0 ? (runningMemoryWindowSum / memorySampleWindow.size()) : 0.0;
	}

	public void updateInputRate(double input_rate) {
		if(inputRateSamples == 0) {
			this.inputRate = input_rate;
			inputRateSamples += 1;
		}else {
			this.inputRate = this.inputRate + (input_rate - this.inputRate)/(inputRateSamples + 1);
			inputRateSamples += 1;
		}
	}

	public double getInputRate() {
		return inputRate;
	}

	public void resetStatistics() {
		selectivity = 0.0;
		selectivitySamples = 0;
		latency = 0;
		operationalLatency = 0;
		throughput = 0;
		throughputSamples = 0;
		memory = 0.0;
		inputRate = 0.0;
		inputRateSamples = 0;
		cpuLoad = 0.0;
		throughputSampleWindow = new LinkedList<Double>();
		runningThroughputWindowSum = 0.0;
		latencySampleWindow = new LinkedList<Long>();
		operationalLatencySampleWindow = new LinkedList<Long>();
		runningLatencyWindowSum = 0L;
		runningOperationalLatencyWindowSum = 0L;
		cpuSampleWindow = new LinkedList<Double>();
		runningCpuWindowSum = 0.0;
		memorySampleWindow = new LinkedList<Double>();
		runningMemoryWindowSum = 0.0;
		throughputSlope = new ControlBasedStatistics();
		latencySlope = new ControlBasedStatistics();
	}

	@Override
	public String toString() {
		return "TaskStatistics [_selectivity=" + selectivity 
				+ ", _latency=" + latency
				+ ", _throughput="
				+ throughput 
				+ ", _memory=" + memory 
				+ ", _input_rate=" + inputRate 
				+ ", _cpu_load=" + cpuLoad + "]";
	}

}
