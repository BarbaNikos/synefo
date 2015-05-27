package gr.katsip.synefo.metric;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;

import com.sun.management.OperatingSystemMXBean;

@SuppressWarnings("restriction")
public class TaskStatistics implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2552450405227774625L;

	private double selectivity;

	private long selectivitySamples;

	private long latency;

	private long previousLatency;

	private long latencySamples;

	private double throughput;

	private long throughputSamples;

	private long throughputPreviousTimestamp;

	private long throughputPreviousTupleNumber;

	private LinkedList<Double> throughputSampleWindow;

	private Double runningThroughputWindowSum;

	private final int sampleWindowSize = 100;

	private LinkedList<Long> latencySampleWindow;

	private Long runningLatencyWindowSum;

	private double memory;
	
	private LinkedList<Double> memorySampleWindow;
	
	private Double runningMemoryWindowSum;

	private double inputRate;

	private long inputRateSamples;
	
	private LinkedList<Double> cpuSampleWindow;
	
	private Double runningCpuWindowSum;

	private double cpuLoad;

	public TaskStatistics() {
		selectivity = 0.0;
		selectivitySamples = 0;
		latency = 0;
		latencySamples = 0;
		throughput = 0;
		throughputSamples = 0;
		memory = 0.0;
		inputRate = 0.0;
		inputRateSamples = 0;
		cpuLoad = 0.0;
		throughputSampleWindow = new LinkedList<Double>();
		runningThroughputWindowSum = 0.0;
		latencySampleWindow = new LinkedList<Long>();
		runningLatencyWindowSum = 0L;
		cpuSampleWindow = new LinkedList<Double>();
		runningCpuWindowSum = 0.0;
		memorySampleWindow = new LinkedList<Double>();
		runningMemoryWindowSum = 0.0;
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

	public void updateLatency() {
		if(latencySamples == 0) {
			this.latency = 0;
			previousLatency = System.currentTimeMillis();
			latencySamples +=1;
		}else {
			long currentTimestamp = System.currentTimeMillis();
			long latency = currentTimestamp - previousLatency;
			this.latency = this.latency + (latency - this.latency)/(latencySamples + 1);
			latencySamples += 1;
			previousLatency = currentTimestamp;
		}
	}

	public void updateLatency(long latency) {
		if(latencySamples == 0) {
			this.latency = latency;
			latencySamples +=1;
		}else {
			this.latency = this.latency + (latency - this.latency)/(latencySamples + 1);
			latencySamples += 1;
		}
	}

	public long getLatency() {
		return latency;
	}

	public void updateThroughput() {
		if(throughputSamples == 0) {
			this.throughput = 0;
			throughputPreviousTupleNumber = 1;
			throughputPreviousTimestamp = System.currentTimeMillis();
			throughputSamples += 1;
		}else {
			long _curr_timestamp = System.currentTimeMillis();
			//Time difference in seconds
			long _thrpt_time_delta = Math.abs(_curr_timestamp - throughputPreviousTimestamp);
			if(_thrpt_time_delta >= 1000) {
				double throughput = throughputPreviousTupleNumber + 1;
				this.throughput = this.throughput + (throughput - this.throughput)/(throughputSamples + 1);
				throughputSamples += 1;
				throughputPreviousTupleNumber = 0;
				throughputPreviousTimestamp = _curr_timestamp;
			}else {
				throughputPreviousTupleNumber += 1;
			}
		}
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
			if(timeDelta >= 1000) {
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
	}

	public void updateWindowLatency(long latency) {
		this.latency = latency;
		if(latencySampleWindow.size() >= sampleWindowSize) {
			Long removedValue = latencySampleWindow.poll();
			runningLatencyWindowSum -= removedValue;
		}
		latencySampleWindow.offer(this.latency);
		runningLatencyWindowSum += this.latency;
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

	public double getThroughput() {
		return throughput;
	}

	public double getWindowThroughput() {
		return throughputSampleWindow.size() > 0 ? (runningThroughputWindowSum / throughputSampleWindow.size()) : 0;
	}
	
	public long getWindowLatency() {
		return latencySampleWindow.size() > 0 ? (runningLatencyWindowSum / latencySampleWindow.size()) : 0;
	}

	public double getCpuLoad() {
		return cpuLoad;
//		return cpuSampleWindow.size() > 0 ? (runningCpuWindowSum / cpuSampleWindow.size()) : 0.0;
	}

	public void updateMemory() {
		Runtime runtime = Runtime.getRuntime();
		/**
		 * The formula below gives the percent of the maximum available memory to the JVM
		 */
//		memory = (double) (runtime.maxMemory() - runtime.totalMemory()) / runtime.maxMemory();
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
		latencySamples = 0;
		throughput = 0;
		throughputSamples = 0;
		memory = 0.0;
		inputRate = 0.0;
		inputRateSamples = 0;
		cpuLoad = 0.0;
		throughputSampleWindow = new LinkedList<Double>();
		runningThroughputWindowSum = 0.0;
		latencySampleWindow = new LinkedList<Long>();
		runningLatencyWindowSum = 0L;
		cpuSampleWindow = new LinkedList<Double>();
		runningCpuWindowSum = 0.0;
		memorySampleWindow = new LinkedList<Double>();
		runningMemoryWindowSum = 0.0;
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
