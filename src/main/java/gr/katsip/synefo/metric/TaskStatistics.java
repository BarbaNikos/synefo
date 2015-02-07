package gr.katsip.synefo.metric;

import java.io.Serializable;
import java.lang.management.ManagementFactory;

//import java.lang.management.MemoryUsage;
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
	
	private double memory;
	
	private long memorySamples;
	
	private double inputRate;
	
	private long inputRateSamples;

	private double cpuLoad;
	
	private long cpuSamples;
	
	public TaskStatistics() {
		selectivity = 0.0;
		selectivitySamples = 0;
		latency = 0;
		latencySamples = 0;
		throughput = 0;
		throughputSamples = 0;
		memory = 0.0;
		memorySamples = 0;
		inputRate = 0.0;
		inputRateSamples = 0;
		cpuLoad = 0.0;
		cpuSamples = 0;
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
			long _curr_timestamp = System.currentTimeMillis();
			long latency = _curr_timestamp - previousLatency;
			this.latency = this.latency + (latency - this.latency)/(latencySamples + 1);
			latencySamples += 1;
			previousLatency = _curr_timestamp;
		}
	}

	public long getLatency() {
		return latency;
	}
	
	public void updateThroughput(long _thrpt_current_tuple_num) {
		if(throughputSamples == 0) {
			this.throughput = 0;
			throughputPreviousTupleNumber = _thrpt_current_tuple_num;
			throughputPreviousTimestamp = System.currentTimeMillis();
			throughputSamples += 1;
		}else {
			long _thrpt_delta = Math.abs(throughputPreviousTupleNumber - _thrpt_current_tuple_num);
			long _curr_timestamp = System.currentTimeMillis();
			long _thrpt_time_delta = Math.abs(_curr_timestamp - throughputPreviousTimestamp) / 1000;
			if(_thrpt_time_delta == 0)
				_thrpt_time_delta = 1;
			double throughput = _thrpt_delta / _thrpt_time_delta;
			this.throughput = this.throughput + (throughput - this.throughput)/(throughputSamples + 1);
			throughputSamples += 1;
			throughputPreviousTupleNumber = _thrpt_current_tuple_num;
			throughputPreviousTimestamp = _curr_timestamp;
		}
	}

	public double getThroughput() {
		return throughput;
	}
	
	public void updateMemory() {
//		MemoryUsage heapMemUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
//		MemoryUsage nonHeapMemUsage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
//		long memory = heapMemUsage.getUsed() + nonHeapMemUsage.getUsed();
		Runtime runtime = Runtime.getRuntime();
		double memory = (runtime.totalMemory() - runtime.freeMemory()) / runtime.totalMemory();
		if(memorySamples == 0) {
//			this.memory = memory;
			this.memory = memory;
			memorySamples += 1;
		}else {
			this.memory = this.memory + (memory - this.memory)/(memorySamples + 1);
			memorySamples += 1;
		}
	}
	
	public double getMemory() {
		return memory;
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
	
	public void updateCpuLoad() {
		double cpu_load = 0.0;
		OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
	            .getOperatingSystemMXBean();
		cpu_load = bean.getProcessCpuLoad();
		if(cpuSamples == 0) {
			this.cpuLoad = cpu_load;
			cpuSamples += 1;
		}else {
			this.cpuLoad = Math.abs(this.cpuLoad + (cpu_load - this.cpuLoad)/(cpuSamples + 1));
			cpuSamples += 1;
		}
	}
	
	public double getCpuLoad() {
		return cpuLoad;
	}
	
	public void resetStatistics() {
		selectivity = 0.0;
		selectivitySamples = 0;
		latency = 0;
		latencySamples = 0;
		throughput = 0;
		throughputSamples = 0;
		memory = 0.0;
		memorySamples = 0;
		inputRate = 0.0;
		inputRateSamples = 0;
		cpuLoad = 0.0;
		cpuSamples = 0;
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
