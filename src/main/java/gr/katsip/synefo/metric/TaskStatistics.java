package gr.katsip.synefo.metric;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import com.sun.management.OperatingSystemMXBean;

@SuppressWarnings("restriction")
public class TaskStatistics implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2552450405227774625L;

	private double _selectivity;
	
	private long _sel_samples;
	
	private long _latency;
	
	private long _prev_latency;
	
	private long _lat_samples;
	
	private double _throughput;
	
	private long _thrpt_samples;
	
	private long _thrpt_prev_timestamp;
	
	private long _thrpt_prev_tuple_num;
	
	private double _memory;
	
	private long _mem_samples;
	
	private double _input_rate;
	
	private long _inrt_samples;

	private double _cpu_load;
	
	private long _cpu_samples;
	
	public TaskStatistics() {
		_selectivity = 0.0;
		_sel_samples = 0;
		_latency = 0;
		_lat_samples = 0;
		_throughput = 0;
		_thrpt_samples = 0;
		_memory = 0.0;
		_mem_samples = 0;
		_input_rate = 0.0;
		_inrt_samples = 0;
		_cpu_load = 0.0;
		_cpu_samples = 0;
	}
	
	public void update_selectivity(double selectivity) {
		if(_sel_samples == 0) {
			_selectivity = selectivity;
			_sel_samples += 1;
		}else {
			_selectivity = _selectivity + (selectivity - _selectivity)/(_sel_samples + 1);
			_sel_samples += 1;
		}
	}

	public double get_selectivity() {
		return _selectivity;
	}
	
	public void update_latency() {
		if(_lat_samples == 0) {
			_latency = 0;
			_prev_latency = System.currentTimeMillis();
			_lat_samples +=1;
		}else {
			long _curr_timestamp = System.currentTimeMillis();
			long latency = _curr_timestamp - _prev_latency;
			_latency = _latency + (latency - _latency)/(_lat_samples + 1);
			_lat_samples += 1;
			_prev_latency = _curr_timestamp;
		}
	}

	public long get_latency() {
		return _latency;
	}
	
	public void update_throughput(long _thrpt_current_tuple_num) {
		if(_thrpt_samples == 0) {
			_throughput = 0;
			_thrpt_prev_tuple_num = _thrpt_current_tuple_num;
			_thrpt_prev_timestamp = System.currentTimeMillis();
			_thrpt_samples += 1;
		}else {
			long _thrpt_delta = Math.abs(_thrpt_prev_tuple_num - _thrpt_current_tuple_num);
			long _curr_timestamp = System.currentTimeMillis();
			long _thrpt_time_delta = Math.abs(_curr_timestamp - _thrpt_prev_timestamp) / 1000;
			if(_thrpt_time_delta == 0)
				_thrpt_time_delta = 1;
			double throughput = _thrpt_delta / _thrpt_time_delta;
			_throughput = _throughput + (throughput - _throughput)/(_thrpt_samples + 1);
			_thrpt_samples += 1;
			_thrpt_prev_tuple_num = _thrpt_current_tuple_num;
			_thrpt_prev_timestamp = _curr_timestamp;
		}
	}

	public double get_throughput() {
		return _throughput;
	}
	
	public void update_memory() {
		MemoryUsage heapMemUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		MemoryUsage nonHeapMemUsage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
		long memory = heapMemUsage.getUsed() + nonHeapMemUsage.getUsed();
		if(_mem_samples == 0) {
			_memory = memory;
			_mem_samples += 1;
		}else {
			_memory = _memory + (memory - _memory)/(_mem_samples + 1);
			_mem_samples += 1;
		}
	}
	
	public double get_memory() {
		return _memory;
	}
	
	public void update_input_rate(double input_rate) {
		if(_inrt_samples == 0) {
			_input_rate = input_rate;
			_inrt_samples += 1;
		}else {
			_input_rate = _input_rate + (input_rate - _input_rate)/(_inrt_samples + 1);
			_inrt_samples += 1;
		}
	}
	
	public double get_input_rate() {
		return _input_rate;
	}
	
	public void update_cpu_load() {
		double cpu_load = 0.0;
		OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
	            .getOperatingSystemMXBean();
		cpu_load = bean.getProcessCpuLoad();
		if(_cpu_samples == 0) {
			_cpu_load = cpu_load;
			_cpu_samples += 1;
		}else {
			_cpu_load = Math.abs(_cpu_load + (cpu_load - _cpu_load)/(_cpu_samples + 1));
			_cpu_samples += 1;
		}
	}
	
	public double get_cpu_load() {
		return _cpu_load;
	}
	
	public void reset_statistics() {
		_selectivity = 0.0;
		_sel_samples = 0;
		_latency = 0;
		_lat_samples = 0;
		_throughput = 0;
		_thrpt_samples = 0;
		_memory = 0.0;
		_mem_samples = 0;
		_input_rate = 0.0;
		_inrt_samples = 0;
		_cpu_load = 0.0;
		_cpu_samples = 0;
	}

	@Override
	public String toString() {
		return "TaskStatistics [_selectivity=" + _selectivity 
				+ ", _latency=" + _latency
				+ ", _throughput="
				+ _throughput 
				+ ", _memory=" + _memory 
				+ ", _input_rate=" + _input_rate 
				+ ", _cpu_load=" + _cpu_load + "]";
	}
	
	
	
}
