package gr.katsip.deprecated.deprecated;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.crypstream.DataCollector;

/**
 * @deprecated
 */
public class StreamgenStatTupleProducer implements AbstractStatTupleProducer, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5118357122275755409L;

	private transient Socket dataProvider;

	private transient BufferedReader input;

	private transient PrintWriter output;

	private Fields fields;

	private String dataProviderIP;
	
	private String producerName;
	
	private DataCollector dataSender = null;

	private String zooConnectionInfo;
	
	private int statReportPeriod;

	public StreamgenStatTupleProducer(String dataProviderIP, 
			String zooConnectionInfo, int statReportPeriod) {
		dataProvider = null;
		this.dataProviderIP = dataProviderIP;
		this.producerName = null;
		this.zooConnectionInfo = zooConnectionInfo;
		this.dataSender = null;
		this.statReportPeriod = statReportPeriod;
	}
	
	public void connect() {
		try {
			dataProvider = new Socket(dataProviderIP, 6666);
			output = new PrintWriter(dataProvider.getOutputStream(), true);
			input = new BufferedReader(new InputStreamReader(dataProvider.getInputStream()));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Values nextTuple() {
		if(dataProvider == null)
			connect();
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, producerName);
		}
		Values val = new Values();
		try {
			if(dataProvider.isClosed() == false) {
				String tuple = input.readLine();
				if(tuple != null && tuple.length() > 0) {
					String[] tupleTokens = tuple.split(",");
					for(int i = 0; i < tupleTokens.length; i++) {
						if(val.size() < fields.size())
							val.add(tupleTokens[i]);
					}
					if(val.size() < fields.size()) {
						while(val.size() < fields.size()) {
							val.add(new String("N/A"));
						}
					}
					return val;
				}
			}else {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			try {
				input.close();
				output.close();
				dataProvider.close();
				return val;
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (NullPointerException e) {
			return val;
		}
		updateData(null);
		return null;
	}

	@Override
	public void setSchema(Fields fields) {
		this.fields = new Fields(fields.toList());
	}

	@Override
	public Fields getSchema() {
		return fields;
	}

	@Override
	public Values nextTuple(TaskStatistics statistics) {
		if(dataProvider == null)
			connect();
		if(dataSender == null) {
			String[] zooTokens = this.zooConnectionInfo.split(":");
			dataSender = new DataCollector(zooTokens[0], Integer.parseInt(zooTokens[1]), 
					statReportPeriod, producerName);
		}
		Values val = new Values();
		try {
			if(dataProvider.isClosed() == false) {
				String tuple = input.readLine();
				if(tuple != null && tuple.length() > 0) {
					String[] tupleTokens = tuple.split(",");
					for(int i = 0; i < tupleTokens.length; i++) {
						if(val.size() < fields.size())
							val.add(tupleTokens[i]);
					}
					if(val.size() < fields.size()) {
						while(val.size() < fields.size()) {
							val.add(new String("N/A"));
						}
					}
					return val;
				}
			}else {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			try {
				input.close();
				output.close();
				dataProvider.close();
				return val;
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (NullPointerException e) {
			return val;
		}
		updateData(statistics);
		return null;
	}

	@Override
	public void updateProducerName(String producerName) {
		this.producerName = producerName;
	}
	
	public void updateData(TaskStatistics stats) {
		float CPU = (float) 0.0;
		float memory = (float) 0.0;
		float latency = (float) 0.0;
		int throughput = 0;
		float sel = (float) 0.0;
		
		if(stats != null) {
			String tuple = 	(float) stats.getCpuLoad() + "," + (float) stats.getMemory() + "," + 
					(int) 0.0 + "," + (int) stats.getWindowThroughput() + "," + 
					(float) 1.0 + ",0,0,0,0,0";
			dataSender.addToBuffer(tuple);
		}else {
			String tuple = CPU + "," + memory + "," + latency + "," + 
					throughput + "," + sel + ",0,0,0,0,0";
			dataSender.addToBuffer(tuple);
		}
	}

}
