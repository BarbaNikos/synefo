package gr.katsip.deprecated.crypefo;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.deprecated.crypstream.DataCollector;
import gr.katsip.deprecated.deprecated.AbstractStatTupleProducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CrypefoDataTupleProducer implements AbstractStatTupleProducer, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3531218023323197721L;

	private transient Socket dataProvider;

	private transient BufferedReader input;

	@SuppressWarnings("unused")
	private transient PrintWriter output;

	private Fields fields;

	private String dataProviderIP;

	private int uniqueId;

	private String producerName;

	private DataCollector dataSender = null;

	private String zooConnectionInfo;

	private int statReportPeriod;

	public CrypefoDataTupleProducer(String dataProviderIP, int unique, String producerName, String zooConnectionInfo, int statReportPeriod) {
		dataProvider = null;
		this.dataProviderIP = dataProviderIP;
		this.uniqueId = unique;
		this.zooConnectionInfo = zooConnectionInfo;
		this.dataSender = null;
		this.statReportPeriod = statReportPeriod;
		this.producerName=producerName;
	}

	public void connect() {
		try {
			/**
			 * Connects to port 6666 using CryptStreamProvider for data tuples
			 */
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
			String tuple = input.readLine();	
			String newTuple = uniqueId + "//$$$//" + tuple;
			if(tuple != null && tuple.length() > 0) {
				val.add(newTuple);
				if(val.size() < fields.size()) {
					while(val.size() < fields.size()) {
						val.add(new String("N/A"));
					}
				}
				//System.out.println("size "+ val.size()+ " "+tuple);
				return val;
			}
		} catch (IOException e) {
			e.printStackTrace();
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
			String tuple = input.readLine();	
			String newTuple = uniqueId + "//$$$//" + tuple;
			if(tuple != null && tuple.length() > 0) {
				val.add(newTuple);
				if(val.size() < fields.size()) {
					while(val.size() < fields.size()) {
						val.add(new String("N/A"));
					}
				}
				//System.out.println("size "+ val.size()+ " "+tuple);
				return val;
			}
		} catch (IOException e) {
			e.printStackTrace();
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
		int latency = 0;
		int throughput = 0;
		float sel = (float) 0.0;
		if(stats != null) {
			String tuple = 	(float) stats.getCpuLoad() + "," + (float) stats.getMemory() + "," + 
					(int) stats.getWindowLatency() + "," + (int) stats.getWindowThroughput() + "," + 
					(float) stats.getSelectivity() + ",0,0,0,0,0";
			dataSender.addToBuffer(tuple);
		}else {
			String tuple = CPU + "," + memory + "," + latency + "," + 
					throughput + "," + sel + ",0,0,0,0,0";
			dataSender.addToBuffer(tuple);
		}
	}

}
