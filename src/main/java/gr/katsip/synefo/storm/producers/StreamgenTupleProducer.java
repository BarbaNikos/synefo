package gr.katsip.synefo.storm.producers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.StringTokenizer;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StreamgenTupleProducer implements AbstractTupleProducer, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7633248381407190059L;

	private transient Socket dataProvider;

	private transient BufferedReader input;

	@SuppressWarnings("unused")
	private transient PrintWriter output;

	private Fields fields;
	
	private String dataProviderIP;
	
	private Integer dataProviderPort;
	
	private long num;

	public StreamgenTupleProducer(String dataProviderIP, Integer dataProviderPort) {
		dataProvider = null;
		this.dataProviderIP = dataProviderIP;
		this.dataProviderPort = dataProviderPort;
		num = 0;
	}
	
	public void connect() {
		try {
			dataProvider = new Socket(dataProviderIP, dataProviderPort);
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
		Values val = new Values();
		try {
			String tuple = input.readLine();
			if(tuple != null && tuple.length() > 0) {
				StringTokenizer strTok = new StringTokenizer(tuple, ",");
				val.add(new Long(num));
//				val.add(new Long(System.currentTimeMillis()));
				while(strTok.hasMoreTokens() && val.size() < fields.size()) {
					val.add(strTok.nextToken());
				}
				if(val.size() < fields.size()) {
					while(val.size() < fields.size()) {
						val.add(new String("N/A"));
					}
				}
				num += 1;
				return val;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
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

}
