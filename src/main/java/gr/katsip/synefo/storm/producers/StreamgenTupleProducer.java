package gr.katsip.synefo.storm.producers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.StringTokenizer;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StreamgenTupleProducer implements AbstractTupleProducer {

	private transient Socket dataProvider;

	private transient BufferedReader input;

	private transient PrintWriter output;

	private Fields fields;
	
	private String dataProviderIP;
	
	private Integer dataProviderPort;

	public StreamgenTupleProducer(String dataProviderIP, Integer dataProviderPort) {
		dataProvider = null;
		this.dataProviderIP = dataProviderIP;
		this.dataProviderPort = dataProviderPort;
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
				while(strTok.hasMoreTokens()) {
					val.add(strTok.nextToken());
				}
				return val;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void setSchema(Fields fields) {
		this.fields = new Fields(fields.toString());
	}

	@Override
	public Fields getSchema() {
		return fields;
	}

}
