package gr.katsip.synefo.storm.topology.crypefo;

import gr.katsip.synefo.storm.producers.AbstractTupleProducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CrypefoDataTupleProducer implements AbstractTupleProducer, Serializable {
	
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

	public CrypefoDataTupleProducer(String dataProviderIP) {
		dataProvider = null;
		this.dataProviderIP = dataProviderIP;
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
		Values val = new Values();
		try {
			String tuple = input.readLine();
			
			if(tuple != null && tuple.length() > 0) {
				String[] tupleTokens_1 = tuple.split(":");				
				String[] tupleTokens = {tupleTokens_1[1]};
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
