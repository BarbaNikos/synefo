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

	private Socket dataProvider;

	private BufferedReader input;

	private PrintWriter output;

	private Fields fields;

	public StreamgenTupleProducer(String dataProviderIP, Integer dataProviderPort) {
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
