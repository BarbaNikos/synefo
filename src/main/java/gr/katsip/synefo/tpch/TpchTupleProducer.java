package gr.katsip.synefo.tpch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import gr.katsip.synefo.storm.producers.AbstractTupleProducer;

public class TpchTupleProducer implements AbstractTupleProducer, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4396139291927373430L;

	private Fields fields;
	
	private Fields schema;
	
	private transient Socket dataProvider;

	private transient BufferedReader input;

	private transient PrintWriter output;

	private String dataProviderAddress;
	
	private Fields projectedSchema;
	
	public TpchTupleProducer(String dataProviderAddress, String[] schema, String[] projectedSchema) {
		dataProvider = null;
		this.dataProviderAddress = dataProviderAddress;
		this.schema = new Fields(schema);
		this.projectedSchema = new Fields(projectedSchema);
	}
	
	public void connect() {
		try {
			dataProvider = new Socket(dataProviderAddress, 6666);
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
		Values values = new Values();
		try {
			if(dataProvider.isClosed() == false) {
				String line = input.readLine();
				String[] attributes = line.split("|");
				for(int i = 0; i < schema.size(); i++) {
					if(projectedSchema.toList().contains(schema.get(i))) {
						values.add(attributes[i]);
					}
				}
				Values tuple = new Values();
				tuple.add(projectedSchema);
				tuple.add(values);
				return tuple;
			}else {
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			try {
				input.close();
				output.close();
				dataProvider.close();
				return null;
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (NullPointerException e) {
			return null;
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
