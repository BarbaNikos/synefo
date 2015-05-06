package gr.katsip.synefo.storm.operators.synefo_comp_ops;
import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;




public class Count implements AbstractOperator, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7933132683878687563L;

	private String ID;

	private int size;

	private int attribute;

	private String predicate;

	private int count=0;

	private int counter=0;

	private int statsCounter=0;

	private int type;

	private List<Values> stateValues;

	private int stats;

	private HashMap<String, Integer> encryptionData = new HashMap<String,Integer>();

	private dataCollector dataSender = null;

	private String zooIP;

	private Integer zooPort;

	private Fields stateSchema;

	private Fields output_schema;

	@Override
	public void init(List<Values> stateValues){
		this.stateValues = stateValues;
	}
	@Override
	public void setStateSchema(Fields stateSchema){
		this.stateSchema = new Fields(stateSchema.toList());
	}
	@Override
	public void setOutputSchema(Fields _output_schema){
		output_schema = new Fields(_output_schema.toList());
	}
	@Override
	public List<Values> execute(Fields fields, Values values){
		if(dataSender == null) {
			dataSender = new dataCollector(zooIP, zooPort, stats, ID);
		}
		if(!values.get(0).toString().contains("SPS")){
			List<Values> returnedTuples = new ArrayList<Values>();
			System.out.println( values.get(0));
			String[] tuples = values.get(0).toString().split(",");
			if(type==0){
				returnedTuples.add(new Values(equiCount(tuples)));
			}
			else if (type==1 || type==2){
				returnedTuples.add(new Values(lessCount(tuples)));
			}
			else if (type==3 || type==4){
				returnedTuples.add(new Values(greaterCount(tuples)));
			}
			else{
				returnedTuples.add(new Values(-1));
			}
			encryptionData.put(tuples[tuples.length-1], encryptionData.get(tuples[tuples.length-1])+1);
			statsCounter++;
			if(statsCounter>1000){
				updateData();
			}
			return returnedTuples;

		}
		else{
			return null;
		}

	}
	@Override
	public List<Values> getStateValues(){
		stateValues.clear();
		Values newCount = new Values();
		newCount.add(count);
		stateValues.add(newCount);
		return stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return output_schema;
	}
	@Override
	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues) {
	}


	public Count(int buff, int attr, String pred, int typ, String client, int statBuffer, String zooIP, Integer zooPort){
		size=buff;
		attribute = attr;
		predicate = pred;
		type = typ;
		ID=client;
		encryptionData.put("pln",0);
		encryptionData.put("RND",0);
		encryptionData.put("DET",0);
		encryptionData.put("OPE",0);
		encryptionData.put("HOM",0);
		stats = statBuffer;
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		dataSender = null;
	}

	public int equiCount(String[] tuple){
		if(tuple[attribute].equalsIgnoreCase(predicate)){
			count++;
		}
		counter++;
		if(counter==size)
		{
			int ret = count;
			count = 0;
			counter=0;
			System.out.println("Count: "+ret);
			return ret;
		}
		else{
			return -1;
		}
	}

	public int lessCount(String[] tuple){
		if(type==1){
			if(Integer.parseInt(tuple[attribute])<Integer.parseInt(predicate)){
				count++;
			}
			counter++;
			if(counter==size)
			{
				int ret = count;
				count = 0;
				counter=0;
				System.out.println("Count: "+ret);
				return ret;
			}
			else{
				return -1;
			}
		}
		else{
			if(Integer.parseInt(tuple[attribute])<Integer.parseInt(predicate)){
				count++;
			}
			counter++;
			if(counter==size)
			{
				int ret = count;
				count = 0;
				counter=0;
				System.out.println("Count: "+ret);
				return ret;
			}
			else{
				return -1;
			}
		}
	}

	public int greaterCount(String[] tuple){
		if(type==1){
			if(Integer.parseInt(tuple[attribute])>Integer.parseInt(predicate)){
				count++;
			}
			counter++;
			if(counter==size)
			{
				int ret = count;
				count = 0;
				counter=0;
				System.out.println("Count: "+ret);
				return ret;
			}
			else{
				return -1;
			}
		}
		else{
			if(Integer.parseInt(tuple[attribute])>=Integer.parseInt(predicate)){
				count++;
			}
			counter++;
			if(counter==size)
			{
				int ret = count;
				count = 0;
				counter=0;
				System.out.println("Count: "+ret);
				return ret;
			}
			else{
				return -1;
			}
		}
	}

	public void updateData(){
		/**
		 * `operator_id` INT NOT NULL,
				`cpu` FLOAT NULL,
					`memory` FLOAT NULL,
		  `latency` INT NULL,
		  `throughput` INT NULL,
		  `selectivity` FLOAT NULL,
		  `plain` INT NULL,
		  `det` INT NULL,
		  `rnd` INT NULL,
		  `ope` INT NULL,
		  `hom` INT NULL,
		 */
		int CPU = 0;
		int memory = 0;
		int latency = 0;
		int throughput = 0;
		int sel = 0;
		//////////////////////////replace 1 with id
		String tuple = 	1+","+CPU+","+memory+","+latency+","+throughput+","+sel+","+encryptionData.get("pln")+","
				+encryptionData.get("RND")+","
				+encryptionData.get("DET")+","
				+encryptionData.get("OPE")+","
				+encryptionData.get("HOM");
		//	System.out.println("UPDATING STATS");
		dataSender.addToBuffer(tuple);
		encryptionData.put("pln",0);
		encryptionData.put("RND",0);
		encryptionData.put("DET",0);
		encryptionData.put("OPE",0);
		encryptionData.put("HOM",0);
	}
}
