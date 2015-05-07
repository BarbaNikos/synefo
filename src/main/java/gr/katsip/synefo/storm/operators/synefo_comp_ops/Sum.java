package gr.katsip.synefo.storm.operators.synefo_comp_ops;
import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class Sum implements AbstractOperator,Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3227638098677786687L;

	int size;
	
	int attribute;
	
	int type;
	
	int sum =0;
	
	long cryptoSum=0;
	
	int counter=0;	
	
	List<Values> stateValues = new ArrayList<Values>();
	
	private HashMap<String, Integer> encryptionData = new HashMap<String,Integer>();
	
	private int stats;
	
	private dataCollector dataSender = null;
	
	private String zooIP;
	
	private Integer zooPort;
	
	private String ID;
	
	private int statsCounter=0;
	
	private Fields stateSchema;

	private Fields output_schema;

	public Sum(int buff, int attr, String ID, int statBuffer, String zooIP, Integer zooPort){//may need predicate later
		size = buff;
		attribute=attr;
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
	

	@Override
	public  List<Values> execute(Fields fields, Values values) {
		if(dataSender == null) {
			dataSender = new dataCollector(zooIP, zooPort, stats, ID);
		}
		if(!values.get(0).toString().contains("SPS")){
			String[] tuples = values.get(0).toString().split(",");
			List<Values> vals = new ArrayList<Values>();
			Values summ = new Values();
			summ.add(regSum(tuples));
			 vals.add(summ);
				encryptionData.put(tuples[tuples.length-1], encryptionData.get(tuples[tuples.length-1])+1);
				statsCounter++;
				if(statsCounter>1000){
					updateData();
				}
			return vals;
		}else{
			return null;
		}
	}
	
	public int regSum(String[] tuple){
		counter++;
		sum+=Integer.parseInt(tuple[attribute]);
		if(counter==size){
			counter=0;
			int sm = sum;
			sum=0;
			System.out.println("Sum: "+sm);
			return sm;
		}
		else{
			return -1;
		}
	}

	@Override
	public void setOutputSchema(Fields _output_schema) {
		output_schema = new Fields(_output_schema.toList());
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema = new Fields(stateSchema.toList());
	}

	


	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> getStateValues() {
		return stateValues;
	}

	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return output_schema;
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
		String tuple = 	ID+","+CPU+","+memory+","+latency+","+throughput+","+sel+","+encryptionData.get("pln")+","
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
