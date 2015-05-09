package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import gr.katsip.synefo.metric.TaskStatistics;
import java.io.Serializable;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Select implements Serializable, AbstractCrypefoOperator  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7785533234885036231L;
	
	Logger logger = LoggerFactory.getLogger(Select.class);

	ArrayList<Integer> selections;

	String predicate;

	int clientOwnerId;

	int attribute;

	int type;	//equi=0; "<" = 1; "<=" = 2...; 

	private List<Values> stateValues;

	private Fields stateSchema;

	private Fields output_schema;

	private HashMap<String, Integer> encryptionData = new HashMap<String,Integer>();
	
	private int statReportPeriod;

	private dataCollector dataSender = null;

	private String zooIP;

	private Integer zooPort;

	private String ID;

	/**
	 * 
	 * @param returnSet
	 * @param pred
	 * @param att
	 * @param typ
	 * @param client
	 * @param statReportPeriod the period of reporting statistics (in tuples)
	 * @param ID
	 * @param zooIP
	 * @param zooPort
	 */
	public Select(ArrayList<Integer> returnSet, String pred, int att, int typ, int client, 
			int statReportPeriod, String ID, String zooIP, Integer zooPort) {
		predicate = pred;
		attribute = att;
		type = typ;
		clientOwnerId = client;
		selections = new ArrayList<Integer>(returnSet);
		encryptionData.put("pln",0);
		encryptionData.put("RND",0);
		encryptionData.put("DET",0);
		encryptionData.put("OPE",0);
		encryptionData.put("HOM",0);
		this.statReportPeriod = statReportPeriod;
		this.ID = ID;
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		dataSender = null;
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

	@Override
	public void mergeState(Fields receivedStateSchema, List<Values> receivedStateValues) {
		//Nothing to do since no state is kept
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
	public List<Values> execute(TaskStatistics statistics, Fields fields,
			Values values) {
		if(dataSender == null) {
			dataSender = new dataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		if(!values.get(0).toString().contains("SPS")) {
			String[] tuples = values.get(0).toString().split(",");
			boolean matches  = false;
			if(type == 0) {
				matches = equiSelect(tuples);
			}else if(type == 1 || type ==2 ) {
				matches = lessSelect(tuples);
			}else if(type == 3 || type == 4) {
				matches = greaterSelect(tuples);
			}else {
				matches = false;
			}
			encryptionData.put(tuples[tuples.length-1], encryptionData.get(tuples[tuples.length-1])+1);
			updateData(statistics);
			Values val = new Values(); val.addAll(values);
			ArrayList<Values> valz = new ArrayList<Values>();
			valz.add(val);
			if(matches)
				return valz;
			else
				return new ArrayList<Values>();
		}else {
			return new ArrayList<Values>();
		}
	}
	
	public List<Values> execute(Fields fields, Values values) {
		if(dataSender == null) {
			dataSender = new dataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		if(!values.get(0).toString().contains("SPS")) {
			String[] tuples = values.get(0).toString().split(",");
			boolean matches  = false;
			if(type == 0) {
				matches = equiSelect(tuples);
			}else if(type == 1 || type ==2 ) {
				matches = lessSelect(tuples);
			}else if(type == 3 || type == 4) {
				matches = greaterSelect(tuples);
			}else {
				matches = false;
			}
			encryptionData.put(tuples[tuples.length-1], encryptionData.get(tuples[tuples.length-1])+1);
			updateData(null);
			Values val = new Values(); val.addAll(values);
			ArrayList<Values> valz = new ArrayList<Values>();
			valz.add(val);
			if(matches)
				return valz;
			else
				return new ArrayList<Values>();
		}else {
			return new ArrayList<Values>();
		}
	}

	public boolean equiSelect(String[] tuple) {
		if(predicate.equals("*") && predicate == null)
			return true;
		if(tuple[attribute].equalsIgnoreCase(predicate))
			return true;
		else
			return false;
	}

	public boolean lessSelect(String[] tuple) {
		if(type == 1) {
			if(Integer.parseInt(tuple[attribute])<Integer.parseInt(predicate))
				return true;
			else
				return false;
		}else {
			if(Integer.parseInt(tuple[attribute])<=Integer.parseInt(predicate)) 
				return true;
			else
				return false;
		}
	}

	public boolean greaterSelect(String[] tuple) {
		if(type == 3){
			if(Integer.parseInt(tuple[attribute])>Integer.parseInt(predicate))
				return true;
			else
				return false;	
		}else {
			if(Integer.parseInt(tuple[attribute]) >= Integer.parseInt(predicate))
				return true;
			else
				return false;
		}
	}

	public void updateData(TaskStatistics stats) {
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
		if(stats != null) {
			String tuple = 	ID + "," + stats.getCpuLoad() + "," + stats.getMemory() + "," + stats.getWindowLatency() + "," + 
					stats.getWindowThroughput() + "," + stats.getSelectivity() + "," + 
					encryptionData.get("pln") + "," + 
					encryptionData.get("RND") + "," + 
					encryptionData.get("DET") + "," + 
					encryptionData.get("OPE") + ","  + 
					encryptionData.get("HOM");
			
			dataSender.addToBuffer(tuple);
			encryptionData.put("pln",0);
			encryptionData.put("RND",0);
			encryptionData.put("DET",0);
			encryptionData.put("OPE",0);
			encryptionData.put("HOM",0);
		}else {
			String tuple = 	ID + "," + CPU + "," + memory + "," + latency + "," + 
					throughput + "," + sel + "," + 
					encryptionData.get("pln") + "," + 
					encryptionData.get("RND") + "," + 
					encryptionData.get("DET") + "," + 
					encryptionData.get("OPE") + ","  + 
					encryptionData.get("HOM");
			
			dataSender.addToBuffer(tuple);
			encryptionData.put("pln",0);
			encryptionData.put("RND",0);
			encryptionData.put("DET",0);
			encryptionData.put("OPE",0);
			encryptionData.put("HOM",0);
		}
	}
	
}
