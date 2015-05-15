package gr.katsip.synefo.storm.operators.crypstream;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractStatOperator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Count implements AbstractStatOperator, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7933132683878687563L;

	Logger logger = LoggerFactory.getLogger(Count.class);

	private String ID;

	private int size;

	private int attribute;

	private String predicate;

	private int count = 0;

	private int counter = 0;

	private int type;

	private List<Values> stateValues;

	private int statReportPeriod;

	private HashMap<String, Integer> encryptionData = new HashMap<String,Integer>();

	private DataCollector dataSender = null;

	private String zooIP;

	private Integer zooPort;

	private Fields stateSchema;

	private Fields output_schema;

	private ZooKeeper zk = null;

	private  Watcher SPSRetrieverWatcher =null;

	private  DataCallback getSPSCallback =null;

	public Count(int buff, int attr, String pred, int typ, String client, 
			int statReportPeriod, String zooIP, Integer zooPort) {
		size = buff;
		attribute = attr;
		predicate = pred;
		type = typ;
		ID = client;
		encryptionData.put("pln",0);
		encryptionData.put("RND",0);
		encryptionData.put("DET",0);
		encryptionData.put("OPE",0);
		encryptionData.put("HOM",0);
		this.statReportPeriod = statReportPeriod;
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		dataSender = null;
	}

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
	public List<Values> execute(TaskStatistics statistics, Fields fields,
			Values values) {
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		SPSRetrieverWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				String path = event.getPath();
				//System.out.println("Select "+ID+" Received event type: " + event.getType()+ " path "+event.getPath());
				if(event.getType() == Event.EventType.NodeDataChanged) {
					//Retrieve operator
					getDataAndWatch();
					System.out.println("NodeDataChanged event: " + path);
					try {
						byte[] data =zk.getData(path,false,null);
						handleUpdate(new String(data));
					} catch (KeeperException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}else if(event.getType() == null) {
					//Retrieve new children
					getDataAndWatch();
				}else{
					getDataAndWatch();
				}
			}
		};
		if(zk==null){
			try {
				zk = new ZooKeeper(this.zooIP + ":" + this.zooPort, 100000, SPSRetrieverWatcher);
			} catch (IOException e) {
				e.printStackTrace();}
		}
		if(!values.get(0).toString().contains("SPS")) {
			List<Values> returnedTuples = new ArrayList<Values>();
			System.out.println( values.get(0));
			String[] tuples = values.get(0).toString().split(Pattern.quote("//$$$//"));
			if(type==0){
				returnedTuples.add(new Values(equiCount(tuples)));
			}
			else if(type==1 || type==2) {
				returnedTuples.add(new Values(lessCount(tuples)));
			}
			else if(type==3 || type==4) {
				returnedTuples.add(new Values(greaterCount(tuples)));
			}
			else {
				returnedTuples.add(new Values(-1));
			}
			String[] encUse= tuples[tuples.length-1].split(" ");
			for(int k =0;k<encUse.length;k++){
				encryptionData.put(encUse[k], encryptionData.get(encUse[k])+1);
			}updateData(statistics);
			return returnedTuples;
		}
		else{
			return new ArrayList<Values>();
		}
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		if(SPSRetrieverWatcher==null){
			SPSRetrieverWatcher = new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					String path = event.getPath();
					//System.out.println("Select "+ID+" Received event type: " + event.getType()+ " path "+event.getPath());
					if(event.getType() == Event.EventType.NodeDataChanged) {
						//Retrieve operator
						getDataAndWatch();
						System.out.println("NodeDataChanged event: " + path);
						try {
							byte[] data =zk.getData(path,false,null);
							handleUpdate(new String(data));
						} catch (KeeperException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else if(event.getType() == null) {
						//Retrieve new children
						getDataAndWatch();
					}else{
						getDataAndWatch();
					}
				}
			};
		}
		if(getSPSCallback==null){
			getSPSCallback = new DataCallback() {
				@Override
				public void processResult(int rc, String path, Object ctx, byte[] data,
						Stat stat) {
					switch(Code.get(rc)) {
					case CONNECTIONLOSS:
						System.out.println("getSPSCallback(): CONNECTIONLOSS");
						getDataAndWatch();
						break;
					case NONODE:
						System.out.println("getSPSCallback(): NONODE");
						break;
					case OK:
						System.out.println("getSPSCallback(): Successfully retrieved new predicate: " + 
								new String(data));
						handleUpdate(new String(data));
						getDataAndWatch();
						break;
					default:
						System.out.println("getDataCallback(): Unexpected scenario: " + 
								KeeperException.create(Code.get(rc), path) );
						break;
					}
				}

			};
		}
			if(!values.get(0).toString().contains("SPS")) {
				List<Values> returnedTuples = new ArrayList<Values>();
				System.out.println( values.get(0));
				String[] tuples = values.get(0).toString().split(Pattern.quote("//$$$//"));
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
				updateData(null);
				return returnedTuples;
			}
			else{
				return new ArrayList<Values>();
			}
		}

		@Override
		public List<Values> getStateValues() {
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
		Integer receivedCount = (Integer) (receivedStateValues.get(0)).get(0);
		count += receivedCount;
		}

		public int equiCount(String[] tuple) {
			if(tuple[attribute].equalsIgnoreCase(predicate)){
				count++;
			}
			counter++;
			if(counter == size) {
				int ret = count;
				count = 0;
				counter=0;
				return ret;
			}else {
				return -1;
			}
		}

		public int lessCount(String[] tuple) {
			if(type==1) {
				if(Integer.parseInt(tuple[attribute])<Integer.parseInt(predicate))
					count++;
				counter++;
				if(counter == size) {
					int ret = count;
					count = 0;
					counter=0;
					return ret;
				}else {
					return -1;
				}
			}else {
				if(Integer.parseInt(tuple[attribute])<Integer.parseInt(predicate))
					count++;
				counter++;
				if(counter == size) {
					int ret = count;
					count = 0;
					counter=0;
					return ret;
				}else {
					return -1;
				}
			}
		}

		public int greaterCount(String[] tuple) {
			if(type==1) {
				if(Integer.parseInt(tuple[attribute])>Integer.parseInt(predicate))
					count++;
				counter++;
				if(counter == size) {
					int ret = count;
					count = 0;
					counter=0;
					return ret;
				}else {
					return -1;
				}
			}else {
				if(Integer.parseInt(tuple[attribute])>=Integer.parseInt(predicate))
					count++;
				counter++;
				if(counter == size) {
					int ret = count;
					count = 0;
					counter=0;
					return ret;
				}else {
					return -1;
				}
			}
		}

		public void updateData(TaskStatistics stats) {
			int CPU = 0;
			int memory = 0;
			int latency = 0;
			int throughput = 0;
			int sel = 0;
			if(stats != null) {
				String tuple = 	 (float) stats.getCpuLoad() + "," + (float) stats.getMemory() + "," + 
						(int) stats.getWindowLatency() + "," + (int) stats.getWindowThroughput() + "," + 
						(float) stats.getSelectivity() + "," + 
						encryptionData.get("pln") + "," +
						encryptionData.get("DET") + "," +
						encryptionData.get("RND") + "," +
						encryptionData.get("OPE") + ","  + 
						encryptionData.get("HOM");

				dataSender.addToBuffer(tuple);
				encryptionData.put("pln",0);
				encryptionData.put("DET",0);
				encryptionData.put("RND",0);
				encryptionData.put("OPE",0);
				encryptionData.put("HOM",0);
			}else {
				String tuple = 	CPU + "," + memory + "," + latency + "," + 
						throughput + "," + sel + "," + 
						encryptionData.get("pln") + "," + 
						encryptionData.get("DET") + "," + 
						encryptionData.get("RND") + "," +
						encryptionData.get("OPE") + ","  + 
						encryptionData.get("HOM");

				dataSender.addToBuffer(tuple);
				encryptionData.put("pln",0);
				encryptionData.put("DET",0);
				encryptionData.put("RND",0);
				encryptionData.put("OPE",0);
				encryptionData.put("HOM",0);
			}
		}
		public void getDataAndWatch() {
		//	System.out.println("Select watch reset in select "+ID);
			zk.getData("/SPS", 
					true, 
					getSPSCallback, 
					"/SPS/".getBytes());
		}

		private void handleUpdate(String data){
			String[] sp = data.split(",");
			if(sp[0].equalsIgnoreCase("count")){
				predicate = sp[1];
				System.out.println("Predicate in "+ID+" changed to: "+predicate);
			}
		}

		@Override
		public void updateOperatorName(String operatorName) {
			this.ID = operatorName;
			
		}

	}
