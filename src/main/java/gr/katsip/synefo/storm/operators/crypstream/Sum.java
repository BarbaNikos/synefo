package gr.katsip.synefo.storm.operators.crypstream;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractStatOperator;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class Sum implements AbstractStatOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3227638098677786687L;

	private int size;

	private int attribute;

	private int type;

	private int sum = 0;

	private int streamId;

	private BigInteger cryptoSum = BigInteger.ONE;

	private int counter = 0;	

	List<Values> stateValues = new ArrayList<Values>();

	private HashMap<String, Integer> encryptionData = new HashMap<String,Integer>();

	private int statReportPeriod;

	private DataCollector dataSender = null;

	private String zooIP;

	private Integer zooPort;

	private String ID;

	private Fields stateSchema;

	private Fields output_schema;

	private ZooKeeper zk = null;
	
	private int statReportCount;

	private  Watcher SPSRetrieverWatcher =null;

	private  DataCallback getSPSCallback = null;

	public Sum(int stream, int buff, int attr, String ID, int statReportPeriod, 
			String zooIP, Integer zooPort) {//may need predicate later
		size = buff;
		attribute = attr;
		encryptionData.put("pln",0);
		encryptionData.put("DET",0);
		encryptionData.put("RND",0);
		encryptionData.put("OPE",0);
		encryptionData.put("HOM",0);
		this.statReportPeriod = statReportPeriod;
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		dataSender = null;
		type = 0;
		this.streamId=stream;
	}

	public int getAttribute(){
		return attribute;
	}

	@Override
	public List<Values> execute(TaskStatistics statistics, Fields fields,
			Values values) {
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		if(SPSRetrieverWatcher==null){
			SPSRetrieverWatcher = new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					String path = event.getPath();
					System.out.println("Select "+ID+" Received event type: " + event.getType()+ " path "+event.getPath());
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
						//System.out.println("getSPSCallback(): Successfully retrieved new predicate: " + 
						//		new String(data));
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
		if(zk==null){
			try {
				zk = new ZooKeeper(this.zooIP + ":" + this.zooPort, 100000, SPSRetrieverWatcher);
			} catch (IOException e) {
				e.printStackTrace();}
		}
		updateData(statistics);
		if(!values.get(0).toString().contains("SPS")) {
			String[] tuples = values.get(0).toString().split(Pattern.quote("//$$$//"));
			//System.out.println(values.get(0));
			List<Values> vals = new ArrayList<Values>();
			Values summ = new Values();
			String[] encUse= tuples[tuples.length-1].split(" ");
			for(int k =0;k<encUse.length;k++){
				encryptionData.put(encUse[k], encryptionData.get(encUse[k])+1);
			}
			if(type==0){
				int ret = regSum(tuples);
				summ.add(tuples[0]+"//$$$//SUM//$$$//"+ret);
				if(ret>0){
					vals.add(summ);
				}
			}
			else{
				BigInteger z = multBigInt(tuples);
				if (z.compareTo(BigInteger.ZERO)==1){
					Values v = new Values();
					v.add(tuples[0]+"//$$$//SUM//$$$//"+z);
					vals.add(v);
				}
			}
			return vals;
		}else {
			return new ArrayList<Values>();
		}
	}



	@Override
	public  List<Values> execute(Fields fields, Values values) {
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		if(!values.get(0).toString().contains("SPS")) {
			String[] tuples = values.get(0).toString().split(Pattern.quote("//$$$//"));
			List<Values> vals = new ArrayList<Values>();
			Values summ = new Values();
			if(type==0){
				summ.add(regSum(tuples));
				vals.add(summ);
			}
			else{
				multBigInt(tuples);
			}String[] encUse= tuples[tuples.length-1].split(" ");
			for(int k =0;k<encUse.length;k++){
				encryptionData.put(encUse[k], encryptionData.get(encUse[k])+1);
			}updateData(null);
			return vals;
		}else {
			return new ArrayList<Values>();
		}
	}

	public BigInteger multBigInt(String[] tuples){
		counter++;
		//System.out.println("Value: "+tuples[attribute]);
		if(counter==size){
			counter = 0;
			BigInteger ret = cryptoSum.multiply(new BigInteger(tuples[attribute]));
			cryptoSum=BigInteger.ONE;
			return ret;
		}else{
			cryptoSum.multiply(new BigInteger(tuples[attribute]));
			return BigInteger.ONE.multiply(new BigInteger("-1"));
		}
	}

	public int regSum(String[] tuple) {
		counter++;
		sum += Integer.parseInt(tuple[attribute]);
		if(counter == size) {
			counter = 0;
			int sm = sum;
			sum = 0;
			return sm;
		}else {
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
		Integer receivedRegSum = (Integer) (receivedStateValues.get(0)).get(0);
		sum += receivedRegSum;		
		cryptoSum.add(new BigInteger(receivedStateValues.get(0).get(1).toString()));
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues = stateValues;
	}

	@Override
	public List<Values> getStateValues() {
		stateValues.clear();
		Values newRegSum = new Values();
		newRegSum.add(sum);
		stateValues.add(newRegSum);
		Values newCryptoSum = new Values();
		newCryptoSum.add(cryptoSum);
		stateValues.add(newCryptoSum);
		return stateValues;
	}

	public Fields getStateSchema() {
		return stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return output_schema;
	}

	public void updateData(TaskStatistics stats) {
		int CPU = 0;
		int memory = 0;
		int latency = 0;
		int throughput = 0;
		int sel = 0;
		if(stats != null) {
			String tuple = 	(float) stats.getCpuLoad() + "," + (float) stats.getMemory() + "," + 
					(int) stats.getWindowLatency() + "," + (int) stats.getWindowThroughput() + "," + 
					(float) stats.getSelectivity() +"," + 
					encryptionData.get("pln") + "," +
					encryptionData.get("DET") + "," +
					encryptionData.get("RND") + "," +
					encryptionData.get("OPE") + ","  + 
					encryptionData.get("HOM");

			dataSender.addToBuffer(tuple);
			if(statReportCount>statReportPeriod){
				encryptionData.put("pln",0);
				encryptionData.put("DET",0);
				encryptionData.put("RND",0);
				encryptionData.put("OPE",0);
				encryptionData.put("HOM",0);
				statReportCount=0;
			}
			statReportCount++;
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
		zk.getData("/SPS", 
				true, 
				getSPSCallback, 
				"/SPS/".getBytes());
	}

	private void handleUpdate(String data){
		String[] sp = data.split(",");
		if(sp[0].equalsIgnoreCase("sum")&&Integer.parseInt(sp[1])==streamId && Integer.parseInt(sp[2])==attribute){
			type = 1;
		}
	}

	@Override
	public void updateOperatorName(String operatorName) {
		this.ID = operatorName;

	}
	
	@Override
	public void reportStatisticBeforeScaleOut() {
		float CPU = (float) 0.0;
		float memory = (float) 0.0;
		int latency = 0;
		int throughput = 0;
		float sel = (float) 0.0;
		String tuple = CPU + "," + memory + "," + latency + "," + 
				throughput + "," + sel + ",0,0,0,0,0";
		dataSender.pushStatisticData(tuple.getBytes());
	}

	@Override
	public void reportStatisticBeforeScaleIn() {
		float CPU = (float) 0.0;
		float memory = (float) 0.0;
		int latency = 0;
		int throughput = 0;
		float sel = (float) 0.0;
		String tuple = CPU + "," + memory + "," + latency + "," + 
				throughput + "," + sel + ",0,0,0,0,0";
		dataSender.pushStatisticData(tuple.getBytes());
	}
	
}
