package gr.katsip.synefo.storm.operators.crypstream;

import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.storm.operators.AbstractStatOperator;

import java.io.FileOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class Client implements AbstractStatOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8137112194881808673L;

	private String ID;

	private String CPABEDecryptFile;

	private int statReportPeriod;

	public String currentTuple;

	private ArrayList<Integer> dataProviders;

	private HashMap<String, Integer> encryptionData = new HashMap<String,Integer>();

	private Map<Integer, HashMap<Integer,byte[]>> keys = new HashMap<Integer, HashMap<Integer,byte[]>>();//maps data provider to key

	private Map<Integer, HashMap<Integer,Integer>> subscriptions = new Hashtable<Integer, HashMap<Integer, Integer>>(); //maps data provider to permission

	private List<Values> stateValues;

	private Fields stateSchema;

	private Fields output_schema;

	private DataCollector dataSender = null;

	private int schemaSize;

	private String zooIP;

	private int zooPort;

	private SPSUpdater spsUpdate =null;

	private BigInteger n;

	private BigInteger nsquare;

	private BigInteger g;

	private BigInteger lambda;

	private int statReportCount;

	ArrayList<String> predicates = null;

	public Client(String idd, String nme, String[] atts, ArrayList<Integer> dataPs, int schemaSiz, String zooIP, int zooPort, ArrayList<String> preds, int statReportPeriod) {
		ID = idd;
		CPABEDecryptFile = nme+""+idd;
		dataProviders = new ArrayList<Integer>(dataPs);
		encryptionData.put("pln",0);
		encryptionData.put("DET",0);
		encryptionData.put("RND",0);
		encryptionData.put("OPE",0);
		encryptionData.put("HOM",0);
		predicates = new ArrayList<String>(preds);
		this.schemaSize=schemaSiz;
		this.zooIP=zooIP;
		this.zooPort=zooPort;
		for(int i=0;i<dataProviders.size();i++){//initilize all to assume full access, until SPS says otheriwse
			subscriptions.put(dataProviders.get(i), new HashMap<Integer,Integer>());
			//System.out.println("made room for "+dataProviders.get(i));
			keys.put((dataProviders.get(i)), new HashMap<Integer,byte[]>());
			for(int y=0;y<schemaSize;y++){
				subscriptions.get((dataProviders.get(i))).put(y,0);
				keys.get((dataProviders.get(i))).put(y,"".getBytes());
			}
		}
		this.statReportPeriod = statReportPeriod;
		this.statReportCount = 0;
	}

	@Override
	public void init(List<Values> stateValues) {
		this.stateValues= stateValues;

	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		this.stateSchema=stateSchema;

	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		this.output_schema = output_schema;

	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		if (spsUpdate == null){
			spsUpdate = new SPSUpdater(zooIP,zooPort);
		}
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		String reduce = values.get(0).toString().replaceAll("\\[", "").replaceAll("\\]","");
		String[] tuples = reduce.split(",");
		if(tuples[0].equalsIgnoreCase("SPS")) {
			processSps(tuples);
		}else {
			if(statReportCount > statReportPeriod) {
				currentTuple=values.get(0).toString();
				//System.out.println(currentTuple);
				processNormal(currentTuple);
				String[] encUse= tuples[tuples.length-1].split(" ");
				for(int k = 0; k < encUse.length; k++) {
					encryptionData.put(encUse[k], encryptionData.get(encUse[k])+1);
				}
			}
		}
		if(statReportCount > statReportPeriod) {
			updateData(null);
			statReportCount = 0;
		}else {
			statReportCount += 1;
		}
		return new ArrayList<Values>();
	}
	
	@Override
	public List<Values> execute(TaskStatistics statistics, Fields fields,
			Values values) {
		if (spsUpdate == null) {
			spsUpdate = new SPSUpdater(zooIP,zooPort);
		}
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		String reduce = values.get(0).toString().replaceAll("\\[", "").replaceAll("\\]","");
		String[] tuples = reduce.split(",");
		if(tuples[0].equalsIgnoreCase("SPS")) {
			processSps(tuples);
		}else {
			String[] encUse= tuples[tuples.length-1].split(" ");
			for(int k = 0; k < encUse.length; k++){
				encryptionData.put(encUse[k], encryptionData.get(encUse[k])+1);
			}
			if(statReportCount > statReportPeriod) {
				currentTuple=values.get(0).toString();
				//System.out.println(currentTuple);
				processNormal(currentTuple);
			}
		}
		if(statReportCount > statReportPeriod) {
			updateData(statistics);
			statReportCount = 0;
		}else {
			statReportCount += 1;
		}
		return new ArrayList<Values>();
	}

	@Override
	public List<Values> getStateValues() {
		return this.stateValues;
	}

	@Override
	public Fields getStateSchema() {
		return this.stateSchema;
	}

	@Override
	public Fields getOutputSchema() {
		return this.output_schema;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		/**
		 * Only one client exists per query and is not allowed to scale-out/in
		 */
	}

	public void processNormal(String tuple) {
		String[] tuples = tuple.split(Pattern.quote("//$$$//"));
		String finalTuple="";
		int clientID = Integer.parseInt(tuples[0]);
//		System.out.println("tup: "+tuple);
		for(int i=1;i<tuples.length;i++) {
			//System.out.println(subscriptions.get(clientID).get(i)+" "+i);
			if(subscriptions.get(clientID).get(i) == 0) {
				finalTuple=finalTuple+", "+tuples[i];
			}else if(subscriptions.get(clientID).get(i) == 1) {

			}else if(subscriptions.get(clientID).get(i) == 2) {
				String result = new String(decryptDetermine(tuples[i].getBytes(),keys.get(clientID).get(i)));
				finalTuple = finalTuple + ", " + result;
				//System.out.println(finalTuple);
			}else if(subscriptions.get(clientID).get(i) == 3) {

			}else if(subscriptions.get(clientID).get(i) == 4) {
				//System.out.println("SUM: "+Decryption(new BigInteger(tuples[i])));
			}
		}
	}

	public void processSps(String[] tuple) {
		//String tuple = "SPS", StreamId, permission, clientID, field, key;
		int clientId = Integer.parseInt(tuple[3]);
		int field = Integer.parseInt(tuple[4]);
		int permission = Integer.parseInt(tuple[2]);
		System.out.println("Client "+ ID +" recieved permission "+permission+" for stream "+ clientId+"."+" field "+field);
		subscriptions.get(clientId).put(field,permission);
		if(permission == 0) {
			//plaintext
			keys.get(clientId).put(field,"".getBytes());
		}else if(permission == 1) {
			//rnd
//			System.out.println("RND KEY: "+tuple[5]);			
			keys.get(clientId).put(field,tuple[5].getBytes());
		}else if(permission == 2) {
			//det
			byte[] newDetKey = null;
			try {
				newDetKey = Hex.decodeHex(tuple[5].toCharArray());
			} catch (DecoderException e1) {
				e1.printStackTrace();
			}
			//System.out.println("NEW DET KEY: "+new String(newDetKey)+" tup "+new String(tuple[5].toCharArray()));
			//predicates clientID,attribute,predicate
			for(int i=0;i<predicates.size();i++) {
				String[] pred = predicates.get(i).split(",");
				if(clientId==Integer.parseInt(pred[0])&& field==Integer.parseInt(pred[1])-1){
					//id, attribute, predicate
					System.out.println("SPS CREATED ID: "+pred[0]+", attribute "+pred[1]+" encrypted "+pred[2]);
					String newUpdate = "select,"+pred[0]+","+(Integer.parseInt(pred[1]))+"," + String.valueOf(Hex.encodeHex(encryptDetermine(pred[2],newDetKey)));
					spsUpdate.createChildNode(newUpdate.getBytes());
//					try {
//						Thread.sleep(10);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
				//	newUpdate = "count," + pred[0] + "," + pred[1] + "," + new String(Hex.encodeHex(encryptDetermine(pred[2],newDetKey)));
				//	spsUpdate.createChildNode(newUpdate.getBytes());
				}
			}
//			System.out.println("DET KEY: "+tuple[5]);
			byte[] newK = null;
			try {
				newK = Hex.decodeHex(tuple[5].toCharArray());
			} catch (DecoderException e) {
				System.err.println("Failed to decode DET key in Client");
				e.printStackTrace();
			} 
			keys.get(clientId).put(field,newK);
		}else if(permission == 3) {
			//ope
//			System.out.println("OPE KEY: "+tuple[5]);
			keys.get(clientId).put(field,tuple[5].getBytes());
			for(int i=0;i<predicates.size();i++) {
				String[] pred = predicates.get(i).split(",");
				if(clientId==Integer.parseInt(pred[0])&& field==Integer.parseInt(pred[1])-1){
					String newUpdate ="select,"+pred[0]+","+pred[1]+","+(pred[2]+Integer.parseInt(tuple[5]));
					spsUpdate.createChildNode(newUpdate.getBytes());
				}
			}
		}else if(permission == 4) {
			//hom
//			System.out.println("HOM KEY: "+tuple[5]);
			keys.get(clientId).put(field,tuple[5].getBytes());
			//			/String ret = n.toString()+","+nsquare.toString()+","+g.toString()+","+lambda.toString();
			n = new BigInteger(tuple[5]);
			nsquare = new BigInteger(tuple[6]);
			g = new BigInteger(tuple[7]);
			lambda = new BigInteger(tuple[8]);
			//Stream ID, BoltID, predicate
			for(int i=0;i<predicates.size();i++) {
				String[] pred = predicates.get(i).split(",");
				if(clientId==Integer.parseInt(pred[0])&& field==Integer.parseInt(pred[1])-1){
					String newUpdate ="sum,"+pred[0]+","+pred[1]+",paillier";
					spsUpdate.createChildNode(newUpdate.getBytes());
				}
			}
		}
	}

	public BigInteger Decryption(BigInteger c) {
		BigInteger u = g.modPow(lambda, nsquare).subtract(BigInteger.ONE).divide(n).modInverse(n);
		return c.modPow(lambda, nsquare).subtract(BigInteger.ONE).divide(n).multiply(u).mod(n);
	}

	public byte[] encryptDetermine(String plnText, byte[] key) {
		boolean isSize=true;
		byte[] newPlainText=null;
		byte[] plainText = plnText.getBytes();
		if(plainText.length%16!=0){
			isSize=false;
			int diff =16-plainText.length%16;
			newPlainText = new byte[plainText.length+(diff)];
			for(int i=0;i<plainText.length;i++){
				newPlainText[i]=plainText[i];
			}
			int counter=0;
			while(counter!=diff){
				if(counter==diff-1){
					newPlainText[plainText.length+counter]=(byte) diff;
				}
				newPlainText[plainText.length+counter]=0;
				counter++;
			}
		}
		byte[] cipherText=null;
		Cipher c=null;
		try {
			c = Cipher.getInstance("AES/ECB/NoPadding");
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Encryption Error 1 at Determine Data Provider: ");
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			System.err.println("Encryption Error 2 at Determine Data Provider: ");
			e.printStackTrace();
		}
		SecretKeySpec k =  new SecretKeySpec(key, "AES");
		try {
			c.init(Cipher.ENCRYPT_MODE, k);
		} catch (InvalidKeyException e) {
			System.err.println("Encryption Error 3 at Determine Data Provider: ");
			e.printStackTrace();
		}
		try {
			if(isSize){
				cipherText = c.doFinal(plainText);
			}else{
				cipherText = c.doFinal(newPlainText);
			}
		} catch (IllegalBlockSizeException e) {
			System.err.println("Encryption Error 4 at Determine Data Provider: ");
			e.printStackTrace();
		} catch (BadPaddingException e) {
			System.err.println("Encryption Error 5 at Determine Data Provider: ");
			e.printStackTrace();
		}
		return cipherText;
	}


	public void setABEDecrypt(byte[] ABEKey) {
		//open file for writing, write key to priv_key, return
		try{
			// Create file 
			FileOutputStream fstream = new FileOutputStream(CPABEDecryptFile);
			//System.out.println("CHECK LENGTH :"+ABEKey.length);
			fstream.write(ABEKey);
			//Close the output stream
			fstream.close();
		}catch (Exception e){//Catch exception if any
			System.err.println("Error in client writitng temp key: " + e.getMessage());
		}
	}

	public byte[] decryptDetermine(byte[] cipherText, byte[] determineKey){
		//select, project, equijoin, count, distinct...
		byte[] plainText=new byte[16];
		Cipher c=null;
		try {
			c = Cipher.getInstance("AES/ECB/NoPadding");
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Decryption Error 1 at Determine Data Provider: "+ID);
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			System.err.println("Decryption Error 2 at Determine Data Provider: "+ID);
			e.printStackTrace();
		}
		SecretKeySpec k =  new SecretKeySpec(determineKey, "AES");
		try {
			c.init(Cipher.DECRYPT_MODE, k);
		} catch (InvalidKeyException e) {
			System.err.println("Decryption Error 3 at Determine Data Provider: "+ID);
			e.printStackTrace();
		}
		try {
			plainText = c.doFinal(plainText);
		} catch (IllegalBlockSizeException e) {
			System.err.println("Decryption Error 4 at Determine Data Provider: "+ID);
			e.printStackTrace();
		} catch (BadPaddingException e) {
			System.err.println("Decryption Error 5 at Determine Data Provider: "+ID);
			e.printStackTrace();
		}
		//int remove  = plainText[plainText.length-2];
		//byte[] ret = new byte[plainText.length-remove];
		//for(int i=0;i<ret.length;i++){
		//		ret[i]=plainText[i];
		//	}
		return plainText;
	}

	@Override
	public void updateOperatorName(String operatorName) {
		this.ID = operatorName;

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
	
	@Override
	public void reportStatisticBeforeScaleOut() {
		float CPU = (float) 0.0;
		float memory = (float) 0.0;
		int latency = 0;
		int throughput = 0;
		float sel = (float) 0.0;
		String tuple = CPU + "," + memory + "," + latency + "," + 
				throughput + "," + sel + ",0,0,0,0,0";
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
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
		if(dataSender == null) {
			dataSender = new DataCollector(zooIP, zooPort, statReportPeriod, ID);
		}
		dataSender.pushStatisticData(tuple.getBytes());
	}
}