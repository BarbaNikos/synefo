package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import gr.katsip.synefo.storm.operators.AbstractOperator;

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

public class Client implements AbstractOperator, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8137112194881808673L;

	private int id;

	private String CPABEDecryptFile;

	private int counter=0;

	private int displayCount=1000;

	public String currentTuple;

	private ArrayList<Integer> dataProviders;

	private Map<Integer, HashMap<Integer,byte[]>> keys = new HashMap<Integer, HashMap<Integer,byte[]>>();//maps data provider to key

	private Map<Integer, HashMap<Integer,Integer>> subscriptions = new Hashtable<Integer, HashMap<Integer, Integer>>(); //maps data provider to permission

	private List<Values> stateValues;

	private Fields stateSchema;

	private Fields output_schema;

	private int schemaSize;
	
	private String zooIP;
	
	private int zooPort;
	
	private SPSUpdater spsUpdate =null;
	
	private BigInteger n;
	private BigInteger nsquare;
	private BigInteger g;
	private BigInteger lambda;
	
	private int count;

	public Client(int idd, String nme, String[] atts, ArrayList<Integer> dataPs, int schemaSiz, String zooIP, int zooPort){
		id = idd;
		CPABEDecryptFile = nme+""+idd;
		dataProviders = new ArrayList<Integer>(dataPs);
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
		//error if coming form multiple sources
	//	System.out.println("fields "+values.get(0));
		String reduce = values.get(0).toString().replaceAll("\\[", "").replaceAll("\\]","");
		//System.out.println(reduce);
		String[] tuples = reduce.split(",");
		if(tuples[0].equalsIgnoreCase("SPS")){
			processSps(tuples);
		}
		else{
			if(counter>0){
				counter=0;
				currentTuple=values.get(0).toString();
				//System.out.println(currentTuple);
				processNormal(currentTuple);
			}
		}
		counter++;
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
		// TODO Auto-generated method stub

	}
	
	public void processNormal(String tuple){
		String[] tuples = tuple.split(Pattern.quote("//$$$//"));
		String finalTuple="";
		//System.out.println("pl: "+tuples.length);
		int clientID= Integer.parseInt(tuples[0]);
		//System.out.println("tup: "+tuple +"size "+tuples.length);
		for(int i=1;i<tuples.length;i++){
			if(subscriptions.get(clientID).get(i)==0){
				finalTuple=finalTuple+", "+tuples[i];
			//	System.out.println(finalTuple);
			}else if(subscriptions.get(clientID).get(i)==1){
				
			}else if(subscriptions.get(clientID).get(i)==2){
			 String result = new String(decryptDetermine(tuples[i].getBytes(),keys.get(clientID).get(i)));
				finalTuple=finalTuple+", "+result;
				System.out.println(finalTuple);
			}else if(subscriptions.get(clientID).get(i)==3){
				
			}else if(subscriptions.get(clientID).get(i)==4){
				System.out.println("SUM: "+Decryption(new BigInteger(tuples[i])));
			}
		}
	}

	public void processSps(String[] tuple){
		//String tuple = "SPS", StreamId, permission, clientID, field, key;
		int clientId = Integer.parseInt(tuple[3]);
		int field = Integer.parseInt(tuple[4]);
		int permission = Integer.parseInt(tuple[2]);
		System.out.println("Client "+id+" recieved permission "+permission+" for stream "+ clientId+"."+" field "+field);
		subscriptions.get(clientId).put(field,permission);
		if(permission == 0){//plaintext
			keys.get(clientId).put(field,"".getBytes());
		}else if(permission == 1){//rnd
			System.out.println("RND KEY: "+tuple[5]);			
			keys.get(clientId).put(field,tuple[5].getBytes());
		}else if(permission == 2){//det
			byte[] newDetKey = null;
			try {
				 newDetKey = Hex.decodeHex(tuple[5].toCharArray());
			} catch (DecoderException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String newUpdate = "select,"+new String(Hex.encodeHex(encryptDetermine("2",newDetKey)));
			spsUpdate.createChildNode(newUpdate.getBytes());
			System.out.println("DET KEY: "+tuple[5]);
			byte[] newK = null;
			try {
				newK = Hex.decodeHex(tuple[5].toCharArray());
			} catch (DecoderException e) {
				System.out.println("Failed to decode DET key in Client");
				e.printStackTrace();
			} 
			keys.get(clientId).put(field,newK);
		}else if(permission == 3){//ope
			System.out.println("OPE KEY: "+tuple[5]);
			keys.get(clientId).put(field,tuple[5].getBytes());
		}else if(permission == 4){//hom
			System.out.println("HOM KEY: "+tuple[5]);
			keys.get(clientId).put(field,tuple[5].getBytes());
//			/String ret = n.toString()+","+nsquare.toString()+","+g.toString()+","+lambda.toString();
			n = new BigInteger(tuple[5]);
			nsquare = new BigInteger(tuple[6]);
			g = new BigInteger(tuple[7]);
			lambda = new BigInteger(tuple[8]);
			String newUpdate = "sum,paillier";
			spsUpdate.createChildNode(newUpdate.getBytes());
		}
	}

	public BigInteger Decryption(BigInteger c) {
		BigInteger u = g.modPow(lambda, nsquare).subtract(BigInteger.ONE).divide(n).modInverse(n);
		return c.modPow(lambda, nsquare).subtract(BigInteger.ONE).divide(n).multiply(u).mod(n);
	}
	
	public byte[] encryptDetermine(String plnText, byte[] key){
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
			count=counter;
		}
		byte[] cipherText=null;
		Cipher c=null;
		try {
			c = Cipher.getInstance("AES/ECB/NoPadding");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Encryption Error 1 at Determine Data Provider: ");
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			System.out.println("Encryption Error 2 at Determine Data Provider: ");
			e.printStackTrace();
		}
		SecretKeySpec k =  new SecretKeySpec(key, "AES");
		try {
			c.init(Cipher.ENCRYPT_MODE, k);
		} catch (InvalidKeyException e) {
			System.out.println("Encryption Error 3 at Determine Data Provider: ");
			e.printStackTrace();
		}
		try {
			if(isSize){
				cipherText = c.doFinal(plainText);
			}else{
				cipherText = c.doFinal(newPlainText);
			}
		} catch (IllegalBlockSizeException e) {
			System.out.println("Encryption Error 4 at Determine Data Provider: ");
			e.printStackTrace();
		} catch (BadPaddingException e) {
			System.out.println("Encryption Error 5 at Determine Data Provider: ");
			e.printStackTrace();
		}
		return cipherText;
	}

	
	public void setABEDecrypt(byte[] ABEKey){
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

	public byte[] decryptDetermine(byte[] cipherText, byte[] determineKey){//select, project, equijoin, count, distinct...
		byte[] plainText=new byte[16];
		Cipher c=null;
		try {
			c = Cipher.getInstance("AES/ECB/NoPadding");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Decryption Error 1 at Determine Data Provider: "+id);
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			System.out.println("Decryption Error 2 at Determine Data Provider: "+id);
			e.printStackTrace();
		}
		SecretKeySpec k =  new SecretKeySpec(determineKey, "AES");
		try {
			c.init(Cipher.DECRYPT_MODE, k);
		} catch (InvalidKeyException e) {
			System.out.println("Decryption Error 3 at Determine Data Provider: "+id);
			e.printStackTrace();
		}
		try {
			plainText = c.doFinal(plainText);
		} catch (IllegalBlockSizeException e) {
			System.out.println("Decryption Error 4 at Determine Data Provider: "+id);
			e.printStackTrace();
		} catch (BadPaddingException e) {
			System.out.println("Decryption Error 5 at Determine Data Provider: "+id);
			e.printStackTrace();
		}
		//int remove  = plainText[plainText.length-2];
		//byte[] ret = new byte[plainText.length-remove];
		//for(int i=0;i<ret.length;i++){
	//		ret[i]=plainText[i];
	//	}
		return plainText;
	}


}