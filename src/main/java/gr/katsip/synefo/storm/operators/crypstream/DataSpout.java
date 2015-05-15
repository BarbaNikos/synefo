package gr.katsip.synefo.storm.operators.crypstream;

import gr.katsip.synefo.storm.producers.AbstractTupleProducer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Queue;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class DataSpout implements AbstractTupleProducer, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1013197213755235165L;
	
	private Fields output_schema;
	
	public static Logger LOG = Logger.getLogger(TestWordSpout.class);
	
	boolean _isDistributed;
	
	SpoutOutputCollector _collector;
	
	int lineCounter=0;
	
	public int id;
	
	public final int numTags=5;
	
	public int streamId;
	
	private boolean init = true;
	
	public byte[] determineKey;
	
	public int protectedAttribute = 0;
	
	public static final  Category log = Category.getInstance(DataSpout.class);;
	
	public  String LOG_PROPERTIES_FILE = "lib/Log4J.properties";
	
	ArrayList<ArrayList<String>> allAttributes = new ArrayList<ArrayList<String>>();
	
	private ArrayList<Integer> perms = new ArrayList<Integer>();
	
	int time;
	
	Queue<Values> tpls = new PriorityQueue<Values>();

	public DataSpout() {
		this(true);
	}

	public DataSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
	}

	public DataSpout(int idd, int streamIdd, int spreadTime, ArrayList<Integer> permiss,ArrayList<ArrayList<String>> allAtts, String pubKey){
		this(true);
		//TODO: add call to twitter generator.
		streamId=streamIdd;
		id=idd;
		determineKey = new byte[16];
		SecureRandom r = new SecureRandom();
		perms = new ArrayList<Integer>(permiss);
		r.nextBytes(determineKey);
		time = spreadTime;
		allAttributes=new ArrayList<ArrayList<String>>(allAtts);
		//	Paillier paillier = new Paillier();
		try{
			// Create file 
			FileWriter fstream = new FileWriter("pub_key_"+id);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(pubKey);
			//Close the output stream
			out.close();
		}catch (Exception e){//Catch exception if any
			System.err.println("Error in data provider writitng public key: " + e.getMessage());
		}
		log.info("Starting Data Provider "+id);
	}

	/***********************************************************************************************
	 * Author: Cory Thoma 3/26/14 
	 * encryptDetermine(byte[] plaintext)  takes the tuple to be encrypted
	 * returns: encrypted tuple + streamid
	 * 
	 * The purpose of this method is to encrypt the tuple (minus the stream id) such that the 
	 * resulting tuple is encrypted such that two equal values correspond to the same encryption
	 **********************************************************************************************/
	public byte[] encryptDetermine(byte[] plainText){//select, project, equijoin, count, distinct...
		boolean isSize=true;
		byte[] newPlainText=null;
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
			System.out.println("Encryption Error 1 at Determine Data Provider: "+id);
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			System.out.println("Encryption Error 2 at Determine Data Provider: "+id);
			e.printStackTrace();
		}
		SecretKeySpec k =  new SecretKeySpec(determineKey, "AES");
		try {
			c.init(Cipher.ENCRYPT_MODE, k);
		} catch (InvalidKeyException e) {
			System.out.println("Encryption Error 3 at Determine Data Provider: "+id);
			e.printStackTrace();
		}
		try {
			if(isSize){
				cipherText = c.doFinal(plainText);
			}else{
				cipherText = c.doFinal(newPlainText);
			}
		} catch (IllegalBlockSizeException e) {
			System.out.println("Encryption Error 4 at Determine Data Provider: "+id);
			e.printStackTrace();
		} catch (BadPaddingException e) {
			System.out.println("Encryption Error 5 at Determine Data Provider: "+id);
			e.printStackTrace();
		}
		return cipherText;
	}

	public byte[] sendKey(byte[] Key, ArrayList<String> attributes){
		byte[] newKey = null;
		return newKey;
	}
	@Override
	public Values nextTuple() {//tuples are of the form {id,encryption type, {data}, time stamp}
		Utils.sleep(time);
		if(init){		
			for(int i=0;i<perms.size();i++){	
				if(perms.get(i)==1){
					sendSps(1,determineKey,new String[]{"Attr1"},new int[]{1});
				}
			}
			init=false;
		}
		String filename = "outTweet.txt";
		try {
			FileInputStream fstream = new FileInputStream(filename);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			boolean afterFirst=false;
			long startT=System.currentTimeMillis();
			System.out.println("START "+System.currentTimeMillis());
			while ((strLine = br.readLine()) != null) {
				if(afterFirst) {
					String use = strLine.replaceAll("\\{","");
					use = use.replaceAll("\\}","");
					//System.out.println(use);
					String[] tples = use.split("\\|"); 
					String[] finalTples = new String[numTags+3];
					finalTples[0]=""+id;
					finalTples[1]=""+0;
					finalTples[numTags+2]=""+System.currentTimeMillis();
					for(int y=0;y<numTags;y++){
						if(y<tples.length){
							finalTples[2+y]=tples[y];
						}
						else{
							finalTples[2+y]="ntple";
						}
					}
					String sendTple="";
					for(int y=0;y<finalTples.length;y++){
						if(y<finalTples.length-1){
							sendTple=sendTple+finalTples[y]+",";
						}
						else{
							sendTple=sendTple+finalTples[y];
						}
					}
					for(int k=0;k<perms.size();k++){
						if(perms.get(k)==0){
							_collector.emit(new Values(sendTple, 0));
						}
						else if(perms.get(k)==1){
							byte[] enc={};
							finalTples[0]=""+id;
							finalTples[1]=""+1;
							String message=id+","+1;
							for(int i=2;i<finalTples.length;i++){
								if(i-2==protectedAttribute){
									enc = encryptDetermine(finalTples[i].getBytes());
									message=message+","+(new String(enc));
									continue;
								}
								message = message+","+finalTples[i];
							}
							//System.out.println("MESSAGE: "+message);
							Values newVal = new Values();
							newVal.add(message);
							if(tpls.isEmpty()){
								br.close();
								in.close();
								fstream.close();
								return(newVal);
							}
							else{
								br.close();
								in.close();
								fstream.close();
								return tpls.remove();
							}
						}
					}
					
				}
				afterFirst=true;
			}
			br.close();
			in.close();
			fstream.close();
			System.out.println("SENT LAST: "+(System.currentTimeMillis()-startT));
		} catch (FileNotFoundException e) {
			System.out.println("Error in Data Provider: "+id+". File: "+filename+" not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error in Data Provider: "+id+". Reading File: "+filename+" error, good luck.");
			e.printStackTrace();
		}
		return null;
	}


	public void sendSps(int permission, byte[] key, String[] attributes, int[] whoCares){
		String whoCaresString = "";
		for(int i=0;i<whoCares.length;i++){
			if(i==whoCares.length-1){
				whoCaresString=whoCares[i]+"";
			}
			else{
				whoCaresString=whoCares[i]+";";
			}
		}
		String tuple = "SPS,"+whoCaresString+","+permission+","+id;
		try{
			// Create file 
			FileOutputStream fstream = new FileOutputStream("new_key_"+id);			
			fstream.write(key);
			fstream.close();
		}catch (Exception e){
			System.err.println("Error in client writitng temp key: " + e.getMessage());
		}
		long curTime=System.currentTimeMillis();
		String[] newKey = new String[(2*attributes.length)+2];
		newKey[0]="/home/cpabe-0.11/cpabe-enc";
		newKey[1]="pub_key";
		newKey[2]="new_key_"+id;
		for(int k=0;k<(attributes.length*2)-1;k+=2){
			newKey[k+3]=attributes[k];
			if((k+4)<(attributes.length*2)+2){
				newKey[k+4]="and";
			}
		}
		try {
			Process pr =Runtime.getRuntime().exec(newKey);
			BufferedReader in = new BufferedReader(new InputStreamReader(pr.getErrorStream()));			
			System.out.println("ENC ERROR "+in.readLine());
		} catch (IOException e) {
			System.out.println("Error in Data Provider. CPABE command error, could not execute commands.");
			e.printStackTrace();
		}
		System.out.println("Encryption CPABE: "+(System.currentTimeMillis()-curTime));
		String finalKey="";
		byte[] detainBytes={};
		try {
			FileInputStream fstream = new FileInputStream("new_key_"+id+".cpabe");
			DataInputStream in = new DataInputStream(fstream);

			detainBytes = new byte[in.available()];
			System.out.println(in.available());
			in.readFully(detainBytes);
			in.close();

			finalKey = new String(detainBytes, 0, detainBytes.length);
			System.out.println("String Length finalKey: "+finalKey.length());
		} catch (FileNotFoundException e) {
			System.out.println("Error in Data Provider. File: new_key_"+id+".cpabe not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error in Data Provider. Reading File:  new_key_"+id+".cpabe error, good luck.");
			e.printStackTrace();
		}
		Values newSP = new Values();
		newSP.add(tuple);
		tpls.add(newSP);
	}

	@Override
	public void setSchema(Fields fields) {
		output_schema = new Fields(fields.toList());
		
	}

	@Override
	public Fields getSchema() {
		return output_schema;
	}


}