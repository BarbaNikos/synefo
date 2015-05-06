package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import gr.katsip.synefo.storm.operators.AbstractOperator;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

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
	
	private HashMap<Integer, byte[]> keys = new HashMap<Integer, byte[]>();//maps data provider to key
	
	private Map<Integer, Integer> subscriptions = new Hashtable<Integer, Integer>(); //maps data provider to permission
	
	public Client(int idd, String nme, String[] atts, String[] querie, HashMap<Integer, ArrayList<String>> schema, ArrayList<Integer> dataPs){
		id = idd;
		CPABEDecryptFile = nme+""+idd;
		dataProviders = new ArrayList<Integer>(dataPs);
		for(int i=0;i<dataProviders.size();i++){//initilize all to assume full access, until SPS says otheriwse
			subscriptions.put(dataProviders.get(i), 0);
		}

	}
	
	public void acceptSps(int perm, byte[] sp, String stream){
		//decrypt with ABE
		subscriptions.put(Integer.parseInt(stream), perm);
		
		System.out.println("UPDATED PERMISSION IN CLIENT"+id+" TO "+perm +" ON STREAM "+stream+ " CHECK "+subscriptions.get(Integer.parseInt(stream)));
		//write to file, decryot, read file in, save as bytes
		try{
			// Create file 
			FileOutputStream fstream = new FileOutputStream("tempKey"+id+".txt.cpabe");
			
			fstream.write(sp);
			//Close the output stream
			fstream.close();
		}catch (Exception e){//Catch exception if any
			System.err.println("Error in client writitng temp key: " + e.getMessage());
		}

		//Decrypt
		System.out.println("File: "+CPABEDecryptFile+" "+id);
		String[] privKey= new String[4];//wrong
		privKey[0]="/home/cpabe-0.11/cpabe-dec";
		privKey[1]="pub_key";
		privKey[2]=CPABEDecryptFile;
		privKey[3]="tempKey"+id+".txt.cpabe";
		//sendSps(1,determineKey,privKey);
		try {
			Process pr =Runtime.getRuntime().exec(privKey);
			BufferedReader in = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
			System.out.println("DEC ERROR CLIENT "+in.readLine());
		} catch (IOException e) {
			System.out.println("Error in Data Provider: "+id+". CPABE command error, could not execute commands.");
			e.printStackTrace();
		}

		//read new key
		byte[] detainBytes={};
		try {
			FileInputStream fstream = new FileInputStream("tempKey"+id+".txt");
			DataInputStream in = new DataInputStream(fstream);

			detainBytes = new byte[in.available()];
			System.out.println(in.available());
			in.readFully(detainBytes);
			in.close();
			
		} catch (FileNotFoundException e) {
			System.out.println("Error in Client: "+id+". File: tempKey.txt not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error in Client: "+id+". Reading File: tempKey.txt error, good luck.");
			e.printStackTrace();
		}
		if(detainBytes.length>0){
		//	System.out.println("KEY IN CLIENT: "+(new String(detainBytes)));
			keys.put(Integer.parseInt(stream), detainBytes);
		}
		else{
			System.out.println("ERROR: new Key not read in client "+id);
		}
	}

	@Override
	public void init(List<Values> stateValues) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setStateSchema(Fields stateSchema) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOutputSchema(Fields output_schema) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Values> execute(Fields fields, Values values) {
		String reduce = values.get(2).toString().replaceAll("\\[", "").replaceAll("\\]","");
		//System.out.println(reduce);
		String[] tuples = reduce.split(",");
		if(tuples[0].equalsIgnoreCase("SPS")){
			//tuples[2]),key, tuples[3]);
			acceptSps(Integer.parseInt(tuples[1]), (byte[]) values.get(3), tuples[3] );
		}
		else{
			if(counter>displayCount){
				counter=0;
				currentTuple=values.get(2).toString();
			}
		}
		counter++;
		return null;
	}

	@Override
	public List<Values> getStateValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getStateSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mergeState(Fields receivedStateSchema,
			List<Values> receivedStateValues) {
		// TODO Auto-generated method stub
		
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
		byte[] plainText=null;
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
		int remove  = plainText[plainText.length-2];
		byte[] ret = new byte[plainText.length-remove];
		for(int i=0;i<ret.length;i++){
			ret[i]=plainText[i];
		}
		return plainText;
	}
	
	
}
