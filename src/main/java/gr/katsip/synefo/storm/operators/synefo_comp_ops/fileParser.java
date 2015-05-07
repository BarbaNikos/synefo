package gr.katsip.synefo.storm.operators.synefo_comp_ops;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class fileParser {
	String fileName;
	String[] dataProviders;
	String[] attributes;
	String[] policies;
	public fileParser(String fileName){
		this.fileName = fileName;
		dataProviders = null;
		attributes = null;
		policies = null;
		initDataSender dataBase = new initDataSender();
		parseFile();
		dataBase.connectToDB();
		dataBase.providerInfoSender(dataProviders.length, policies, dataProviders, attributes);
	}
	
	public void parseFile(){
		BufferedReader br =null;
		try {
			br = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			System.out.println("fileParser.java: Error opening file "+fileName);
			e.printStackTrace();
		}
		 
		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				/**
				 * Processes line by line
				 */
				String[] newLine = line.split(";"); 
				if(newLine[0].equalsIgnoreCase("data_providers")){
					dataProviders = newLine[1].split(",");
				}else if(newLine[0].equalsIgnoreCase("attributes")){
					attributes = newLine[1].split(",");
				}else if(newLine[0].equalsIgnoreCase("policies")){
					policies = newLine[1].split(",");
				}
			}
		} catch (IOException e) {
			System.out.println("fileParser.java: Error reading line in file "+fileName);
			e.printStackTrace();
		}
	 
		try {
			br.close();
		} catch (IOException e) {
			System.out.println("fileParser.java: Error closing file "+fileName);
			e.printStackTrace();
		}
	}
}
