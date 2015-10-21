package gr.katsip.deprecated.server.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import gr.katsip.deprecated.cestorm.db.CEStormDatabaseManager;
import gr.katsip.deprecated.TopologyXMLParser.ResourceThresholdParser;
import gr.katsip.deprecated.server.Synefo;

/**
 * @deprecated
 */
public class SynefoMain {
	public static void main( String[] args ) throws FileNotFoundException, IOException {
		String dbServerIp = null;
		String dbServerUser = null;
		String dbServerPass = null;
		if(args.length < 2) {
			System.err.println("arguments: <resource-file-thresholds.xml> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN> <optional: db-info-file>");
			System.exit(1);
		}
		ResourceThresholdParser parser = new ResourceThresholdParser();
		parser.parseThresholds(args[0]);
		String zooIP = args[1];
		CEStormDatabaseManager ceDb = null;
		if(args.length == 3) {
			System.out.println("Database Configuration file provided. Parsing connection information...");
			try(BufferedReader br = new BufferedReader(new FileReader(new File(args[2])))) {
			    for(String line; (line = br.readLine()) != null;) {
			    	String[] lineTokens = line.split(":");
			    	if(line.contains("db-server-ip:"))
				    	dbServerIp = "jdbc:mysql://" + lineTokens[1] + "/";
			    	else if(line.contains("db-schema-name:")) 
			    		dbServerIp = dbServerIp + lineTokens[1];
			    	else if(line.contains("db-user:"))
			    		dbServerUser = lineTokens[1];
			    	else if(line.contains("db-password:"))
			    		dbServerPass = lineTokens[1];
			    	else {
			    		System.err.println("Invalid db-info file provided. Please use proper formatted file. Format: ");
			    		System.err.println("db-server-ip:\"proper-ip-here\"");
			    		System.err.println("db-schema-name:\"proper-schema-name-here\"");
			    		System.err.println("db-user:\"proper-username-here\"");
			    		System.err.println("db-password:\"proper-user-password-here\"");
						System.exit(1);
			    	}
			    }
			}
			ceDb = new CEStormDatabaseManager(dbServerIp, 
					dbServerUser, dbServerPass);
		}else {
			ceDb = null;
		}
		Synefo synEFO = new Synefo(zooIP, parser.get_thresholds(), ceDb);
		synEFO.runServer();
	}
}
