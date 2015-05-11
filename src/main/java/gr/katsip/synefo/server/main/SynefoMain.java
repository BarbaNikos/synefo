package gr.katsip.synefo.server.main;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.synefo.TopologyXMLParser.ResourceThresholdParser;
import gr.katsip.synefo.server.Synefo;

public class SynefoMain {
	public static void main( String[] args ) {
		if(args.length < 3) {
			System.err.println("arguments: <resource-file-thresholds.xml> <zookeeper-ip> <zookeeper-port>");
			System.exit(1);
		}
		ResourceThresholdParser parser = new ResourceThresholdParser();
		parser.parseThresholds(args[0]);
		String zooIP = args[1];
		Integer zooPort = Integer.parseInt(args[2]);
		CEStormDatabaseManager ceDb = new CEStormDatabaseManager(
				"jdbc:mysql://db10.cs.pitt.edu:3306/ce_storm", "stormxl_user", "bMWBdTdMyf7uuB69");
		Synefo synEFO = new Synefo(zooIP, zooPort, parser.get_thresholds(), ceDb);
		synEFO.runServer();
	}
}
