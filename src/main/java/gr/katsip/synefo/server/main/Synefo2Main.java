package gr.katsip.synefo.server.main;

import gr.katsip.synefo.TopologyXMLParser.ResourceThresholdParser;
import gr.katsip.synefo.server2.Synefo;

public class Synefo2Main {

	public static void main(String[] args) {
		if(args.length < 3) {
			System.err.println("arguments: <resource-file-thresholds.xml> <zookeeper-ip> <zookeeper-port>");
			System.exit(1);
		}
		ResourceThresholdParser parser = new ResourceThresholdParser();
		parser.parseThresholds(args[0]);
		String zooIP = args[1];
		Integer zooPort = Integer.parseInt(args[2]);
		Synefo synEFO = new Synefo(zooIP, zooPort, parser.get_thresholds(), null);
		synEFO.runServer();
	}

}
