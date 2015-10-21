package gr.katsip.deprecated.server.main;

import gr.katsip.deprecated.TopologyXMLParser.ResourceThresholdParser;
import gr.katsip.deprecated.server2.Synefo;

/**
 * @deprecated
 */
public class Synefo2Main {

	public static void main(String[] args) {
		if(args.length < 2) {
			System.err.println("arguments: <resource-file-thresholds.xml> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN>");
			System.exit(1);
		}
		ResourceThresholdParser parser = new ResourceThresholdParser();
		parser.parseThresholds(args[0]);
		String zooIP = args[1];
		Synefo synEFO = new Synefo(zooIP, parser.get_thresholds(), null);
		synEFO.runServer();
	}

}
