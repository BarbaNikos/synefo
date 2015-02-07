package gr.katsip.synefo.server.main;

import gr.katsip.synefo.TopologyXMLParser.ResourceThresholdParser;
import gr.katsip.synefo.server.Synefo;

public class SynEFOMain {
    public static void main( String[] args ) {
//    	if(args.length < 1) {
//    		System.err.println("No resource usage threshold found.");
//    		System.err.println("Please provide one as a command-line argument.");
//    		System.err.println("usage: ./SynEFOMain <resource-file-thresholds.xml>).");
//    		System.exit(1);
//    	}
    	ResourceThresholdParser parser = new ResourceThresholdParser();
//    	parser.parseThresholds(args[0]);
    	parser.parseThresholds("conf/resource_thresholds.xml");
    	Synefo synEFO = new Synefo("127.0.0.1", 2181, parser.get_thresholds());
		synEFO.runServer();
    }
}
