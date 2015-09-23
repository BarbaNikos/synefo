package gr.katsip.synefo.balancer;

import gr.katsip.synefo.TopologyXMLParser.ResourceThresholdParser;

/**
 * Created by nick on 9/23/15.
 */
public class App {
    public static void main(String[] args) {
        if(args.length < 2) {
            System.err.println("arguments: <resource-file-thresholds.xml> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN>");
            System.exit(1);
        }
        ResourceThresholdParser parser = new ResourceThresholdParser();
        parser.parseThresholds(args[0]);
        String zookeeperAddress = args[1];
        BalanceServer server = new BalanceServer(zookeeperAddress, parser.get_thresholds(), null);
        server.runServer();
    }
}
