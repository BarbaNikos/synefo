package gr.katsip.synefo.balancer;

import gr.katsip.synefo.TopologyXMLParser.ResourceThresholdParser;
import gr.katsip.synefo.storm.api.Pair;

import java.util.HashMap;

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
        HashMap<String, Pair<String, String>> inputRateThresholds = new HashMap<>();
        Pair<String, String> pair = new Pair<>();
        pair.lowerBound = "1000";
        pair.upperBound = "1000";
        inputRateThresholds.put("input-rate", pair);
        BalanceServer server = new BalanceServer(zookeeperAddress, parser.get_thresholds(), null);
        server.runServer();
    }
}
