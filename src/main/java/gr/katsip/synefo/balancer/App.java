package gr.katsip.synefo.balancer;

import gr.katsip.synefo.storm.api.Pair;
import java.util.HashMap;

/**
 * Created by nick on 9/23/15.
 */
public class App {
    public static void main(String[] args) {
        if(args.length < 3) {
            System.err.println("arguments: <resource-file-thresholds.xml> <zoo-ip1:port1,zoo-ip2:port2,...,zoo-ipN:portN> <INIT_MINIMAL_RESOURCES>");
            System.exit(1);
        }
//        ResourceThresholdParser parser = new ResourceThresholdParser();
//        parser.parseThresholds(args[0]);
        String zookeeperAddress = args[1];
        boolean INIT_MINIMAL_RESOURCES = Boolean.parseBoolean(args[2]);
        HashMap<String, Pair<Number, Number>> inputRateThresholds = new HashMap<>();
        Pair<Number, Number> pair = new Pair<>();
        pair.lowerBound = new Integer(500);
        pair.upperBound = new Integer(500);
        inputRateThresholds.put("input-rate", pair);
//        BalanceServer server = new BalanceServer(zookeeperAddress, parser.get_thresholds(), null);
        BalanceServer server = new BalanceServer(zookeeperAddress, inputRateThresholds, INIT_MINIMAL_RESOURCES);
        server.runServer();
    }
}
