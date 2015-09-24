package gr.katsip.synefo.balancer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by katsip on 9/24/2015.
 */
public class Util {

    public static String serializeTopology(Map<String, List<String>> topology) {
        StringBuilder strBuild = new StringBuilder();
        Iterator<Map.Entry<String, List<String>>> itr = topology.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, List<String>> entry = itr.next();
            String task = entry.getKey();
            strBuild.append(task + "=");
            for(String downstreamTask : entry.getValue()) {
                strBuild.append(downstreamTask + ",");
            }
            if(strBuild.length() > 0 && strBuild.charAt(strBuild.length() - 1) == ',') {
                strBuild.setLength(strBuild.length() - 1);
            }
            strBuild.append("&");
        }
        if(strBuild.length() > 0 && strBuild.charAt(strBuild.length() - 1) == '&') {
            strBuild.setLength(strBuild.length() - 1);
        }
        return strBuild.toString();
    }

    public static HashMap<String, ArrayList<String>> deserializeTopology(String serializedTopology) {
        HashMap<String, ArrayList<String>> topology = new HashMap<String, ArrayList<String>>();
        String[] pairs = serializedTopology.split("&");
        for (String pair : pairs) {
            if (pair != "") {
                String[] tokens = pair.split("=");
                String key = tokens[0];
                String[] values = tokens[1].split(",");
                ArrayList<String> tasks = new ArrayList<String>();
                for(int i = 1; i < values.length; i++) {
                    tasks.add(values[i]);
                }
                topology.put(key, tasks);
            }
        }
        return topology;
    }
}
