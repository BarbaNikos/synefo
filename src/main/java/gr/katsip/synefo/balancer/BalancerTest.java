package gr.katsip.synefo.balancer;

import gr.katsip.synefo.utils.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by katsip on 12/3/2015.
 */
public class BalancerTest {

    public static void main(String[] args) {
        HashMap<String, ArrayList<String>> topology = new HashMap<>();
        ArrayList<String> tasks;
        tasks = new ArrayList<>();
        tasks.add("cust_ord_dispatch");
        topology.put("customer", tasks);
        topology.put("order", new ArrayList<>(tasks));
        tasks = new ArrayList<>();
        tasks.add("cust_ord_join");
        topology.put("cust_ord_dispatch", tasks);
        tasks = new ArrayList<>();
        tasks.add("cust_ord_line_sup_dispatch");
        topology.put("cust_ord_join", tasks);
        tasks = new ArrayList<>();
        tasks.add("line_sup_dispatch");
        topology.put("lineitem", tasks);
        topology.put("supplier", new ArrayList<>(tasks));
        tasks = new ArrayList<>();
        tasks.add("line_sup_join");
        topology.put("line_sup_dispatch", tasks);
        tasks = new ArrayList<>();
        tasks.add("cust_ord_line_sup_dispatch");
        topology.put("line_sup_join", tasks);
        tasks = new ArrayList<>();
        tasks.add("cust_ord_line_sup_join");
        topology.put("cust_ord_line_sup_dispatch", tasks);
        topology.put("cust_ord_line_sup_join", new ArrayList<String>());

        if (topology.isEmpty() == false) {
            int k = topology.size();
        }

        ConcurrentHashMap<String, Integer> taskIdentifierIndex = new ConcurrentHashMap<>();
        taskIdentifierIndex.put("order", 14);
        taskIdentifierIndex.put("supplier", 15);
        taskIdentifierIndex.put("customer", 9);
        taskIdentifierIndex.put("lineitem", 13);

        taskIdentifierIndex.put("cust_ord_dispatch", 3);
        taskIdentifierIndex.put("cust_ord_join", 5);
        taskIdentifierIndex.put("cust_ord_join", 4);

        taskIdentifierIndex.put("line_sup_dispatch", 10);
        taskIdentifierIndex.put("line_sup_join", 11);
        taskIdentifierIndex.put("line_sup_join", 12);

        taskIdentifierIndex.put("cust_ord_line_sup_dispatch", 16);

        taskIdentifierIndex.put("cust_ord_line_sup_join", 7);
        taskIdentifierIndex.put("cust_ord_line_sup_join", 8);

        Util.topologyTaskExpand(taskIdentifierIndex, new ConcurrentHashMap<String, ArrayList<String>>(topology));
    }
}
