package gr.katsip.synefo.test;
import java.util.Random;

import backtype.storm.tuple.Values;
import gr.katsip.synefo.metric.TaskStatistics;
import gr.katsip.synefo.utils.SynefoConstant;


public class StatisticTest {

	public static void main(String[] args) {
		String synefoHeader = SynefoConstant.QUERY_LATENCY_METRIC + ":s_1:" + System.currentTimeMillis();
		Long synefoTimestamp = null;
		if(synefoHeader != null && synefoHeader.equals("") == false) {
			if(synefoHeader.contains("/") && synefoHeader.contains(SynefoConstant.PUNCT_TUPLE_TAG) == true 
					&& synefoHeader.contains(SynefoConstant.ACTION_PREFIX) == true
					&& synefoHeader.contains(SynefoConstant.COMP_IP_TAG) == true) {
				String[] headerFields = synefoHeader.split("/");
				if(headerFields[0].equals(SynefoConstant.PUNCT_TUPLE_TAG)) {
					System.out.println("Punctuation tuple");
				}
			}else if(synefoHeader.contains(SynefoConstant.QUERY_LATENCY_METRIC) == true) {
				System.out.println("Query-Latency-Metric");
			}else if(synefoHeader.contains(SynefoConstant.OP_LATENCY_METRIC)) {
				System.out.println("Operator-Latency-Metric");
			}
			TaskStatistics stats = new TaskStatistics();
			int j = 0;
			for(int i = 0; i < 10000; i++) {
				stats.updateWindowThroughput();
				if(j == 100) {
					j = 0;
					Double throughput = stats.getWindowThroughput();
					System.out.println("Throughput: " + throughput);
				}else {
					j++;
				}
				Random rand = new Random();
				try {
					Thread.sleep(rand.nextInt(4));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Throughput: " + stats.getWindowThroughput());
		}
	}

}
