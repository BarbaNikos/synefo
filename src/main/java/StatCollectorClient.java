import gr.katsip.synefo.storm.operators.synefo_comp_ops.dataCollector;


public class StatCollectorClient {

	public static void main(String[] args) {
		dataCollector collector = new dataCollector("localhost", 2181, 1000, "select_bolt_1");
		dataCollector collector2 = new dataCollector("localhost", 2181, 1000, "select_bolt_2");
		dataCollector collector3 = new dataCollector("localhost", 2181, 1000, "select_bolt_3");
		dataCollector collector4 = new dataCollector("localhost", 2181, 1000, "select_bolt_4");
		
		for(int i = 0; i < 100000; ++i) {
//			try {
//				Thread.sleep(5);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			if(i % 4 == 0)
				collector.addToBuffer("select_bolt_1,1," + i + "," + i + "," + i);
			else if(i % 4 == 1)
				collector2.addToBuffer("select_bolt_2,1," + i + "," + i + "," + i);
			else if(i % 4 == 2)
				collector3.addToBuffer("select_bolt_3,1," + i + "," + i + "," + i);
			else
				collector4.addToBuffer("select_bolt_4,1," + i + "," + i + "," + i);
		}
	}

}
