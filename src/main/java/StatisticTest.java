import java.util.Random;
import gr.katsip.synefo.metric.TaskStatistics;


public class StatisticTest {

	public static void main(String[] args) {
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
