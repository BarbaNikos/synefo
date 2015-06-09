import java.util.Random;

import gr.katsip.synefo.metric.ControlBasedStatistics;
import junit.framework.TestCase;


public class SlopeUpdateTest extends TestCase {

	ControlBasedStatistics controlStatistics = new ControlBasedStatistics();
	
	public void testUpdateSlope() {
		controlStatistics.updateSlope((double) 1.0);
		controlStatistics.updateSlope((double) 2.0);
		assert(controlStatistics.getSlope() > 0);
		for(int i = 0; i < 1000; i++) {
			Random rnd = new Random();
			controlStatistics.updateSlope(rnd.nextDouble());
		}
	}

	public void testGetSlope() {
		controlStatistics.getSlope();
	}

}
