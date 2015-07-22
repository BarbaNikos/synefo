package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;
import gr.katsip.cestorm.db.OperatorStatisticCollector;

import org.junit.Test;

public class OperatorStatCollectorTest {

	@Test
	public void test() {
		OperatorStatisticCollector opStat = new OperatorStatisticCollector("52.28.92.157:2181", 
				"52.28.9.123", "root", "myCQl_Is_#1", 1);
		opStat.init();
	}

}
