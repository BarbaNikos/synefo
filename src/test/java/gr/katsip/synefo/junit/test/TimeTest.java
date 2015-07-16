package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;

import org.junit.Test;

public class TimeTest {

	@Test
	public void test() throws InterruptedException {
		long t1 = System.currentTimeMillis();
		Thread.sleep(1);
		long t2 = System.currentTimeMillis();
		System.out.println(Math.abs(t2 - t1));
		if(Math.abs(t2 - t1) >= 1000)
			System.out.println("Yes");
		else
			System.out.println("No");
	}

}
