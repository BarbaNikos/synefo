package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

public class ConcurrencyTest {
	
	volatile Long foo1;
	
	AtomicLong atomicFoo;

	@Test
	public void test() {
		long foo = 0;
		Lock lock = new ReentrantLock();
		long start = System.currentTimeMillis();
		for(long l = 0; l < 500000000L; l++) {
			foo++;
		}
		long end = System.currentTimeMillis();
		System.out.println("Raw performance: " + (end - start) + " msec");
		
		foo1 = new Long(0L);
		start = System.currentTimeMillis();
		for(long l = 0; l < 500000000L; l++) {
			foo1++;
		}
		end = System.currentTimeMillis();
		System.out.println("Volatile performance: " + (end - start) + " msec");
		
		atomicFoo = new AtomicLong(0);
		start = System.currentTimeMillis();
		for(long l = 0; l < 500000000L; l++) {
			atomicFoo.getAndIncrement();
		}
		end = System.currentTimeMillis();
		System.out.println("AtomicLong performance: " + (end - start) + " msec");
		
		foo = 0;
		start = System.currentTimeMillis();
		for(long l = 0; l < 500000000L; l++) {
			lock.lock();
			try {
				foo++;
			}finally {
				lock.unlock();
			}
		}
		end = System.currentTimeMillis();
		System.out.println("Lock performance: " + (end - start) + " msec");
	}

}
