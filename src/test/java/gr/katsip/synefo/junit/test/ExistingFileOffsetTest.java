package gr.katsip.synefo.junit.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import org.junit.Test;

public class ExistingFileOffsetTest {

	@Test
	public void test() throws IOException {
		AsynchronousFileChannel statisticFileChannel = AsynchronousFileChannel.open(Paths.get("stat-file.txt"), 
				StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		CompletionHandler<Integer, Object> statisticFileHandler = new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer result, Object attachment) {}
			@Override
			public void failed(Throwable exc, Object attachment) {}
		};
		long statisticFileOffset = 0L;
		Random rand = new Random();
		int size = rand.nextInt(10) + 1;
		byte[] buffer = new byte[size];
		for(int i = 0; i < buffer.length; i++)
			buffer[i] = '1';
		statisticFileChannel.write(ByteBuffer.wrap(buffer), statisticFileOffset, "write", statisticFileHandler);
		statisticFileChannel.close();
		statisticFileChannel = AsynchronousFileChannel.open(Paths.get("stat-file.txt"), 
				StandardOpenOption.WRITE);
		statisticFileOffset = statisticFileChannel.size();
		int size2 = rand.nextInt(10) + 1;
		buffer = new byte[size2];
		for(int i = 0; i < buffer.length; i++)
			buffer[i] = '2';
		statisticFileChannel.write(ByteBuffer.wrap(buffer), statisticFileOffset, "write2", statisticFileHandler);
		statisticFileChannel.close();
		System.out.println("Expected to locate " + size + " \'1\' and " + size2 + "\'2\' characters.");
		statisticFileChannel = AsynchronousFileChannel.open(Paths.get("stat-file.txt"), 
				StandardOpenOption.READ);
		ByteBuffer bufferOne = ByteBuffer.allocate(size);
		statisticFileChannel.read(bufferOne, 0L, "read", statisticFileHandler);
		for(int i = 0; i < bufferOne.limit(); i++)
			System.out.print((char) bufferOne.get(i));
		ByteBuffer bufferTwo = ByteBuffer.allocate(size2);
		statisticFileChannel.read(bufferTwo, size + 1, "read2", statisticFileHandler);
		for(int i = 0; i < bufferTwo.limit(); i++)
			System.out.print((char) bufferTwo.get(i));
		System.out.println(".");
		for(int i = 0; i < bufferOne.limit(); i++) {
			System.out.print((char) bufferOne.get(i) + ",");
//			assertEquals('1', (char) bufferOne.get(i));
		}
		for(int i = 0; i < bufferTwo.limit() - 1; i++) {
			System.out.print((char) bufferTwo.get(i) + ",");
//			assertEquals('2', (char) bufferTwo.get(i));
		}
		statisticFileChannel.close();
		File f = new File("stat-file.txt");
		if(f.exists())
			f.delete();
	}

}
