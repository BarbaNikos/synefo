package gr.katsip.synefo.storm.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by katsip on 9/17/2015.
 */
public class SourceFileProducer implements Runnable {

    Logger logger = LoggerFactory.getLogger(SourceFileProducer.class);

    private File file;

    private BufferedReader reader;

    private ArrayBlockingQueue<String> buffer;

    private String fileName;

    private String endMarker;

    public SourceFileProducer(ArrayBlockingQueue<String> buffer, String endMarker, String fileName) {
        this.buffer = buffer;
        this.endMarker = endMarker;
        file = new File(fileName);
        if(file.exists()) {
            try {
                reader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        this.fileName = fileName;
    }

    @Override
    public void run() {
        logger.info("thread-" + Thread.currentThread().getId() + " initiating with source file: " + fileName);
        String line = "";
        try {
            while((line = reader.readLine()) != null) {
                try {
                    buffer.put(line);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                buffer.put(endMarker);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("thread-" + Thread.currentThread().getId() + " scanned and buffered the whole file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
