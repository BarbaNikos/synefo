package gr.katsip.synefo.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by katsip on 1/8/2016.
 */
public class StatisticFileWriter implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(StatisticFileWriter.class);

    private File log;

    private FileOutputStream foStream;

    private BufferedWriter writer;

    public StatisticFileWriter(String directory, String taskName) {
        log = new File(directory + File.separator + taskName + ".log");
        if (log.exists())
            log.delete();
        try {
            log.createNewFile();
        } catch (IOException e) {
            logger.error("failed to create file: " + directory + File.separator + taskName + ".log");
            e.printStackTrace();
        }
        try {
//            writer = new BufferedWriter(new FileWriter(log));
            foStream = new FileOutputStream(log);
        } catch (IOException e) {
            logger.error("failed to initialize writer: " + directory + File.separator + taskName + ".log");
            e.printStackTrace();
        }
    }

    public void writeData(String dataPoint) {
        try {
//            writer.write(dataPoint + "\n");
            foStream.write(dataPoint.getBytes("UTF-8"));
            foStream.flush();
        } catch (IOException e) {
            logger.error("exception thrown while writing data");
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            foStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
