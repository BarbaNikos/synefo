package gr.katsip.file.filegen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by katsip on 11/2/2015.
 */
public class ScaleScenarioFileGenerator {

    public static void main(String[] args) throws IOException {
        File inner = new File("inner.tbl");
        File outer = new File("outer.tbl");
        if (inner.exists())
            inner.delete();
        if (outer.exists())
            outer.delete();
        inner.createNewFile();
        outer.createNewFile();
        PrintWriter innerWriter = new PrintWriter(new FileWriter(inner));
        PrintWriter outerWriter = new PrintWriter(new FileWriter(outer));
        /**
         * First Period: Key "1" is dominating
         */
        long oneCounter = (long) Math.ceil(0.95 * 600);
        long twoCounter = (long) Math.ceil(0.05 * 600);
        while (oneCounter > 0 && twoCounter > 0) {
            if (oneCounter > 0) {
                innerWriter.println("1");
                outerWriter.println("1");
                oneCounter--;
            }
            if (twoCounter > 0) {
                innerWriter.println("2");
                outerWriter.println("2");
                twoCounter--;
            }
        }
        /**
         * Second Period: Equal number of keys
         */
        oneCounter = (long) Math.ceil(0.5 * 600);
        twoCounter = (long) Math.ceil(0.5 * 600);
        while (oneCounter > 0 && twoCounter > 0) {
            if (oneCounter > 0) {
                innerWriter.println("1");
                outerWriter.println("1");
                oneCounter--;
            }
            if (twoCounter > 0) {
                innerWriter.println("2");
                outerWriter.println("2");
                twoCounter--;
            }
        }
        /**
         * Third Period: Key "2" is dominating
         */
        oneCounter = (long) Math.ceil(0.05 * 600);
        twoCounter = (long) Math.ceil(0.95 * 600);
        while (oneCounter > 0 && twoCounter > 0) {
            if (oneCounter > 0) {
                innerWriter.println("1");
                outerWriter.println("1");
                oneCounter--;
            }
            if (twoCounter > 0) {
                innerWriter.println("2");
                outerWriter.println("2");
                twoCounter--;
            }
        }
        /**
         * Fourth Period: Equal number of keys
         */
        /**
         * Second Period: Equal number of keys
         */
        oneCounter = (long) Math.ceil(0.5 * 600);
        twoCounter = (long) Math.ceil(0.5 * 600);
        while (oneCounter > 0 && twoCounter > 0) {
            if (oneCounter > 0) {
                innerWriter.println("1");
                outerWriter.println("1");
                oneCounter--;
            }
            if (twoCounter > 0) {
                innerWriter.println("2");
                outerWriter.println("2");
                twoCounter--;
            }
        }
        innerWriter.flush();
        outerWriter.flush();
        innerWriter.close();
        outerWriter.close();
    }
}
