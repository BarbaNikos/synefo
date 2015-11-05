package gr.katsip.file.filegen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by katsip on 11/5/2015.
 */
public class ScaleScenarioBalancedFileGenerator {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("arguments: output-directory scale-period reluctancy");
            System.exit(1);
        }
        int scalePeriod = Integer.parseInt(args[1]);
        int reluctancy = Integer.parseInt(args[2]);
        File inner = new File(args[0] + File.separator + "inner.tbl");
        File outer = new File(args[0] + File.separator + "outer.tbl");
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
        long oneCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
        long twoCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
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
        oneCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
        twoCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
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
        oneCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
        twoCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
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
        oneCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
        twoCounter = (long) Math.ceil(0.5 * (reluctancy * scalePeriod / 2));
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
