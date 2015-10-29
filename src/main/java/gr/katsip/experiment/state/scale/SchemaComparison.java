package gr.katsip.experiment.state.scale;

import gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated.BasicCollocatedEquiWindow;
import gr.katsip.synefo.storm.operators.relational.elastic.joiner.collocated.CollocatedJoinBolt;
import gr.katsip.synefo.utils.SynefoConstant;

import java.util.LinkedList;

/**
 * Created by katsip on 10/20/2015.
 */
public class SchemaComparison {
    public static void main(String args[]) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(SynefoConstant.COL_SCALE_ACTION_PREFIX + ":" + SynefoConstant.COL_ADD_ACTION);
        stringBuilder.append("|" + SynefoConstant.COL_KEYS + ":");
        stringBuilder.append("|" + SynefoConstant.COL_PEER + ":1");
        String header = "COL_ACTION:C_ADD|C_KEYS:123,421|C_PEER:11";
        System.out.println(CollocatedJoinBolt.isScaleHeader(header));
        String action = header.split("[|]")[0];
        action = header.split("[|]")[0].split(":")[1];
        String serializedMigratedKeys = header.split("[|]")[1].split(":")[1];
        String candidateTask = header.split("[|]")[2].split(":")[1];
    }
}
