package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import com.intellij.dvcs.branch.DvcsSyncSettings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by katsip on 9/14/2015.
 */
public class NewJoinDispatcherTest {

    private static final String OUTER_RELATION_NAME = "OUTER";

    private static final String INNER_RELATION_NAME = "INNER";

    private static final String[] OUTER = { "O_OUTER_KEY" , "O_INNER_KEY" };

    private static final String[] INNER = { "I_INNER_KEY" , "I_OUTER_KEY" };

    private static final String[] OUT = {"timestamp", "attributes", "values"};

    private static final Fields OUTER_SCHEMA = new Fields(OUTER);

    private static final Fields INNER_SCHEMA = new Fields(INNER);

    private static final Fields OUT_SCHEMA = new Fields(OUT);

    private static final String OUTER_PRIMARY_KEY = OUTER[0];

    private static final String OUTER_FOREIGN_KEY = OUTER[1];

    private static final String INNER_PRIMARY_KEY = INNER[0];

    private static final String INNER_FOREIGN_KEY = INNER[1];

    private HashMap<String, List<Integer>> taskToRelationIndex;

    @Test
    public void testExpandTask() throws Exception {
        randomTaskToRelationInitialization();
        NewJoinDispatcher dispatcher = new NewJoinDispatcher(OUTER_RELATION_NAME, OUTER_SCHEMA,
                OUTER_PRIMARY_KEY, OUTER_FOREIGN_KEY,
                INNER_RELATION_NAME, INNER_SCHEMA,
                INNER_PRIMARY_KEY, INNER_FOREIGN_KEY, OUT_SCHEMA);
        dispatcher.setTaskToRelationIndex(taskToRelationIndex);

        Values values = new Values();
        values.add("12345");
        values.add("2345");
        Values tuple = new Values();
        tuple.add(new Fields(OUTER_SCHEMA.toList()));
        tuple.add(values);

        dispatcher.execute(null, null, OUTER_SCHEMA, tuple);

        values = new Values();
        values.add("12345");
        values.add("3456");
        tuple = new Values();
        tuple.add(new Fields(OUTER_SCHEMA.toList()));
        tuple.add(values);

        dispatcher.execute(null, null, OUTER_SCHEMA, tuple);

        values = new Values();
        values.add("23456");
        values.add("3456");
        tuple = new Values();
        tuple.add(new Fields(OUTER_SCHEMA.toList()));
        tuple.add(values);

        dispatcher.execute(null, null, OUTER_SCHEMA, tuple);

        values = new Values();
        values.add("3456");
        values.add("23456");
        tuple = new Values();
        tuple.add(new Fields(INNER_SCHEMA.toList()));
        tuple.add(values);

        dispatcher.execute(null, null, INNER_SCHEMA, tuple);

        dispatcher.expandTask(OUTER_RELATION_NAME, 1, 23);
    }

    @Test
    public void testMergeTask() throws Exception {

    }

    @Test
    public void testSetTaskToRelationIndex() throws Exception {
        randomTaskToRelationInitialization();
        NewJoinDispatcher dispatcher = new NewJoinDispatcher(OUTER_RELATION_NAME, OUTER_SCHEMA,
                OUTER_PRIMARY_KEY, OUTER_FOREIGN_KEY,
                INNER_RELATION_NAME, INNER_SCHEMA,
                INNER_PRIMARY_KEY, INNER_FOREIGN_KEY, OUT_SCHEMA);
        dispatcher.setTaskToRelationIndex(taskToRelationIndex);

    }

    private void randomTaskToRelationInitialization() {
        taskToRelationIndex = new HashMap<>();
        List<Integer> outerList = new ArrayList<>();
        outerList.add(1);
        outerList.add(3);
        outerList.add(5);
        outerList.add(2);
        outerList.add(4);
        taskToRelationIndex.put(OUTER_RELATION_NAME, outerList);
        List<Integer> innerList = new ArrayList<>();
        innerList.add(7);
        innerList.add(6);
        innerList.add(13);
        innerList.add(11);
        taskToRelationIndex.put(INNER_RELATION_NAME, innerList);
    }

    @Test
    public void testExecute() throws Exception {
        Values values = new Values();
        values.add("12345");
        values.add("2345");
        Values tuple = new Values();
        tuple.add(new Fields(OUTER_SCHEMA.toList()));
        tuple.add(values);
        randomTaskToRelationInitialization();
        NewJoinDispatcher dispatcher = new NewJoinDispatcher(OUTER_RELATION_NAME, OUTER_SCHEMA,
                OUTER_PRIMARY_KEY, OUTER_FOREIGN_KEY,
                INNER_RELATION_NAME, INNER_SCHEMA,
                INNER_PRIMARY_KEY, INNER_FOREIGN_KEY, OUT_SCHEMA);
        dispatcher.setTaskToRelationIndex(taskToRelationIndex);

        dispatcher.execute(null, null, OUTER_SCHEMA, tuple);

        values = new Values();
        values.add("12345");
        values.add("3456");
        tuple = new Values();
        tuple.add(new Fields(OUTER_SCHEMA.toList()));
        tuple.add(values);

        dispatcher.execute(null, null, OUTER_SCHEMA, tuple);

        values = new Values();
        values.add("23456");
        values.add("3456");
        tuple = new Values();
        tuple.add(new Fields(OUTER_SCHEMA.toList()));
        tuple.add(values);

        dispatcher.execute(null, null, OUTER_SCHEMA, tuple);

        values = new Values();
        values.add("3456");
        values.add("23456");
        tuple = new Values();
        tuple.add(new Fields(INNER_SCHEMA.toList()));
        tuple.add(values);

        dispatcher.execute(null, null, INNER_SCHEMA, tuple);
    }
}