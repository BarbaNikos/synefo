package gr.katsip.synefo.storm.operators.relational.elastic;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by katsip on 10/8/2015.
 */
public class WindowDispatcher implements Serializable, Dispatcher {

    Logger logger = LoggerFactory.getLogger(WindowDispatcher.class);

    private String outerRelationName;

    private String innerRelationName;

    private Fields outerRelationSchema;

    private Fields innerRelationSchema;

    private String outerRelationKey;

    private String innerRelationKey;

    private String outerRelationForeignKey;

    private String innerRelationForeignKey;

    private Fields outputSchema;

    private long stateSize = 0L;

    private HashMap<String, List<Integer>> taskToRelationIndex;

    public class DispatchWindow implements Serializable {

        public long start;

        public long end;

        public long stateSize = 0L;

        private HashMap<String, List<Integer>> outerRelationIndex;

        private HashMap<String, List<Integer>> innerRelationIndex;

        public DispatchWindow() {
            stateSize = 0L;
            start = 0L;
            end = 0L;
            outerRelationIndex = new HashMap<>();
            innerRelationIndex = new HashMap<>();
        }
    }

    private long window;

    private long slide;

    private List<DispatchWindow> circularCache;

    private int cacheSize;

    public WindowDispatcher(String outerRelationName, Fields outerRelationSchema,
                            String outerRelationKey, String outerRelationForeignKey,
                            String innerRelationName, Fields innerRelationSchema,
                            String innerRelationKey, String innerRelationForeignKey, Fields outputSchema,
                            long window, long slide) {
        this.outerRelationName = outerRelationName;
        this.outerRelationSchema = new Fields(outerRelationSchema.toList());
        this.outerRelationKey = outerRelationKey;
        this.outerRelationForeignKey = outerRelationForeignKey;
        this.innerRelationName = innerRelationName;
        this.innerRelationSchema = new Fields(innerRelationSchema.toList());
        this.innerRelationKey = innerRelationKey;
        this.innerRelationForeignKey = innerRelationForeignKey;
        taskToRelationIndex = null;
        this.outputSchema = new Fields(outputSchema.toList());
        circularCache = new LinkedList<>();
        this.window = window;
        this.slide = slide;
        cacheSize = (int) Math.ceil(window / slide);
    }

    @Override
    public void setTaskToRelationIndex(HashMap<String, List<Integer>> taskToRelationIndex) {
        this.taskToRelationIndex = new HashMap<>(taskToRelationIndex);
    }

    @Override
    public void setOutputSchema(Fields outputSchema) {
        this.outputSchema = new Fields(outputSchema.toList());
    }

    @Override
    public int execute(Tuple anchor, OutputCollector collector, Fields fields, Values values) {
        return 0;
    }

    @Override
    public Fields getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void mergeState(List<Values> state) {

    }

    @Override
    public List<Values> getState() {
        return null;
    }

    @Override
    public long getStateSize() {
        return 0;
    }

    @Override
    public void updateIndex(String scaleAction, String taskWithIdentifier, String relation, List<String> result) {

    }
}
