package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class QueryPlanDotter {
    // current node identifier
    private int nodeId;
    // intermediate state of graph
    private StringBuffer sb;

    public QueryPlanDotter(){
        nodeId = 0;
        sb = new StringBuffer();
    }

    /**
     * Add a node and increase the node identifier
     * @param msg
     * @return
     */
    private int addNode(String msg) {
        nodeId++;
        sb.append(nodeId);
        sb.append(" [label='");
        sb.append(msg);
        sb.append("'];\n");
        return nodeId;
    }

    /**
     * Add an edge from one node to another
     * @param from source node
     * @param to destination node
     */
    private void addEdge(int from, int to) {
        sb.append(from);
        sb.append(" -> ");
        sb.append(to);
        sb.append(";\n");
    }

    /**
     * Build a minimal dot graph. Really only care about joins and displaying impute info,
     * everything else abstracted to just operator label.
     * @param plan
     * @param parentId
     */
    private void buildGraph(DbIterator plan, int parentId) {
        String label;
        if (plan == null) {
            return;
        }

        if (plan instanceof SeqScan) {
            SeqScan seq = (SeqScan) plan;
            label = "scan(" + (seq.getAlias() == null ? seq.getTableName() : seq.getAlias()) + ")";
        } else {
            Operator operator = (Operator) plan;
            DbIterator[] children = ((Operator) plan).getChildren();

            if (operator instanceof Join) {
                Join j = (Join) operator;
                JoinPredicate jpred = j.getJoinPredicate();
                label = "Join(" + jpred.getField1() + jpred.getOperator() + jpred.getField2() + ")";
            } else if (operator instanceof Aggregate) {
                Aggregate a = (Aggregate) operator;
                int gfield = a.groupField();
                label = "Agg";
                if (gfield != Aggregator.NO_GROUPING) {
                    label = "Agg + Group-by";
                }
            } else if (operator instanceof Project) {
                label = "Project";
            } else if (operator instanceof Impute) {
                Impute imp = (Impute) operator;
                List<QuantifiedName> imputed = imp.getImputedAttr();
                label = "impute(" + Arrays.toString(imputed.toArray()) + ")";
            } else if (operator instanceof Drop) {
                Drop d = (Drop) operator;
                List<QuantifiedName> dropped = d.getImputedAttr();
                label = "drop(" + Arrays.toString(dropped.toArray()) + ")";
            } else {
                label = "Unknown Op";
            }

            int id = addNode(label);
            addEdge(parentId, id);
            for (DbIterator child : children) {
                buildGraph(child, id);
            }
        }
    }

    public void initGraph() {
        sb.append(" 0 [label = 'query'];\n");
    }

    public static String draw(DbIterator plan) {
        QueryPlanDotter p = new QueryPlanDotter();
        p.initGraph();
        p.buildGraph(plan, 0);
        return p.sb.toString();
    }

    public static void print(DbIterator plan, String fileName) throws IOException {
        File f = new File(fileName);
        PrintWriter writer = new PrintWriter(f);
        String graph = QueryPlanDotter.draw(plan);
        writer.write(graph);
        writer.close();
    }
}
