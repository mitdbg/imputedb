package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class QueryPlanDotter {
    // current node identifier
    private int nodeId;
    // intermediate state of graph
    private StringBuilder sb;

    public QueryPlanDotter(){
        nodeId = 0;
        sb = new StringBuilder();
    }

    /**
     * Add a node and increase the node identifier
     * @param msg
     * @return
     */
    private int addNode(String msg) {
        nodeId++;
        sb.append(nodeId);
        sb.append(" [label=\"");
        sb.append(msg);
        sb.append("\"];\n");
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
        DbIterator[] children;
        if (plan == null) {
            return;
        }

        if (plan instanceof SeqScan) {
            SeqScan seq = (SeqScan) plan;
            label = "scan(" + (seq.getAlias() == null ? seq.getTableName() : seq.getAlias()) + ")";
            children = null;
        } else {
            Operator operator = (Operator) plan;
            children = ((Operator) plan).getChildren();

            if (operator instanceof Join) {
                Join j = (Join) operator;
                JoinPredicate jpred = j.getJoinPredicate();
                String left = j.getJoinField1Name();
                String right = j.getJoinField2Name();
                label = "Join(" + left + jpred.getOperator() + right + ")";
            } else if (operator instanceof Aggregate) {
                Aggregate a = (Aggregate) operator;
                int gfield = a.groupField();
                label = "Agg(" + a.aggregateFieldName() + ")";
                if (gfield != Aggregator.NO_GROUPING) {
                    label += "+ group(" + a.groupFieldName() +")";
                }
            } else if (operator instanceof Project) {
                Project p = (Project) operator;
                List<String> fieldNames = new ArrayList<>();
                TupleDesc td = p.getTupleDesc();
                for(int i = 0; i < td.numFields(); i++) {
                    fieldNames.add(td.getFieldName(i));
                }
                label = "Project(" + Arrays.toString(fieldNames.toArray()) + ")";
            } else if (operator instanceof Filter) {
                Filter f = (Filter) operator;
                String fieldName = f.getTupleDesc().getFieldName(f.getPredicate().getField());
                label = "Filter(" + fieldName + ")";
            } else if (operator instanceof Impute) {
                Impute imp = (Impute) operator;
                Collection<String> imputed = imp.dropFields;
                String oplabel;
                if (imp instanceof Drop) {
                    oplabel = "drop";
                } else if (imp instanceof ImputeHotDeck) {
                    oplabel = "impute_hotdeck";
                } else if (imp instanceof ImputeTotallyRandom) {
                    oplabel = "impute_total_random";
                } else if (imp instanceof ImputeRegressionTree) {
                    oplabel = "impute_tree";
                } else {
                    oplabel = "impute_unk";
                }

                label = oplabel + "(" + Arrays.toString(imputed.toArray()) + ")";
            } else {
                label = "Unk Op";
            }
        }

        int id = addNode(label);
        addEdge(id, parentId);
        if (children != null) {
            for (DbIterator child : children) {
                buildGraph(child, id);
            }
        }
    }

    public void initGraph() {
        sb.append("digraph g {\n0 [label=\"query\"];\n");
    }

    public void closeGraph() {
        sb.append("}\n");
    }

    public static String draw(DbIterator plan) {
        QueryPlanDotter p = new QueryPlanDotter();
        p.initGraph();
        p.buildGraph(plan, 0);
        p.closeGraph();
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
