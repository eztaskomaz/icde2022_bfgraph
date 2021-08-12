import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.Iterator;

public class StartNodeSelector {

    private MyNode startNode;

    public MyNode getStartNode() {
        return startNode;
    }

    public MyNode chooseStartNode(Query queryGraph, GraphDatabaseService database) {
        double rankValue = 0;
        for(MyNode node : queryGraph.getQueryNodes()) {
            int frequency = calculateFrequency(database, node.getLabels().iterator().next());
            double rankValueOfNode = calculateRankValue(frequency, node.getDegree());
            if(rankValue <= rankValueOfNode) {
                rankValue = rankValueOfNode;
                startNode = node;
            }
        }

        return startNode;
    }

    private int calculateFrequency(GraphDatabaseService database, Label label) {
        int numberOfDataVerticesWithLabel = 0;
        Iterator<Node> allDataVerticesWithLabel = database.findNodes(label);
        while(allDataVerticesWithLabel.hasNext()) {
            allDataVerticesWithLabel.next();
            numberOfDataVerticesWithLabel++;
        }
        return numberOfDataVerticesWithLabel;
    }

    private double calculateRankValue(int frequency, int degree) {
        return frequency / degree;
    }

}
