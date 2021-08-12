import org.neo4j.graphdb.Node;

public class Candidate {

    private MyNode queryNode;
    private Node graphNode;

    public Candidate() {

    }

    public Candidate(MyNode queryNode, Node graphNode) {
        this.queryNode = queryNode;
        this.graphNode = graphNode;
    }

    public MyNode getQueryNode() {
        return queryNode;
    }

    public void setQueryNode(MyNode queryNode) {
        this.queryNode = queryNode;
    }

    public Node getGraphNode() {
        return graphNode;
    }

    public void setGraphNode(Node graphNode) {
        this.graphNode = graphNode;
    }
}
