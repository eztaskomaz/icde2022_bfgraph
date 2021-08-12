import org.neo4j.graphdb.Node;

public class DataVertex  {

    private Node node;
    private Boolean visited;

    public DataVertex(Node node, Boolean visited) {
        this.node = node;
        this.visited = visited;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Boolean getVisited() {
        return visited;
    }

    public void setVisited(Boolean visited) {
        this.visited = visited;
    }
}
