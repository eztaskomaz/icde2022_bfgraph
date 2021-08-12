import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;

public class CandidateRegion {

    private NecNode queryVertex;
    private Node dataVertex;
    private List<Node> nodesInCandidateRegion;
    private boolean matched;

    public CandidateRegion() {
        this.nodesInCandidateRegion = new ArrayList<>();
    }

    public CandidateRegion(NecNode queryVertex, Node dataVertex) {
        this();
        this.queryVertex = queryVertex;
        this.dataVertex = dataVertex;
    }

    public NecNode getQueryVertex() {
        return queryVertex;
    }

    public void setQueryVertex(NecNode queryVertex) {
        this.queryVertex = queryVertex;
    }

    public Node getDataVertex() {
        return dataVertex;
    }

    public void setDataVertex(Node dataVertex) {
        this.dataVertex = dataVertex;
    }

    public List<Node> getNodesInCandidateRegion() {
        return nodesInCandidateRegion;
    }

    public void setNodesInCandidateRegion(List<Node> nodesInCandidateRegion) {
        this.nodesInCandidateRegion = nodesInCandidateRegion;
    }

    public boolean isMatched() {
        return matched;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

}
