import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;

public class Combination {

    private List<Node> dataVertexList;

    public Combination() {
        this.dataVertexList = new ArrayList<>();
    }

    public Combination(List<Node> dataVertexList) {
        this.dataVertexList = dataVertexList;
    }

    public List<Node> getDataVertexList() {
        return dataVertexList;
    }

    public void setDataVertexList(List<Node> dataVertexList) {
        this.dataVertexList = dataVertexList;
    }
}
