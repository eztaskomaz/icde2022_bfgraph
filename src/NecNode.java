import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.List;

public class NecNode {

    private Integer id;
    private Label label;
    private MyRelation relationToParent;
    private MyRelation relationFromParent;
    private NecNode parentNecNode;
    private List<NecNode> childNecNodes;
    private List<MyNode> necMembers;
    private Double order;

    public NecNode(Integer id, Label label) {
        this.id = id;
        this.label = label;
        this.necMembers = new ArrayList<MyNode>();
        this.childNecNodes = new ArrayList<NecNode>();
    }

    public Integer getId() {
        return this.id;
    }

    public void addChild(NecNode childNode) {
        childNecNodes.add(childNode);
    }

    public void addMember(MyNode queryNode) {
        necMembers.add(queryNode);
    }

    public List<NecNode> getChildren() {
        return childNecNodes;
    }

    public List<MyNode> getNECMembers() {
        return necMembers;
    }

    public void setRelations(MyRelation outgoingRelation, MyRelation incomingRelation) {
        this.relationToParent = outgoingRelation;
        this.relationFromParent = incomingRelation;
    }

    public void setParentNecNode(NecNode parentNecNode) {
        this.parentNecNode = parentNecNode;
    }

    public Double getOrder() {
        return order;
    }

    public void setOrder(Double order) {
        this.order = order;
    }
}
