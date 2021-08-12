import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.neo4j.graphdb.Label;

public class NodeClass {

    private int id;
    private int degree;
    private Label label;

    public NodeClass() {
    }

    public NodeClass(int id, int degree, Label label) {
        this.id = id;
        this.degree = degree;
        this.label = label;
    }

    public NodeClass(int degree, Label label) {
        this.degree = degree;
        this.label = label;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public Label getLabel() {
        return label;
    }

    public void setLabel(Label label) {
        this.label = label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof NodeClass)) return false;

        NodeClass nodeClass = (NodeClass) o;

        return new EqualsBuilder()
                .append(degree, nodeClass.degree)
                .append(label, nodeClass.label)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(degree)
                .append(label)
                .toHashCode();
    }
}
