import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.HashMap;
import java.util.Map;

public class MyRelation {

	private RelationshipType type;
	private int id;
	private int startNodeId;
	private int endNodeId;
	private Map<String, String> propertyMap = null;
	private boolean isPartOfMst;
	private Integer countOnGraph;
	private int weight;

	public MyRelation() {

	}

	public MyRelation(RelationshipType type, int id, int startNodeId, int endNodeId) {
		this.type = type;
		this.id = id;
		this.startNodeId = startNodeId;
		this.endNodeId = endNodeId;
		this.propertyMap = new HashMap<String, String>();
	}

	public RelationshipType getType() {
		return type;
	}

	public void createMyRelation(RelationshipType relationshipType, int startNodeId, int endNodeId, int id) {
		this.type = relationshipType;
		this.id = id;
		this.startNodeId = startNodeId;
		this.endNodeId = endNodeId;
	}

	public int getId() {
		return id;
	}
		
	public Direction getDirection(int nodeId) {
		if (nodeId == startNodeId)
			return Direction.OUTGOING;
		else
			return Direction.INCOMING;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public Integer getCountOnGraph() {
		return countOnGraph;
	}

	public void setCountOnGraph(Integer countOnGraph) {
		this.countOnGraph = countOnGraph;
	}

	public boolean isPartOfMst() {
		return isPartOfMst;
	}

	public void setPartOfMst(boolean partOfMst) {
		isPartOfMst = partOfMst;
	}

	public int getStartNodeId() {
		return startNodeId;
	}
		
	public int getEndNodeId() {
		return endNodeId;
	}
		
	public int getTheOtherNodeId(int node_id) {
		if (node_id == startNodeId)
			return endNodeId;
		else
			return startNodeId;
	}

	public boolean setProperty(String key, String value) {
		propertyMap.put(key, value);
		return true;
	}

	public void setType(RelationshipType type) {
		this.type = type;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setStartNodeId(int startNodeId) {
		this.startNodeId = startNodeId;
	}

	public void setEndNodeId(int endNodeId) {
		this.endNodeId = endNodeId;
	}

	public void setPropertyMap(Map<String, String> propertyMap) {
		this.propertyMap = propertyMap;
	}

	public String getProperty(String key) {
		return propertyMap.get(key);
	}
		
	public Map<String, String> getPropertyMap() {
		return propertyMap;
	}
		
	public boolean hasAnyProperty() {
		if (propertyMap.isEmpty())
			return false;
		return true;
	}

	public boolean hasProperty(String key) {
		if (propertyMap.containsKey(key))
			return true;
		return false;
	}

}