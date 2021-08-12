import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.util.*;
import java.util.stream.Collectors;

public class MyNode {
	
	private int id;
	private List<Label> labelList = null;
	private List<MyRelation> outgoingRelationList = null;
	private List<MyRelation> incomingRelationList = null;
	private List<Integer> neighborNodeIdList = null;
	private Map<String, String> propertyMap = null;

	private double degreeM;
	private double pFeasible;
	private MyNode parent;
	private Set<MyNode> children;
	private NodeClass nodeClass;
	private Boolean visited;
	private boolean isPartOfMst;

	private Integer candidateNodeSize;
	private Integer order;
	private Double centralityValue;

	public MyNode() {
		labelList = new ArrayList<Label>();
		outgoingRelationList = new ArrayList<MyRelation>();
		incomingRelationList = new ArrayList<MyRelation>();
		neighborNodeIdList = new ArrayList<Integer>();
		propertyMap = new HashMap<String, String>();
		visited = false;
	}

	public boolean isPartOfMst() {
		return isPartOfMst;
	}

	public void setPartOfMst(boolean partOfMst) {
		isPartOfMst = partOfMst;
	}

	public Double getCentralityValue() {
		return centralityValue;
	}

	public void setCentralityValue(Double centralityValue) {
		this.centralityValue = centralityValue;
	}

	public Integer getOrder() {
		return order;
	}

	public void setOrder(Integer order) {
		this.order = order;
	}

	public Integer getCandidateNodeSize() {
		return candidateNodeSize;
	}

	public void setCandidateNodeSize(Integer candidateNodeSize) {
		this.candidateNodeSize = candidateNodeSize;
	}

	public NodeClass getNodeClass() {
		return nodeClass;
	}

	public void setNodeClass(NodeClass nodeClass) {
		this.nodeClass = nodeClass;
	}

	public MyNode getParent() {
		return parent;
	}

	public void setParent(MyNode parent) {
		this.parent = parent;
	}

	public Set<MyNode> getChildren() {
		return children;
	}

	public void setChildren(Set<MyNode> children) {
		this.children = children;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public Integer getId() {
		return this.id;
	}

	public void addLabel(Label l) {
		labelList.add(l);
	}
	
	public List<Label> getLabels() {
		return labelList;
	}

	public Label getLabel() {
		return labelList.iterator().next();
	}
	
	public boolean hasLabel(Label l) {
		return labelList.contains(l);
	}

	public void setProperty(String key, String value) {
		propertyMap.put(key, value);
	}
	
	public String getProperty(String key) {
		return propertyMap.get(key);
	}
	
	public Map<String, String> getPropertyMap() {
		return propertyMap;
	}

	public double getpFeasible() {
		return pFeasible;
	}

	public void setpFeasible(double pFeasible) {
		this.pFeasible = pFeasible;
	}

	public double getDegreeM() {
		return degreeM;
	}

	public void setDegreeM(double degreeM) {
		this.degreeM = degreeM;
	}

	public Boolean getVisited() {
		return visited;
	}

	public void setVisited(Boolean visited) {
		this.visited = visited;
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
	
	public boolean addNeighbor(int nodeId) {
		neighborNodeIdList.add(nodeId);
		return true;
	}
	
	public boolean isNeighborTo(int nodeId) {
		if (neighborNodeIdList.contains(nodeId))
			return true;
		else
			return false;
	}
	
	public List<Integer> getNeighborsIdList() {
		return neighborNodeIdList;
	}
	
	public boolean addRelation(MyRelation r, Direction d ) {
		if (d.equals(Direction.OUTGOING))
			outgoingRelationList.add(r);
		else
			incomingRelationList.add(r);
		return true;	
	}
	
	public boolean hasRelation( RelationshipType type, Direction d) {
		List<MyRelation> directionalRelationListOfThisNode;
		if (d.equals(Direction.OUTGOING))
			directionalRelationListOfThisNode = outgoingRelationList;
		else
			directionalRelationListOfThisNode = incomingRelationList;
		
		for (int i=0; i<directionalRelationListOfThisNode.size(); i++)
			if ( directionalRelationListOfThisNode.get(i).getType().toString().equals(type.toString()) )
				return true;
		return false;
	}

	public List<MyRelation> getIncomingRelationsWithNeighbor(MyNode neighborNode) {
		List<MyRelation> incomingRelations = getRelations(Direction.INCOMING);
		return incomingRelations.stream().filter(relation -> relation.getStartNodeId() == neighborNode.getId()).collect(Collectors.toList());
	}

	public List<MyRelation> getOutgoingRelationsWithNeighbor(MyNode neighborNode) {
		List<MyRelation> outgoingRelations = getRelations(Direction.OUTGOING);
		return outgoingRelations.stream().filter(relation -> relation.getEndNodeId() == neighborNode.getId()).collect(Collectors.toList());
	}

	public List<MyRelation> getAllRelations() {
		List<MyRelation> allRelations = new ArrayList<>();
		List<MyRelation> outgoingRelations = this.getRelations(Direction.OUTGOING);
		List<MyRelation> ingoingRelations = this.getRelations(Direction.INCOMING);
		allRelations.addAll(outgoingRelations);
		allRelations.addAll(ingoingRelations);
		return allRelations;
	}

	public List<MyRelation> getRelations(Direction d) {
		if (d.equals(Direction.OUTGOING))
			return outgoingRelationList;
		else
			return incomingRelationList;
	}
	
	public List<MyRelation> getRelations(RelationshipType type, Direction d) {
		List<MyRelation> directionalRelationListOfThisNode;
		if (d.equals(Direction.OUTGOING))
			directionalRelationListOfThisNode = outgoingRelationList;
		else
			directionalRelationListOfThisNode = incomingRelationList;
		
		List<MyRelation> resultRelationList = new ArrayList<MyRelation>();
		MyRelation tempRelation;
		for (int i=0; i<directionalRelationListOfThisNode.size(); i++) {
			tempRelation = directionalRelationListOfThisNode.get(i);
			if ( tempRelation.getType().toString().equals(type.toString()) )
				resultRelationList.add( tempRelation);
		}
		return resultRelationList;
	}

	public Set<MyNode> getPredecessorNodeList(Query gueryGraph) {
		Set<MyNode> predecessorNodeList = new HashSet<>();
		List<MyNode> neighborList = this.getNeighbors(gueryGraph);

		List<MyRelation> incomingRelations = this.getRelations(Direction.INCOMING);

		incomingRelations.forEach(relation -> {
			MyNode predecessorNode = neighborList.stream().filter(neighbor -> neighbor.getId() == relation.getStartNodeId()).findFirst().orElse(null);
			predecessorNodeList.add(predecessorNode);
		});

		return predecessorNodeList;
	}

	public Set<MyNode> getSuccessorNodeList(Query queryGraph) {
		Set<MyNode> successorNodeList = new HashSet<>();
		List<MyNode> neighborList = this.getNeighbors(queryGraph);

		List<MyRelation> outgoingRelations = this.getRelations(Direction.OUTGOING);

		outgoingRelations.forEach(relation -> {
			MyNode successorNode = neighborList.stream().filter(neighbor -> neighbor.getId() == relation.getEndNodeId()).findFirst().orElse(null);
			successorNodeList.add(successorNode);
		});

		return successorNodeList;
	}
	
	public List<Object> getRelationTypesWithCount(Direction d) {
		List<MyRelation> directionalRelationListOfThisNode;
		if (d.equals(Direction.OUTGOING))
			directionalRelationListOfThisNode = outgoingRelationList;
		else
			directionalRelationListOfThisNode = incomingRelationList;
		
		List<Object> result_Type_Count_List = new ArrayList<Object>();
		List<RelationshipType> typeList = new ArrayList<RelationshipType>();
		MyRelation tempRelation;
		
		for (int i=0; i<directionalRelationListOfThisNode.size(); i++) {
			tempRelation = directionalRelationListOfThisNode.get(i);
			int ind = typeList.indexOf(tempRelation.getType());
			if ( ind >= 0 ) {
				ind = 2*ind+1;
				int count = (Integer)result_Type_Count_List.get(ind);
				result_Type_Count_List.set(ind, count+1);
			}
			else {
				typeList.add(tempRelation.getType());
				result_Type_Count_List.add(tempRelation.getType());
				result_Type_Count_List.add(1);
			}
		}
		return result_Type_Count_List;
	}
	
	public int getDegree() {
		return outgoingRelationList.size() + incomingRelationList.size();
	}

	public Integer getDegreeForOrder() {
		return outgoingRelationList.size() + incomingRelationList.size();
	}
	
	public int getDegree(Direction d) {
		if (d.equals(Direction.OUTGOING))
			return outgoingRelationList.size();
		else
			return incomingRelationList.size();
	}

	public List<MyNode> getNeighbors(Query queryGraph) {
		List<Integer> neighborsIdList = this.getNeighborsIdList();
		List<MyNode> neighborsList = new ArrayList<>();
		neighborsIdList.forEach(neighborsId -> neighborsList.add(queryGraph.getNodeById(neighborsId)));
		return neighborsList;
	}

	public List<Integer> getNeighbors() {
		return neighborNodeIdList;
	}

}
