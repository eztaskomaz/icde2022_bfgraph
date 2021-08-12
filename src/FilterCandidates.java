import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.*;

public class FilterCandidates {

	private final GraphDatabaseService database;
	private Query queryGraph;
	private List<List<Node>> candidateNodes;
	private List<MyNode> queryNodes;
	private LinkedList<Integer> candidateSizeOrder;
	String response = "";
	
	/**
	 * Constructor of the class
	 */
	
	public FilterCandidates(GraphDatabaseService database, Query queryGraph, List<List<Node>> candidateNodes, List<MyNode> queryNodes, LinkedList<Integer> candidateSizeOrder) {
		this.database = database;
		this.queryGraph = queryGraph;
		this.candidateNodes = candidateNodes;
		this.queryNodes = queryNodes;
		this.candidateSizeOrder = candidateSizeOrder;
	}
	
	/**
	 * This function searches candidate nodes for each query node by GraphQL algorithm specifications
	 */

	public String filterCandidates() {
		long startTime = 0, endTime = 0, timeDifference;

		// Iterate through each node of query graph
		for(Iterator<MyNode> i = queryNodes.iterator(); i.hasNext(); ) {

			MyNode queryNode = i.next();
		    Iterator<Node> candidateNodesIterator = null;
		    List<Node> candidateNodesList = new ArrayList<Node>();
		    response += "QUERY NODE-" + queryNode.getId() + "\n";
		    try ( Transaction tx = database.beginTx() )
		    {
		    	// Step-1: 
		    	startTime = System.nanoTime(); 
		    	candidateNodesIterator = filterCandidatesByLabelAndProperty(queryNode, candidateNodesIterator);
		    	endTime = System.nanoTime();
		    	timeDifference = (endTime - startTime) / 1000000000;
		    	response += "- Step-1: " + timeDifference + " seconds" + "\t" + "\n";
		    	
		    	// Step-2: 
		    	startTime = System.nanoTime();
		    	candidateNodesIterator = filterCandidatesByRelationships(queryNode, candidateNodesIterator, Direction.OUTGOING);
		    	endTime = System.nanoTime();
		    	timeDifference = (endTime - startTime) / 1000000000;
		    	response += "- Step-2: " + timeDifference + " seconds" + "\t" + "\n";
		    	
		    	// Step-3: 
		    	startTime = System.nanoTime();
		    	candidateNodesIterator = filterCandidatesByRelationships(queryNode, candidateNodesIterator, Direction.INCOMING);
		    	endTime = System.nanoTime();
		    	timeDifference = (endTime - startTime) / 1000000000;
		    	response += "- Step-3: " + timeDifference + " seconds" + "\t" + "\n";

		    	// Step-4: 
//		    	startTime = System.nanoTime();
//		    	candidateNodesIterator = filterCandidatesByBFS(queryNode, candidateNodesIterator, 2);
//		    	endTime = System.nanoTime();
//		    	timeDifference = (endTime - startTime) / 1000000000;
		    	for ( ; candidateNodesIterator.hasNext(); )
		    		candidateNodesList.add(candidateNodesIterator.next());
		    	candidateNodes.add(candidateNodesList);
		    	response += "--------------------------------" + "\t" + candidateNodesList.size() + " nodes" + "\n";
//		    	response += "- Step-4: " + timeDifference + " seconds" + "\t" + candidateNodesList.size() + " nodes" + "\n";
		    	
		    	insertByOrder(queryNode.getId(), candidateNodesList.size());
		        tx.success();
		        tx.close();
			}
		    catch (Exception e) {
		    	response += e.getMessage();
		    }
		}
		return response;
	}
	
	/**
	 * This function finds the candidate nodes with respect to node label by iterating through each 
	 * label of the node that will be matched
	 */
	
	public Iterator<Node> filterCandidatesByLabelAndProperty(MyNode queryNode, Iterator<Node> candidateNodesIterator) {
		
		List<Label> labelList = queryNode.getLabels();
		Label firstLabel = labelList.get(0);
		Set<String> propertyKeySet = queryNode.getPropertyMap().keySet(); // This may be an empty set
		List<Node> candidateNodesList = new ArrayList<Node>();

		if (queryNode.hasAnyProperty()) {
			String firstPropertyKey = propertyKeySet.iterator().next();
			String firstPropertyValue = queryNode.getProperty(firstPropertyKey);
			//candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, firstPropertyValue);
			if (StringUtils.isNumeric(firstPropertyValue)) {
				candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, Double.parseDouble(firstPropertyValue));
			}
			else {
				candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, firstPropertyValue);
			}

		}
		else
			candidateNodesIterator = database.findNodes(firstLabel);

		// Check for other labels and properties
		for ( ; candidateNodesIterator.hasNext(); ) {
			Node node = candidateNodesIterator.next();
			boolean satisfied = true;
			if (labelList.size() > 1)
				satisfied = checkRestOfNodeLabels(node, labelList.iterator());
			if (satisfied && propertyKeySet.size() > 1)
					satisfied = checkRestOfNodeProperties(node, queryNode, propertyKeySet.iterator(), true);
			if (satisfied)
				candidateNodesList.add(node);
		}
		return candidateNodesList.iterator();
	}
	
	/**
	 * This function checks whether the node given in function parameter also has all the other labels
	 */
	
	public boolean checkRestOfNodeLabels(Node node, Iterator<Label> labelIterator) {
		labelIterator.next();
		for ( ; labelIterator.hasNext(); ) {
			Label label = labelIterator.next();
			if (node.hasLabel(label) == false)
				return false;
		}
		return true;
	}
	
	/**
	 * This function checks whether the node given in function parameter also has all the other properties
	 */
	
	public boolean checkRestOfNodeProperties(Node node, MyNode queryNode, Iterator<String> propertyKeyIterator, boolean skipFirst) {
		if (skipFirst)
			propertyKeyIterator.next();
		for ( ; propertyKeyIterator.hasNext(); ) {
			String propertyKey = propertyKeyIterator.next();
			String propertyValue = queryNode.getProperty(propertyKey);
			if (node.hasProperty(propertyKey))
				if ( analyzePropertyByType(node.getProperty(propertyKey), propertyValue) )
					continue;
			return false;
		}
		return true;
	}
	
	/**
	 * This function checks candidate db relations with respect to their properties.
	 * If a candidate relation doesn't have an expected property key-value mapping, then 
	 * it is not added to candidateRelationList.
	 */
	
	public void checkRelationProperties(MyRelation relation, Iterable<Relationship> candidateRelations, List<Relationship> candidateRelationList) {
		for (Iterator<Relationship> crIterator = candidateRelations.iterator(); crIterator.hasNext(); ) {
			Relationship candidateRelation = crIterator.next();
			boolean satisfied = true;
			for (Iterator<String> keyIterator = relation.getPropertyMap().keySet().iterator(); keyIterator.hasNext(); ){
				String propertyKey = keyIterator.next();
				if ( candidateRelation.hasProperty(propertyKey) )
					if ( analyzePropertyByType(candidateRelation.getProperty(propertyKey), relation.getProperty(propertyKey)) )
						continue;	
				satisfied = false;
				break;
			}
			if (satisfied)
				candidateRelationList.add(candidateRelation);
		}
	}
	
	/**
	 * This function finds out that in which data type the property value is hold in the database.
	 * Then it checks whether the property value of candidate node and property value of query node
	 * are equal or not.
	 */
	
	public boolean analyzePropertyByType(Object dbPropertyValue, String queryPropertyValue) {
		Object propertyValue;
		if ( dbPropertyValue.getClass().equals(Integer.class) )
			propertyValue = Integer.parseInt(queryPropertyValue);
		else if ( dbPropertyValue.getClass().equals(Long.class) )
			propertyValue = Long.parseLong(queryPropertyValue);
		else if ( dbPropertyValue.getClass().equals(Double.class) )
			propertyValue = Double.parseDouble(queryPropertyValue);
		else
			propertyValue = queryPropertyValue;
		if (dbPropertyValue.equals(propertyValue))
			return true;
		else
			return false;
	}
	
	/**
	 * This function filters the candidate nodes with respect to adjacent relationships' types and count 
	 * instead of checking by neighbor nodes' labels: This way was chosen since getting 
	 * neighbor nodes' labels can be done only by iterating through each relationship 
	 * which is too costly.
	 */
	
	public Iterator<Node> filterCandidatesByRelationships(MyNode queryNode, Iterator<Node> candidateNodesIterator, Direction direction) {
		// Filter with respect to relationships in direction d
		List<Node> tempCandidateNodesList = null;
		List<Object> relationTypeCountList = queryNode.getRelationTypesWithCount(direction);
		for(Iterator<Object> i = relationTypeCountList.iterator(); i.hasNext(); ) {
			RelationshipType type = (RelationshipType) i.next();
			int count = (int) i.next();
			List<Node> candidateNodesList = new ArrayList<Node>();
			for( ; candidateNodesIterator.hasNext(); ) {
				Node node = candidateNodesIterator.next();
				if ( node.getDegree(type, direction) >= count )
					candidateNodesList.add(node);
			}
			candidateNodesIterator = candidateNodesList.iterator();
			if (tempCandidateNodesList != null)
				tempCandidateNodesList.clear();	// Clear the previous step's candidate nodes to save memory
			tempCandidateNodesList = candidateNodesList;
		}
		return candidateNodesIterator;			
	}

	/**
	 * This function is the mini version of filterCandidatesByRelationships() function which 
	 * checks the relationship count of a single candidate db node, instead of many candidate 
	 * nodes through an iterator, and it returns true or false
	 */
	
	public boolean checkRelationshipCount(MyNode queryNode, Node candidateDbNode, Direction direction) {
		List<Object> relationTypeCountList = queryNode.getRelationTypesWithCount(direction);
		for(Iterator<Object> i = relationTypeCountList.iterator(); i.hasNext(); ) {
			RelationshipType type = (RelationshipType) i.next();
			int count = (int) i.next();
			if ( candidateDbNode.getDegree(type, direction) < count )
				return false;
		}
		return true;
	}
	
	/**
	 * This function filters the candidate nodes with respect to pseudo BFS trees iteratively through the depth 
	 * starting from 2 to parameter depth 
	 */
	
	public Iterator<Node> filterCandidatesByBFS(MyNode queryNode, Iterator<Node> candidateNodesIterator, int depth) {
		List<Node> tempCandidateNodesList = null;
		boolean branch_exists = false;
		for (int i=2; i<=depth; i++) {
			List<Node> candidateNodesList = new ArrayList<Node>();
			List<Integer> ancestors = new ArrayList<Integer>();
			List<List<Integer>> branches = queryGraph.dfsPaths(queryNode, i, ancestors);
			for ( ; candidateNodesIterator.hasNext(); ) {
				Node candidateNode = candidateNodesIterator.next();
				for (Iterator<List<Integer>> b = branches.iterator(); b.hasNext(); ) {
					List<Integer> branch = b.next();
					TraversalDescription td = database.traversalDescription()
													.uniqueness(Uniqueness.NODE_PATH)
													.uniqueness(Uniqueness.RELATIONSHIP_PATH)
													.expand(new DynamicPathFinder(queryGraph, branch)).depthFirst()
													.evaluator(Evaluators.toDepth(i));
					branch_exists = false;
					for ( Path position : td.traverse(candidateNode))
						if (position.length() == i)
							branch_exists = true;
					if (branch_exists == false)
						break;
				}
				if (branch_exists)
					candidateNodesList.add(candidateNode);
			}
			candidateNodesIterator = candidateNodesList.iterator();
			if (tempCandidateNodesList != null)
				tempCandidateNodesList.clear();	// Clear the previous step's candidate nodes to save memory
			tempCandidateNodesList = candidateNodesList;
		}
		return candidateNodesIterator;
	}

	/**
	 * This function orders the ids of query nodes in candidateOrderSize with respect to candidateNodes.size() 
	 */
	
	public void insertByOrder(int node_id, int candidateListSize) {
		int upperBorder = candidateSizeOrder.size(), lowerBorder = 0;
		if (upperBorder == 0) {
			candidateSizeOrder.add(node_id);
			return;
		}
		for (int pivot = (upperBorder - lowerBorder) / 2;  ; pivot = lowerBorder + (upperBorder - lowerBorder) / 2) {
			int index = candidateSizeOrder.get(pivot);
			if (pivot == lowerBorder) {
				if (candidateListSize < candidateNodes.get(index).size())
					candidateSizeOrder.add(lowerBorder, node_id);
				else
					candidateSizeOrder.add(upperBorder, node_id);
				break;
			}
			if (candidateListSize < candidateNodes.get(index).size())
				upperBorder = pivot;
			else
				lowerBorder = pivot;
		}
	}
}
