import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FindIsomorphicPatterns {

	private List<Integer> matchedQueryNodeIds = new ArrayList<Integer>();
	private List<Long> matchedDbNodeIds = new ArrayList<Long>();
	private List<Long> matchedDbRelationIds = new ArrayList<Long>();
	private List<Pair<List<Long>, List<Long>>> matchedSubgraphs = new ArrayList<Pair<List<Long>, List<Long>>>();
	private List<MyNode> queryNodes;
	private List<List<Node>> candidateNodes;
	private Query queryGraph;
	private LinkedList<Integer> candidateSizeOrder;
	private int recursiveCallCount = 0;
	private final GraphDatabaseService database;
	String response;
	int matchCount = 0;


	/**
	 * Constructor of the class
	 */
	
	public FindIsomorphicPatterns(GraphDatabaseService database, List<MyNode> queryNodes, List<List<Node>> candidateNodes, Query queryGraph, LinkedList<Integer> candidateSizeOrder, String response) {
		this.database = database;
		this.queryNodes = queryNodes;
		this.candidateNodes = candidateNodes;
		this.queryGraph = queryGraph;
		this.candidateSizeOrder = candidateSizeOrder;
		this.response = response;
	}
	
	/**
	 * This function returns the matchedSubgraphs member of this class
	 */
	
	public List<Pair<List<Long>, List<Long>>> getMatchedSubgraphs() {
		return this.matchedSubgraphs;
	}

	public int getMatchedSubgraphCount() {
		return this.matchCount;
	}

	/**
	 * This function returns the recursiveCallCount member of this class
	 */
	
	public int getRecursiveCallCount() {
		return this.recursiveCallCount;
	}
	
	/**
	 * This function finds all the matches of the query graph in the Database
	 */
	
	public void findIsomorphicSubgraphs() {
		if (matchedQueryNodeIds.size() == queryNodes.size()) {
			matchedSubgraphs.add(Pair.of(matchedDbNodeIds, matchedDbRelationIds));
			matchCount++;
			return;
		}
		int u_id = nextQueryVertexId();
		for (Iterator<Node> c = candidateNodes.get(u_id).iterator(); c.hasNext(); ) {
			Node v = c.next();
			if ( matchedDbNodeIds.contains(v.getId()) == false ) {
				recursiveCallCount++;
				int discoveredEdgeCount = isJoinable(v, u_id);
				if (discoveredEdgeCount > 0 || matchedQueryNodeIds.isEmpty()) {
					matchedQueryNodeIds.add(u_id);
					matchedDbNodeIds.add(v.getId());
					findIsomorphicSubgraphs();
//					if (matchedSubgraphs.size() > 0)	// S�L
//						return;							// S�L
					// Remove the lastly added db & query nodes for backtracing:
					int lastItemIndex = matchedQueryNodeIds.size() - 1;
					matchedQueryNodeIds.remove(lastItemIndex);
					List<Long> tempMatchedDbNodeIds = new ArrayList<Long>(matchedDbNodeIds);
					tempMatchedDbNodeIds.remove(lastItemIndex);
					matchedDbNodeIds = tempMatchedDbNodeIds;
					// Remove the lastly added db relationships for backtracing:
					List<Long> tempMatchedDbRelationIds = new ArrayList<Long>();
					for (int j=0; j < matchedDbRelationIds.size() - discoveredEdgeCount; j++)
						tempMatchedDbRelationIds.add(matchedDbRelationIds.get(j));
					matchedDbRelationIds = tempMatchedDbRelationIds;
				}
			}
		}
		return;
	}
	
	/**
	 * This function checks whether Node v given in function parameter is joinable in the current 
	 * partially matched subgraph. It returns the newly discovered (matched) edge count during the match 
	 * operation of Node v.
	 */
	
	public int isJoinable(Node v, int u_id) {
		int discoveredEdgeCount = 0;
		boolean isPossible = false;
		for (int i = 0; i < matchedQueryNodeIds.size(); i++) {
			int matched_u_id = matchedQueryNodeIds.get(i);
			Long matched_v_id = matchedDbNodeIds.get(i);
			MyRelation relation = queryGraph.getRelationByNodeIds(u_id, matched_u_id);
			if (relation != null) {
				isPossible = isRelationMatchable(relation, u_id, matched_u_id, matched_v_id, v);
				if ( isPossible )
					discoveredEdgeCount++;
				else
					break;
			}
			MyRelation reverseRelation = queryGraph.getRelationByNodeIds(matched_u_id, u_id);
			if (reverseRelation != null) {
				isPossible = isRelationMatchable(reverseRelation, u_id, matched_u_id, matched_v_id, v);
				if (isPossible)
					discoveredEdgeCount++;
				else
					break;
			}
		}
		if (isPossible == false) {
			for (int i=0; i<discoveredEdgeCount; i++)
				matchedDbRelationIds.remove(matchedDbRelationIds.size()-1);
			discoveredEdgeCount = 0;
		}
		return discoveredEdgeCount;
	}

	/**
	 * Checks whether the relation between the query nodes with id u_id and matched_u_id has a match between the 
	 * correspondent db node of matched_u and db node v 
	 */
	
	public boolean isRelationMatchable(MyRelation relation, int u_id, int matched_u_id, Long matched_v_id, Node v) {
		boolean exists = false;
		try ( Transaction tx = database.beginTx() ) 
	    {
			int degree_of_v = v.getDegree(relation.getType(), relation.getDirection(u_id));
			int degree_of_matched_v = database.getNodeById(matched_v_id).getDegree(relation.getType(), relation.getDirection(matched_u_id));
			
			if (degree_of_v <= degree_of_matched_v) {
				Node dbNode = v;
				Direction direction = relation.getDirection(u_id);
				Long otherDbNodeId = matched_v_id;
				exists = doesRelationExist(dbNode, otherDbNodeId, relation.getType(), direction);
			}
			else {
				Node dbNode = database.getNodeById(matched_v_id);
				Direction direction = relation.getDirection(matched_u_id);
				Long otherDbNodeId = v.getId();
				exists = doesRelationExist(dbNode, otherDbNodeId, relation.getType(), direction);
			}
			
			tx.success();
	    	tx.close();
	    }
	    catch (Exception e) {
	    	response += e.getMessage();
	    }
		return exists;
	}

	/**
	 * This function searches for a specific relation between dbNode and otherDbNode given in function parameter.
	 * The type and direction is also given in function parameter and direction is described with respect to dbNode. 
	 */
	
	public boolean doesRelationExist(Node dbNode, Long otherDbNodeId, RelationshipType type, Direction direction) {
		for (Iterator<Relationship> r = dbNode.getRelationships(type, direction).iterator(); r.hasNext(); ) {
			Relationship dbRelation = r.next();
			if ( dbRelation.getOtherNode(dbNode).getId() == otherDbNodeId ) {
				matchedDbRelationIds.add(dbRelation.getId());
				return true;
			}
		}
		return false;
	}
	
	/**
	 * This function returns the next query vertex that will be matched by GraphQL algorithm specifications
	 */
	
	public int nextQueryVertexId() {
		int nodeId;
		for (int i=0; i < candidateSizeOrder.size(); i++) {
			nodeId = candidateSizeOrder.get(i);
			if (matchedQueryNodeIds.contains(nodeId) == false) {
				if (matchedQueryNodeIds.isEmpty())
					return nodeId;
				MyNode node = queryNodes.get(nodeId);
				for (Iterator<Integer> n = node.getNeighbors().iterator(); n.hasNext(); )
					if (matchedQueryNodeIds.contains(n.next()))
						return nodeId;
			}
		}
		return -1; // When this function is called accordingly, it will never return -1
	}
	
}
