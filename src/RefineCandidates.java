import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;

import java.util.*;

public class RefineCandidates {

	private Map<Pair<MyNode, Node>, Integer> markedNodesInfo = new HashMap<Pair<MyNode, Node>, Integer>();
	private List<List<Node>> candidateNodes;
	private Query queryGraph;
	GraphDatabaseService database;
	
	/**
	 * Constructor of the class
	 */
	
	public RefineCandidates(GraphDatabaseService database, List<List<Node>> candidateNodes, List<MyNode> queryNodes, Query queryGraph) {
		this.candidateNodes = candidateNodes;
		this.queryGraph = queryGraph;
		this.database = database;
		
		for (int i=0; i < queryNodes.size(); i++) {
			MyNode queryNode = queryNodes.get(i);
			List<Node> candidateNodesList = candidateNodes.get(i);
			for (int j=0; j < candidateNodesList.size(); j++) {
				Node candidateNode = candidateNodesList.get(j);
				Pair<MyNode, Node> keyPair = Pair.of(queryNode, candidateNode);
				markedNodesInfo.put(keyPair, 1);	// 1 --> marked, 0 --> not marked
			}
		}
	}
	
	/**
	 * This function refines the candidates by examining their neighborhood with respect to
	 * the condition that if v is a candidate for u, then for every neighbor node of u, there 
	 * must be a candidate node in the neighborhood of v.
	 */
	
	public void refineSearchSpace(int refinementLevel) {
		for (int level=1; level <= refinementLevel; level++) {
			List<Pair<MyNode, Node>> toBeRemovedPairs = new ArrayList<Pair<MyNode, Node>>();
			List<Pair<MyNode, Node>> toBeUnmarkedPairs = new ArrayList<Pair<MyNode, Node>>();
			List<Pair<MyNode, Node>> toBeMarkedPairs = new ArrayList<Pair<MyNode, Node>>();
			for (Iterator<Pair<MyNode, Node>> keyIterator = markedNodesInfo.keySet().iterator(); keyIterator.hasNext(); ) {
				Pair<MyNode, Node> key = keyIterator.next();
				MyNode queryNode = key.first();
				Node candidateNode = key.other();
				Map<Integer, List<Node>> bipartiteMatrix = new HashMap<Integer, List<Node>>();
				deduceCandidatesInNeighborhood(Direction.OUTGOING, queryNode, candidateNode, bipartiteMatrix);
				deduceCandidatesInNeighborhood(Direction.INCOMING, queryNode, candidateNode, bipartiteMatrix);
				if (isBipartiteSemiperfect(bipartiteMatrix))
					toBeUnmarkedPairs.add(key);
				else {
					toBeRemovedPairs.add(key);
					for (Iterator<Integer> b = bipartiteMatrix.keySet().iterator(); b.hasNext(); ) {
						int neighborId = b.next();
						List<Node> bipartiteCandidates = bipartiteMatrix.get(neighborId);
						for (Iterator<Node> c = bipartiteCandidates.iterator(); c.hasNext(); ) {
							Node candidate = c.next();
							toBeMarkedPairs.add(Pair.of(queryGraph.getNodeById(neighborId), candidate));
						}
					}
				}
				clearBipartiteMatrix(bipartiteMatrix);
			}
			if (toBeMarkedPairs.isEmpty())
				level = refinementLevel; // break;
			handleMarkings(toBeRemovedPairs, toBeUnmarkedPairs, toBeMarkedPairs);
		}
		markedNodesInfo.clear();
		return;
	}
	
	/**
	 * This function handles the mark-unmark-remove operations for the <queryNode,candidateNode> pairs.
	 * It updates markedNodesInfo map and candidateNodes list accordingly.
	 */
	
	public void handleMarkings(List<Pair<MyNode, Node>> toBeRemovedPairs, List<Pair<MyNode, Node>> toBeUnmarkedPairs, List<Pair<MyNode, Node>> toBeMarkedPairs) {
		// Here, the order of handling lists is important!
		for (int i=0; i < toBeUnmarkedPairs.size(); i++) {
			Pair<MyNode, Node> key = toBeUnmarkedPairs.get(i);
			markedNodesInfo.remove(key);
		}
		for (int i=0; i < toBeMarkedPairs.size(); i++) {
			Pair<MyNode, Node> key = toBeMarkedPairs.get(i);
			markedNodesInfo.put(key, 1);
		}
		for (int i=0; i < toBeRemovedPairs.size(); i++) {
			Pair<MyNode, Node> key = toBeRemovedPairs.get(i);
			markedNodesInfo.remove(key);
			candidateNodes.get(key.first().getId()).remove(key.other());
		}
		toBeRemovedPairs.clear();
		toBeUnmarkedPairs.clear();
		toBeMarkedPairs.clear();
	}
	
	/**
	 * This function deduces the candidate nodes for neighbors of queryNode residing in the neighborhood 
	 * of candidateNode. The 1-neighborhood of nodes is analyzed.
	 */
	
	public void deduceCandidatesInNeighborhood(Direction direction, MyNode queryNode, Node candidateNode, Map<Integer, List<Node>> bipartiteMatrix) {
		try ( Transaction tx = database.beginTx() ) 
	    {
			for (Iterator<MyRelation> r = queryNode.getRelations(direction).iterator(); r.hasNext(); ) {
				MyRelation queryRelation = r.next();
				int neighborId = queryRelation.getTheOtherNodeId(queryNode.getId());
				List<Node> neighborCandidates = candidateNodes.get(neighborId);
				List<Node> bipartiteCandidates = new ArrayList<Node>();
				for (Iterator<Relationship> s = candidateNode.getRelationships(direction, queryRelation.getType()).iterator(); s.hasNext(); ) {
					Relationship candidateRelation = s.next();
					Node potentialNeighborCandidate = candidateRelation.getOtherNode(candidateNode);
					if (neighborCandidates.contains(potentialNeighborCandidate))
						bipartiteCandidates.add(potentialNeighborCandidate);
				}
				bipartiteMatrix.put(neighborId, bipartiteCandidates);
			}
			tx.success();
	    	tx.close();
	    }
		 catch (Exception e) {}
	}
	
	/**
	 * This function checks whether the bipartite matrix is semi-perfect or not; in other words, whether 
	 * ever key (neighborId) in bipartiteMatrix map can be matched with a different candidate.
	 */
	
	public boolean isBipartiteSemiperfect(Map<Integer, List<Node>> bipartiteMatrix) {
		Map<Integer, Integer> startIndexes = new HashMap<Integer, Integer>();
		Map<Integer, Long> usedNodeIds = new HashMap<Integer, Long>();
		List<Integer> keyOrder = new ArrayList<Integer>();
		
		// Check if there is an empty bipartiteCandidates list
		for (Iterator<Integer> keyIterator = bipartiteMatrix.keySet().iterator(); keyIterator.hasNext(); ) {
			int key = keyIterator.next();
			List<Node> bipartiteCandidates = bipartiteMatrix.get(key);
			if (bipartiteCandidates.isEmpty()) {
				clearStorage(startIndexes, usedNodeIds, keyOrder);
				return false;
			}
			startIndexes.put(key, 0);
			usedNodeIds.put(key, (long) -1);
			keyOrder.add(key);
		}
		// This part can be implemented more efficient
		for (int i=0; i < keyOrder.size(); ) {
			if (i < 0) {
				clearStorage(startIndexes, usedNodeIds, keyOrder);
				return false;
			}
			int neighborId = keyOrder.get(i);
			List<Node> bipartiteCandidates = bipartiteMatrix.get(neighborId);
			boolean isFound = false;
			for (int j = startIndexes.get(neighborId); j < bipartiteCandidates.size(); j++) {
				Long toBeUsedNodeId = bipartiteCandidates.get(j).getId();
				if (usedNodeIds.containsValue(toBeUsedNodeId) == false) {
					usedNodeIds.put(neighborId, toBeUsedNodeId);
					startIndexes.put(neighborId, j+1);
					isFound = true;
					i++;
					break;
				}
			}
			if (isFound == false) {
				usedNodeIds.put(neighborId, (long) -1);
				startIndexes.put(neighborId, 0);
				i--;
			}
		}
		clearStorage(startIndexes, usedNodeIds, keyOrder);
		return true;
	}
	
	/**
	 * This function clears the content of bipartite matrix by cleaning each value list also.
	 */
	
	public void clearBipartiteMatrix(Map<Integer, List<Node>> bipartiteMatrix) {
		for (Iterator<Integer> b = bipartiteMatrix.keySet().iterator(); b.hasNext(); ) {
			int key = b.next();
			List<Node> valueList = bipartiteMatrix.get(key);
			valueList.clear();
		}
		bipartiteMatrix.clear();
	}
	
	/**
	 * This function clear the storage of given maps and list
	 */
	
	public void clearStorage(Map<Integer, Integer> startIndexes, Map<Integer, Long> usedNodeIds, List<Integer> keyOrder) {
		startIndexes.clear();
		usedNodeIds.clear();
		keyOrder.clear();
	}
	
}
