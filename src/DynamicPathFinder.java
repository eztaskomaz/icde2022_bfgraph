import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicPathFinder implements PathExpander<String> {

	private List<Integer> branch;
	private Query queryGraph;
	
	/**
	 * The constructor of the class
	 */
	
	public DynamicPathFinder(Query queryGraph, List<Integer> branch) {
		this.queryGraph = queryGraph;
		this.branch = branch;
	}
	
	/**
	 * This function returns the suitable relationships which will expand the path one more edge by following 
	 * the order of  node labels, relationship types and relationship directions supplied in branch.
	 */
	
	@Override
	public Iterable<Relationship> expand(Path path, BranchState<String> state) {
		List<Relationship> results = new ArrayList<Relationship>();
		int length = path.length();
		int firstNodeId = branch.get(length);
		int secondNodeId = branch.get(length+1);
		MyNode secondNode = queryGraph.getNodeById(secondNodeId);
		MyRelation relation = queryGraph.getRelationByNodeIds(firstNodeId, secondNodeId);
		if (relation == null)
			relation = queryGraph.getRelationByNodeIds(secondNodeId, firstNodeId);
			
		Node currentNode = path.endNode();
		for (Relationship r : currentNode.getRelationships(relation.getType(), relation.getDirection(firstNodeId))) {
			Node nextNode = r.getOtherNode(currentNode);
			for (Iterator<Label> i = secondNode.getLabels().iterator(); ; ) {
				Label label = i.next();
				if (nextNode.hasLabel(label) == false)
					break;
				if (i.hasNext() == false) {
					results.add(r);
					break;
				}
			}
		}
		return results;
	}

	/**
	 * This function comes from PathExpander interface. It just returns null.
	 */
	
	@Override
	public PathExpander<String> reverse() {
		// TODO Auto-generated method stub
		return null;
	}

}
