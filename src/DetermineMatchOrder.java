import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DetermineMatchOrder {

	private List<List<Integer>> relationNodeMatrix = null;
	private Map<RelationshipType, Integer> matrixIndexOfRelationType = null; 
	private List<MyNode> queryNodes = null;
	
	public DetermineMatchOrder(List<MyNode> queryNodes) {
		this.relationNodeMatrix = new ArrayList<List<Integer>>();
		this.matrixIndexOfRelationType = new HashMap<RelationshipType, Integer>();
		this.queryNodes = queryNodes;
	}
	
	/**
	 * This function fills the relationNodeMatrix which is the matrix including a
	 * relation type with its direction in each row and each of its columns represents 
	 * a query node and each entry shows whether the corresponding query node has 
	 * the corresponding relationship type with the specified direction, or not.
	 */
	
	public void createNodeRelationMatrix() {
		for (int nodeId=0; nodeId < queryNodes.size(); nodeId++) {
			MyNode queryNode = queryNodes.get(nodeId);
			List<MyRelation> relationList;
			for (int directionId=0; directionId < 2; directionId++) {
				if (directionId == 0)
					relationList = queryNode.getRelations(Direction.OUTGOING);
				else
					relationList = queryNode.getRelations(Direction.INCOMING);
				for (int relationId=0; relationId < relationList.size(); relationId++) {
					MyRelation relation = relationList.get(relationId);
					RelationshipType type = relation.getType();
					int index;
					List<Integer> outgoingRelationRow, incomingRelationRow;
					if ( matrixIndexOfRelationType.containsKey(type) ) {
						index = matrixIndexOfRelationType.get(type);
						outgoingRelationRow = relationNodeMatrix.get(index*2);
						incomingRelationRow = relationNodeMatrix.get(index*2 + 1);
					}
					else {
						index = matrixIndexOfRelationType.size();
						matrixIndexOfRelationType.put(type, index);
						outgoingRelationRow = new ArrayList<Integer>();
						incomingRelationRow = new ArrayList<Integer>();
						for(int n=0; n < queryNodes.size(); n++) {
							outgoingRelationRow.add(0);
							incomingRelationRow.add(0);
						}
						relationNodeMatrix.add(outgoingRelationRow);
						relationNodeMatrix.add(incomingRelationRow);
					}
					if (directionId == 0) 
						outgoingRelationRow.set(nodeId, 1);
					else
						incomingRelationRow.set(nodeId, 1);
				}
			}
		}
	}
	
	/**
	 * This function saves the content of relationNode matrix into a String variable.
	 */
	
	public String printRelationNodeMatrix() {
		String content = "The Content of Node-Relation Matrix\n";
		for (int i=0; i < relationNodeMatrix.size(); i++) {
			List<Integer> matrixRow = relationNodeMatrix.get(i);
			for (int j=0; j < queryNodes.size(); j++) {
				content += matrixRow.get(j) + " ";
			}
			content += "\n";
		}
		return content;
	}
}
