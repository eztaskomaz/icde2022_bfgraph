import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class Query {

    private String query;
    private List<MyNode> queryNodes;
    private Iterable<RelationshipType> allRelationTypes;
    private Map<Integer, MyNode> nodeMap = new HashMap<Integer, MyNode>();
    private Map<Pair<Integer,Integer>, MyRelation> relationMap = new HashMap<>();

    public Query(List<MyNode> queryNodes, String query, Iterable<RelationshipType> allRelationTypes) {
        this.queryNodes = queryNodes;
        this.query = query;
        this.allRelationTypes = allRelationTypes;
    }

    public List<MyNode> getQueryNodes() {
        return queryNodes;
    }

    public List<MyNode> getQueryNodesWithLabel(Label label) {
        return queryNodes.stream().filter(node -> node.getLabel().equals(label)).collect(Collectors.toList());
    }

    public void setQueryNodes(List<MyNode> queryNodes) {
        this.queryNodes = queryNodes;
    }

    public MyNode getNodeById(int id) {
        return nodeMap.get(id);
    }

    public MyRelation getRelationByNodeIds(int startNodeId, int endNodeId) {
        return relationMap.get(Pair.of(startNodeId, endNodeId));
    }

    public void extractQueryItems() {
        String[] edges = query.split("\\)");
        String[] edgeItems = new String[6];

        for(int i=0; i<edges.length; i++) {
            edges[i] = edges[i].substring(1);
            edgeItems = edges[i].split(",");
            int node1_id = Integer.parseInt(edgeItems[0]);
            int node2_id = Integer.parseInt(edgeItems[1]);

            if (queryNodes.size() < node1_id+1 )
                defineNode(edgeItems[2]);
            if (queryNodes.size() < node2_id+1 )
                defineNode(edgeItems[5]);

            MyNode node1  = queryNodes.get( node1_id );
            MyNode node2 = queryNodes.get( node2_id );
            relateNodes(node1, node2, edgeItems[3], edgeItems[4], i);
        }
    }

    private void defineNode(String qualifications) {
        MyNode tempNode = new MyNode();
        int count = qualifications.split("&").length;
        String[] nodeItems = new String[count];
        nodeItems = qualifications.split("&");
        for (int i=1; i < count; i++) {
            String[] property = nodeItems[i].split("=");
            tempNode.setProperty(property[0], property[1]);
        }
        tempNode.addLabel(DynamicLabel.label(nodeItems[0]));
        tempNode.setId(queryNodes.size());
        nodeMap.put(queryNodes.size(), tempNode);
        queryNodes.add(tempNode);
    }

    private void relateNodes(MyNode node1, MyNode node2, String qualifications, String direction, int relationId) {
        if (node1.isNeighborTo(node2.getId()) == false) {
            node1.addNeighbor(node2.getId());
            node2.addNeighbor(node1.getId());
        }

        if (direction.equals("INCOMING")) {
            MyNode tempNode = node1;
            node1 = node2;
            node2 = tempNode;
        }

        MyRelation newRelation = defineRelation(qualifications, relationId, node1.getId(), node2.getId());
        node1.addRelation(newRelation, Direction.OUTGOING);
        node2.addRelation(newRelation, Direction.INCOMING);
    }

    private MyRelation defineRelation(String qualifications, int relationId, int startNodeId, int endNodeId) {
        int count = qualifications.split("&").length;
        String[] relationItems = new String[count];
        relationItems = qualifications.split("&");
        String relationTypeName = relationItems[0];
        RelationshipType relationType = null;

        for (Iterator<RelationshipType> j = allRelationTypes.iterator(); j.hasNext(); ) {
            RelationshipType type = j.next();
            if (type.name().equals(relationTypeName)) {
                relationType = type;
                break;
            }
        }
        MyRelation newRelation = new MyRelation(relationType, relationId, startNodeId, endNodeId);
        for (int i=1; i < count; i++) {
            String[] property = relationItems[i].split("=");
            newRelation.setProperty(property[0], property[1]);
        }
        relationMap.put(Pair.of(startNodeId, endNodeId), newRelation);

        return newRelation;
    }

    public List<List<Integer>> dfsPaths(MyNode startNode, int depth, List<Integer> ancestors) {
        List<List<Integer>> resultList = new ArrayList<List<Integer>>();
        List<Integer> visitedNodeIds = new ArrayList<Integer>(); // Holds the visited nodes at the current depth
        List<Integer> branch = new ArrayList<Integer>();
        int lastIndex = ancestors.size();
        int startNodeId = startNode.getId();
        ancestors.add(startNodeId);
        for (Iterator<Integer> i = ancestors.iterator(); i.hasNext(); )
            branch.add(i.next());
        if (depth == 0 || startNode.getDegree() == 0) {
            resultList.add(branch);
            ancestors.remove(lastIndex);
            return resultList;
        }

        for (Iterator<MyRelation> i = startNode.getRelations(Direction.OUTGOING).iterator(); i.hasNext(); ) {
            MyRelation relation = i.next();
            int node_id = relation.getTheOtherNodeId(startNodeId);
            if (ancestors.contains(node_id) == false && visitedNodeIds.contains(node_id) == false) {
                visitedNodeIds.add(node_id);
                MyNode node = nodeMap.get(node_id);
                resultList.addAll( dfsPaths(node, depth-1, ancestors) );
            }
        }
        for (Iterator<MyRelation> i = startNode.getRelations(Direction.INCOMING).iterator(); i.hasNext(); ) {
            MyRelation relation = i.next();
            int node_id = relation.getTheOtherNodeId(startNodeId);
            if (ancestors.contains(node_id) == false && visitedNodeIds.contains(node_id) == false) {
                visitedNodeIds.add(node_id);
                MyNode node = nodeMap.get(node_id);
                resultList.addAll( dfsPaths(node, depth-1, ancestors) );
            }
        }
        if (visitedNodeIds.isEmpty())
            resultList.add(branch);
        ancestors.remove(lastIndex);
        visitedNodeIds.clear();
        return resultList;
    }

    public Map<Pair<Integer, Integer>, MyRelation> getRelationMap() {
        return relationMap;
    }

}
