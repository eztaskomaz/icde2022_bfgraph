import org.graphstream.algorithm.BetweennessCentrality;
import org.graphstream.algorithm.measure.ClosenessCentrality;
import org.graphstream.algorithm.measure.EigenvectorCentrality;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;

import java.util.*;

public class Centrality {

    //Other centrality methods can be calculated one time based on not changing order

    public Graph createGraphForCentrality(Query queryGraph) {
        Graph graph = new SingleGraph("QueryGraph");
        for(MyNode myNode:queryGraph.getQueryNodes()) {
            Node node = graph.addNode(myNode.getId().toString());
        }

        for(Map.Entry<Pair<Integer,Integer>, MyRelation> IdPairRelationMap: queryGraph.getRelationMap().entrySet()) {
            Pair<Integer, Integer> endStartNodePair = IdPairRelationMap.getKey();
            MyRelation relation = IdPairRelationMap.getValue();
            Edge edge = graph.addEdge(endStartNodePair.first().toString() + endStartNodePair.other().toString(), endStartNodePair.first().toString(), endStartNodePair.other().toString(), true);
            if(relation.getWeight() > 0) {
                edge.setAttribute("weight", relation.getWeight());
            }
        }
        return graph;
    }

    public List<MyNode> determineMatchingOrderBasedOnNewApproach(List<MyNode> queryNodes, Query queryGraph, GraphDatabaseService database) {
        calculateRelationTypeCount(queryNodes, database);
        Graph graph = createGraphForCentrality(queryGraph);
        List<MyNode> orderedNodeList = determineMatchingOrderBasedOnClosenessCentrality(graph, queryNodes);
        return orderedNodeList;
    }

    public void calculateRelationTypeCount(List<MyNode> queryNodes, GraphDatabaseService database) {
        Set<MyRelation> allRelationsInQueryGraph = new HashSet<>();
        Set<RelationshipType> relationshipTypeSet = new HashSet<>();
        queryNodes.forEach(queryNode -> {
            List<MyRelation> allRelations = queryNode.getRelations(Direction.BOTH);
            allRelations.forEach(relation -> {
                RelationshipType type = relation.getType();
                relationshipTypeSet.add(type);
            });
            allRelationsInQueryGraph.addAll(allRelations);
        });
        relationshipTypeSet.forEach(relationshipType -> {
            Integer relationshipCount = getRelationshipCount(database, relationshipType);
            allRelationsInQueryGraph.forEach(relation ->  {
                if(relation.getType().equals(relationshipType)) {
                    relation.setCountOnGraph(relationshipCount);
                }
            });
        });
        List<MyRelation> orderedRelationListBasedOnCount = new ArrayList<>();
        orderedRelationListBasedOnCount.addAll(allRelationsInQueryGraph);
        Collections.sort(orderedRelationListBasedOnCount, (t1, t2) -> t1.getCountOnGraph().compareTo(t2.getCountOnGraph()));
        int weight = 0;
        for(MyRelation orderedRelation : orderedRelationListBasedOnCount){
            orderedRelation.setWeight(weight);
            weight++;
        }
    }

    public Integer getRelationshipCount(GraphDatabaseService db, RelationshipType relationshipType) {
        try (Transaction ignored = db.beginTx();
             Result result = db.execute( "MATCH (n)-[r:" + relationshipType.name() + "]-() RETURN COUNT(r) AS COUNT" ) )
        {
            int resultSize = Integer.parseInt(result.columnAs("COUNT").next().toString());
            return resultSize;
        }
    }

    public List<MyNode> determineMatchingOrderBasedOnDegreeCentrality(List<MyNode> queryNodes) {
        List<MyNode> orderedNodeList = new ArrayList<>();
        orderedNodeList.addAll(queryNodes);
        Collections.sort(orderedNodeList, (t1, t2) -> t1.getDegreeForOrder().compareTo(t2.getDegreeForOrder()));
        return orderedNodeList;
    }

    public List<MyNode> determineMatchingOrderBasedOnClosenessCentrality(Graph graph, List<MyNode> queryNodes) {
        List<MyNode> orderedNodeList = new ArrayList<>();
        orderedNodeList.addAll(queryNodes);
        if (orderedNodeList.size() > 2) {
            ClosenessCentrality cc = new ClosenessCentrality();
            cc.init(graph);
            cc.compute();

            orderedNodeList.forEach(orderedNode -> {
                Node node = graph.getNode(orderedNode.getId().toString());
                String closeness = node.getAttribute("closeness").toString();
                if(closeness == "Infinity") {
                    orderedNode.setCentralityValue(0.000000001d);
                } else {
                    orderedNode.setCentralityValue(Double.valueOf(closeness));
                }
            });
            Collections.sort(orderedNodeList, (t1, t2) -> t1.getCentralityValue().compareTo(t2.getCentralityValue()));
        }
        return orderedNodeList;
    }

    public List<MyNode> determineMatchingOrderBasedOnBetweenlessCentrality(Graph graph, List<MyNode> queryNodes) {
        List<MyNode> orderedNodeList = new ArrayList<>();
        orderedNodeList.addAll(queryNodes);
        if (orderedNodeList.size() > 2) {
            BetweennessCentrality bc = new BetweennessCentrality();
            bc.computeEdgeCentrality(false);
            bc.betweennessCentrality(graph);

            orderedNodeList.forEach(orderedNode -> {
                Node node = graph.getNode(orderedNode.getId().toString());
                orderedNode.setCentralityValue(Double.valueOf(node.getAttribute("Cb").toString()));
            });
            Collections.sort(orderedNodeList, (t1, t2) -> t1.getCentralityValue().compareTo(t2.getCentralityValue()));
        }
        return orderedNodeList;
    }

    public List<MyNode> determineMatchingOrderBasedOnEigenvectorCentrality(Graph graph, List<MyNode> queryNodes) {
        List<MyNode> orderedNodeList = new ArrayList<>();
        orderedNodeList.addAll(queryNodes);
        if (orderedNodeList.size() > 2) {
            EigenvectorCentrality ec = new EigenvectorCentrality();
            ec.init(graph);
            ec.compute();

            orderedNodeList.forEach(orderedNode -> {
                Node node = graph.getNode(orderedNode.getId().toString());
                orderedNode.setCentralityValue(Double.valueOf(node.getAttribute("DEFAULT_ATTRIBUTE_KEY").toString()));
            });
            Collections.sort(orderedNodeList, (t1, t2) -> t1.getCentralityValue().compareTo(t2.getCentralityValue()));
        }
        return orderedNodeList;
    }


}
