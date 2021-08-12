import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;

import java.util.*;

public class DualIso {

    private final GraphDatabaseService database;
    private Iterable<RelationshipType> allRelationTypes;
    private List<MyNode> queryNodes = new ArrayList<MyNode>();

    private Set<Relationship> incomingRelationListOfDataVertex = new HashSet<>();
    private Set<Relationship> outgoingRelationListOfDataVertex = new HashSet<>();
    private Set<Relationship> allTypesOfRelationListOfDataVertex = new HashSet<>();
    private Set<Node> neighborListOfDataVertex = new HashSet<>();

    public DualIso(GraphDatabaseService database) {
        this.database = database;
        try (Transaction tx = database.beginTx())
        {
            allRelationTypes = database.getAllRelationshipTypes();
            tx.success();
            tx.close();
        }
        catch (Exception e) {
            e.getMessage();
        }
    }

    public GraphDatabaseService getDatabase() {
        return database;
    }

    public Iterable<RelationshipType> getAllRelationTypes() {
        return allRelationTypes;
    }

    public void setAllRelationTypes(Iterable<RelationshipType> allRelationTypes) {
        this.allRelationTypes = allRelationTypes;
    }

    public List<MyNode> getQueryNodes() {
        return queryNodes;
    }

    public void setQueryNodes(List<MyNode> queryNodes) {
        this.queryNodes = queryNodes;
    }

    public void executeQuery(String query) {
        MeasureSourceConsumption msc = new MeasureSourceConsumption();
        msc.startCalculateTimeAndCpuConsumption();

        Query queryGraph = new Query(queryNodes, query, allRelationTypes);
        queryGraph.extractQueryItems();

        Map<MyNode, Set<Node>> matches = findMatches(queryGraph);

        msc.endCalculateTimeAndCpuConsumption();
        msc.printConsumptions(matches.size());
    }

    private Map<MyNode, Set<Node>> findMatches(Query queryGraph) {
        Map<MyNode, Set<Node>> matches = new LinkedHashMap<>();
        Map<MyNode, Set<Node>> queryVertexFeasibleSolutionMap = getFeasibleSolutionMapForAllQueryVertices(queryGraph);
        dualSim(queryGraph,queryVertexFeasibleSolutionMap);
        search(queryGraph, queryVertexFeasibleSolutionMap, matches, 0);
        return matches;
    }

    private Map<MyNode,Set<Node>> getFeasibleSolutionMapForAllQueryVertices(Query queryGraph) {
        Map<MyNode, Set<Node>> queryVertexFeasibleSolutionMap = new LinkedHashMap<>();
        queryGraph.getQueryNodes().forEach(queryVertex -> {
            //Set<Node> feasibleSolutionForGivenNode = getFeasibleSolutionForGivenNode(queryVertex);
            Set<Node> feasibleSolutionForGivenNode = getFeasibleSolutionForGivenNodeBasedOnProperties(queryVertex);
            queryVertexFeasibleSolutionMap.put(queryVertex, feasibleSolutionForGivenNode);
        });
        return queryVertexFeasibleSolutionMap;
    }

    private void search(Query queryGraph, Map<MyNode, Set<Node>> feasibleSolutionList, Map<MyNode, Set<Node>> matches, Integer depth) {
        if(depth == queryGraph.getQueryNodes().size()) {
            if(matches.isEmpty()) {
                matches.putAll(feasibleSolutionList);
            } else {
                for (Map.Entry<MyNode, Set<Node>> entry : feasibleSolutionList.entrySet()) {
                    if(matches.get(entry.getKey()) != null) {
                        matches.get(entry.getKey()).addAll(entry.getValue());
                    } else {
                        matches.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        } else {
            for(Node dataVertex : feasibleSolutionList.get(queryGraph.getNodeById(depth))) {
                if(dataVertexIsNotInOtherFeasibleSolution(queryGraph, dataVertex, feasibleSolutionList, depth)) {
                    Map<MyNode, Set<Node>> copyOfFeasibleSolutionList = copyMap(feasibleSolutionList);
                    copyOfFeasibleSolutionList.get(queryGraph.getNodeById(depth)).clear();
                    copyOfFeasibleSolutionList.get(queryGraph.getNodeById(depth)).add(dataVertex);
                    dualSim(queryGraph, copyOfFeasibleSolutionList);
                    if(!copyOfFeasibleSolutionList.isEmpty()) {
                        search(queryGraph, copyOfFeasibleSolutionList, matches, (depth+1));
                    }
                }
            }
        }
    }

    private Map<MyNode, Set<Node>> copyMap(Map<MyNode, Set<Node>> originalMap) {
        Map<MyNode, Set<Node>> copyMap = new LinkedHashMap<>();
        for (Map.Entry<MyNode, Set<Node>> entry : originalMap.entrySet()) {
            copyMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return copyMap;
    }

    private Set<Node> copySet(Set<Node> originalSet) {
        Set<Node> copySet = new HashSet<>();
        for (Node node : originalSet) {
            copySet.add(node);
        }
        return copySet;
    }

    private boolean dataVertexIsNotInOtherFeasibleSolution(Query queryGraph, Node dataVertex, Map<MyNode, Set<Node>> exactFeasibleSolutionList, Integer depth) {
        for(int i = 0; i < depth; i++) {
            if(exactFeasibleSolutionList.get(queryGraph.getNodeById(i)).contains(dataVertex)) {
                return false;
            }
        }
        return true;
    }

    private Map<MyNode, Set<Node>> dualSim(Query queryGraph, Map<MyNode, Set<Node>> queryVertexFeasibleSolutionMap) {
        boolean changed = true;
        while(changed) {
            changed = false;
            for(MyNode queryVertex : queryGraph.getQueryNodes()) {
                for(MyNode adjacentQueryVertex : queryVertex.getNeighbors(queryGraph)) {
                    Set<Node> subFeasibleSolutionListForAdjacentQueryVertex = new HashSet<>();
                    Set<Node> feasibleSolutionListForQueryVertex = new HashSet<>();
                    feasibleSolutionListForQueryVertex.addAll(queryVertexFeasibleSolutionMap.get(queryVertex));
                    for(Node dataVertexInFeasibleSolutionList : feasibleSolutionListForQueryVertex) {
                        getDataVertexNeighbors(dataVertexInFeasibleSolutionList);
                        Set<Node> adjacentDataVertexOfDataVertexInFeasibleSolutionList = copySet(neighborListOfDataVertex);
                        Set<Node> feasibleSolutionListForAdjacentQueryVertexBasedOnDataVertex = new HashSet<>();
                        feasibleSolutionListForAdjacentQueryVertexBasedOnDataVertex.addAll(adjacentDataVertexOfDataVertexInFeasibleSolutionList);
                        feasibleSolutionListForAdjacentQueryVertexBasedOnDataVertex.retainAll(queryVertexFeasibleSolutionMap.get(adjacentQueryVertex));
                        if(feasibleSolutionListForAdjacentQueryVertexBasedOnDataVertex.isEmpty()) {
                            queryVertexFeasibleSolutionMap.get(queryVertex).remove(dataVertexInFeasibleSolutionList);
                            if(queryVertexFeasibleSolutionMap.get(queryVertex).isEmpty()) {
                                return new LinkedHashMap<>();
                            }
                            changed = true;
                        }
                        subFeasibleSolutionListForAdjacentQueryVertex.addAll(feasibleSolutionListForAdjacentQueryVertexBasedOnDataVertex);
                    }
                    if(subFeasibleSolutionListForAdjacentQueryVertex.isEmpty()) {
                        return new LinkedHashMap<>();
                    }
                    if(subFeasibleSolutionListForAdjacentQueryVertex.size() < queryVertexFeasibleSolutionMap.get(adjacentQueryVertex).size()) {
                        changed = true;
                    }
                    queryVertexFeasibleSolutionMap.get(adjacentQueryVertex).retainAll(subFeasibleSolutionListForAdjacentQueryVertex);
                }
            }
        }
        return queryVertexFeasibleSolutionMap;
    }

    private Set<Node> getFeasibleSolutionForGivenNode(MyNode queryVertex) {
        ResourceIterator<Node> allDataVerticesWithLabel = database.findNodes(queryVertex.getLabel());
        Set<Node> feasibleSolutionList = new HashSet<>();
        while(allDataVerticesWithLabel.hasNext()) {
            Node nextNode = allDataVerticesWithLabel.next();
            feasibleSolutionList.add(nextNode);
        }
        return feasibleSolutionList;
    }

    private Set<Node> getFeasibleSolutionForGivenNodeBasedOnProperties(MyNode queryVertex) {
        Set<Node> dataVertexList = new HashSet<>();
        ResourceIterator<Node> candidateNodesIterator;
        Set<String> propertyKeySet = queryVertex.getPropertyMap().keySet();

        if (!propertyKeySet.isEmpty()) {
            String firstPropertyKey = propertyKeySet.iterator().next();
            String firstPropertyValue = queryVertex.getProperty(firstPropertyKey);
            if (StringUtils.isNumeric(firstPropertyValue)) {
                candidateNodesIterator = database.findNodes(queryVertex.getLabel(), firstPropertyKey, Double.parseDouble(firstPropertyValue));
            } else {
                candidateNodesIterator = database.findNodes(queryVertex.getLabel(), firstPropertyKey, firstPropertyValue);
            }

        } else {
            candidateNodesIterator = database.findNodes(queryVertex.getLabel());
        }
        candidateNodesIterator.forEachRemaining(dataVertexList::add);
        return dataVertexList;
    }


    public void getInAnOutRelationshipsOfDataVertex(Node dataVertex) {
        this.incomingRelationListOfDataVertex = new HashSet<>();
        this.outgoingRelationListOfDataVertex = new HashSet<>();
        this.allTypesOfRelationListOfDataVertex = new HashSet<>();

        Iterable<Relationship> relationships = dataVertex.getRelationships();
        relationships.forEach(relationship -> {
            if(!incomingRelationListOfDataVertex.contains(relationship) && relationship.getEndNode().getId() != dataVertex.getId()) {
                incomingRelationListOfDataVertex.add(relationship);
            }
            if(!outgoingRelationListOfDataVertex.contains(relationship) && relationship.getEndNode().getId() == dataVertex.getId()) {
                outgoingRelationListOfDataVertex.add(relationship);
            }
        });
        allTypesOfRelationListOfDataVertex.addAll(incomingRelationListOfDataVertex);
        allTypesOfRelationListOfDataVertex.addAll(outgoingRelationListOfDataVertex);
    }

    private void getDataVertexNeighbors(Node dataVertex) {
        this.getInAnOutRelationshipsOfDataVertex(dataVertex);
        this.neighborListOfDataVertex = new HashSet<>();
        this.allTypesOfRelationListOfDataVertex.forEach(relationship -> {
            if(!this.neighborListOfDataVertex.contains(relationship.getOtherNode(dataVertex))) {
                this.neighborListOfDataVertex.add(relationship.getOtherNode(dataVertex));
            }
        });
    }

}
