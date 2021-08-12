import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;

import java.util.*;
import java.util.stream.Collectors;

public class VF3 {

    private final GraphDatabaseService database;
    private Iterable<RelationshipType> allRelationTypes;
    private List<MyNode> queryNodes = new ArrayList<MyNode>();

    private Set<Relationship> incomingRelationListOfDataVertex = new HashSet<>();
    private Set<Relationship> outgoingRelationListOfDataVertex = new HashSet<>();
    private Set<Relationship> allTypesOfRelationListOfDataVertex = new HashSet<>();
    private Set<Node> neighborListOfDataVertex = new HashSet<>();

    private Map<Integer, Set<MyNode>> predecessorTerminalSetLevelMap = new HashMap<>();
    private Map<Integer, Set<MyNode>> successorTerminalSetLevelMap = new HashMap<>();

    private Map<Integer, Set<MyNode>> terminalSetLevelMap = new HashMap<>();
    private Map<Integer, Set<MyNode>> coreSetLevelMap = new HashMap<>();

    private Map<Label, List<Node>> visitedNodeMapWithLabel = new HashMap<>();
    //private List<Node> visitedNodeList = new ArrayList<>();
    private Map<MyNode, List<Node>> visitedNodeMap = new HashMap<>();

    public VF3(GraphDatabaseService database) {
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

    public Set<Relationship> getIncomingRelationListOfDataVertex() {
        return incomingRelationListOfDataVertex;
    }

    public void setIncomingRelationListOfDataVertex(Set<Relationship> incomingRelationListOfDataVertex) {
        this.incomingRelationListOfDataVertex = incomingRelationListOfDataVertex;
    }

    public Set<Relationship> getOutgoingRelationListOfDataVertex() {
        return outgoingRelationListOfDataVertex;
    }

    public void setOutgoingRelationListOfDataVertex(Set<Relationship> outgoingRelationListOfDataVertex) {
        this.outgoingRelationListOfDataVertex = outgoingRelationListOfDataVertex;
    }

    public Set<Relationship> getAllTypesOfRelationListOfDataVertex() {
        return allTypesOfRelationListOfDataVertex;
    }

    public void setAllTypesOfRelationListOfDataVertex(Set<Relationship> allTypesOfRelationListOfDataVertex) {
        this.allTypesOfRelationListOfDataVertex = allTypesOfRelationListOfDataVertex;
    }

    public Set<Node> getNeighborListOfDataVertex() {
        return neighborListOfDataVertex;
    }

    public void setNeighborListOfDataVertex(Set<Node> neighborListOfDataVertex) {
        this.neighborListOfDataVertex = neighborListOfDataVertex;
    }

    public void executeQuery(String query) {
        MeasureSourceConsumption msc = new MeasureSourceConsumption();
        msc.startCalculateTimeAndCpuConsumption();

        Query queryGraph = new Query(queryNodes, query, allRelationTypes);
        queryGraph.extractQueryItems();

        Set<Map<MyNode, Node>> solutionMap = new HashSet<>();
        calculateSubGraphIsoProbabilityToFindFeasibleCandidiateForAllNodes();
        Set<MyNode> nodeExplorationSequence = new LinkedHashSet<>();
        generateNodeSequencer(nodeExplorationSequence);
        preprocessFirstGraph(nodeExplorationSequence, queryGraph);

        createVisitedNodeMap(nodeExplorationSequence);
        Map<MyNode, Node> firstState = new HashMap<>();

        match(firstState, queryGraph, nodeExplorationSequence, solutionMap);

        msc.endCalculateTimeAndCpuConsumption();
        msc.printConsumptions(solutionMap.size());
    }

    private void calculateSubGraphIsoProbabilityToFindFeasibleCandidiateForAllNodes() {
        queryNodes.forEach(node -> node.setpFeasible(calculateSubGraphIsoProbabilityToFindFeasibleCandidiateOfTheNode(node)));
    }

    private double calculateSubGraphIsoProbabilityToFindFeasibleCandidiateOfTheNode(MyNode queryVertex) {
        Set<Node> allNodesInGraphDB = findAllNodesInGraphDB();
        Set<Node> nodesInGraphDBWithGivenLabel = findNodesInGraphDBWithGivenLabel(queryVertex.getLabel());
        Set<Node> nodesInGraphDBWithGivenDegreeOrMore = findNodesInGraphDBWithGivenDegreeOrMore(queryVertex, allNodesInGraphDB);

        double probabilityToFindACandidateNodeWithNodesLabel = nodesInGraphDBWithGivenLabel.size() / allNodesInGraphDB.size();
        double probabilityToFindANodeWithGivenDegree = nodesInGraphDBWithGivenDegreeOrMore.size() / allNodesInGraphDB.size();
        return probabilityToFindACandidateNodeWithNodesLabel * probabilityToFindANodeWithGivenDegree;
    }

    private Set<Node> findAllNodesInGraphDB() {
        Set<Node> dataVertexList = new HashSet<>();
        ResourceIterable<Node> allNodes = database.getAllNodes();
        for(Node node : allNodes) {
            dataVertexList.add(node);
        }
        return dataVertexList;
    }

    private Set<Node> findNodesInGraphDBWithGivenLabel(Label label) {
        Set<Node> dataVertexList = new HashSet<>();
        ResourceIterator<Node> nodeList = database.findNodes(label);
        nodeList.forEachRemaining(dataVertexList::add);
        return dataVertexList;
    }

    private Set<Node> findNodesInGraphDBWithGivenLabelAndProperties(Label firstLabel, MyNode queryNode) {
        Set<Node> dataVertexList = new HashSet<>();
        ResourceIterator<Node> candidateNodesIterator;
        Set<String> propertyKeySet = queryNode.getPropertyMap().keySet();

        if (!propertyKeySet.isEmpty()) {
            String firstPropertyKey = propertyKeySet.iterator().next();
            String firstPropertyValue = queryNode.getProperty(firstPropertyKey);
            if (StringUtils.isNumeric(firstPropertyValue)) {
                candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, Double.parseDouble(firstPropertyValue));
            } else {
                candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, firstPropertyValue);
            }

        } else {
            candidateNodesIterator = database.findNodes(firstLabel);
        }
        candidateNodesIterator.forEachRemaining(dataVertexList::add);
        return dataVertexList;
    }

    private Set<Node> findNodesInGraphDBWithGivenDegreeOrMore(MyNode queryVertex, Set<Node> allNodesInGraphDB) {
        Set<Node> dataVertexList = new HashSet<>();
        allNodesInGraphDB.forEach(node -> {
            if(queryVertex.getDegree() <= node.getDegree()) {
                dataVertexList.add(node);
            }
        });
        return dataVertexList;
    }

    private Set<MyNode> generateNodeSequencer(Set<MyNode> nodeExplorationSequence) {
        queryNodes.forEach(node -> node.setDegreeM(0));
        MyNode minPFeasibleAndMaxDegree = findMinPFeasibleAndMaxDegree();
        nodeExplorationSequence.add(minPFeasibleAndMaxDegree);
        updateDegreeM(nodeExplorationSequence);
        fillTheNodeExplorationSequence(nodeExplorationSequence);
        return nodeExplorationSequence;
    }

    private MyNode findMinPFeasibleAndMaxDegree() {
        MyNode nodeWithMinPFeasible = new MyNode();
        for(MyNode node : queryNodes) {
            if((node.getpFeasible() > nodeWithMinPFeasible.getpFeasible()) || (node.getpFeasible() == nodeWithMinPFeasible.getpFeasible() && node.getDegree() > nodeWithMinPFeasible.getDegree())) {
                nodeWithMinPFeasible = node;
            }
        }
        return nodeWithMinPFeasible;
    }

    private void updateDegreeM(Set<MyNode> nodeExplorationSequence) {
        queryNodes.forEach(queryNode -> {
            int degreeM = 0;
            for(MyNode node : nodeExplorationSequence) {
                if(queryNode.isNeighborTo(node.getId())) {
                    degreeM++;
                }
            }
            queryNode.setDegreeM(degreeM);
        });
    }

    private void fillTheNodeExplorationSequence(Set<MyNode> nodeExplorationSequence) {
        Set<MyNode> queryNodesThatAreNotInNodeExplorationSequence = copySet(new HashSet<>(queryNodes));
        queryNodesThatAreNotInNodeExplorationSequence.removeAll(new HashSet<>(nodeExplorationSequence));
        while(!queryNodesThatAreNotInNodeExplorationSequence.isEmpty()) {
            MyNode theNodeThatGoingToAddInNodeExplorationSequence = findMaxDegreeMAndMinPFeasibleAndMaxDegree(queryNodesThatAreNotInNodeExplorationSequence);
            nodeExplorationSequence.add(theNodeThatGoingToAddInNodeExplorationSequence);
            queryNodesThatAreNotInNodeExplorationSequence.remove(theNodeThatGoingToAddInNodeExplorationSequence);
            updateDegreeM(nodeExplorationSequence);
        }
    }

    private MyNode findMaxDegreeMAndMinPFeasibleAndMaxDegree(Set<MyNode> queryNodesThatAreNotInNodeExplorationSequence) {
        MyNode nodeWithMinDegreeM = new MyNode();
        for(MyNode node: queryNodesThatAreNotInNodeExplorationSequence) {
            if((node.getDegreeM() > nodeWithMinDegreeM.getDegreeM() || (node.getDegreeM() == nodeWithMinDegreeM.getDegreeM() && node.getpFeasible() > nodeWithMinDegreeM.getpFeasible())
                    || (node.getDegreeM() == nodeWithMinDegreeM.getDegreeM() && node.getpFeasible() == nodeWithMinDegreeM.getpFeasible() && node.getDegree() > nodeWithMinDegreeM.getDegree()))) {
                nodeWithMinDegreeM = node;
            }
        }
        return nodeWithMinDegreeM;
    }

    private void preprocessFirstGraph(Set<MyNode> nodeExplorationSequence, Query queryGraph) {
        Set<MyNode> predecessorTerminalSet = new HashSet<>();
        Set<MyNode> successorTerminalSet = new HashSet<>();
        Set<MyNode> coreSet = new HashSet<>();

        predecessorTerminalSetLevelMap.put(0, predecessorTerminalSet);
        successorTerminalSetLevelMap.put(0, successorTerminalSet);
        coreSetLevelMap.put(0, coreSet);

        int i = 1;
        for(MyNode node : nodeExplorationSequence) {
            coreSet.add(node);
            coreSetLevelMap.put(i, coreSet);

            for(MyNode predecessorNode : node.getPredecessorNodeList(queryGraph)) {
                if(!predecessorTerminalSet.contains(predecessorNode) && !coreSet.contains(predecessorNode)) {
                    predecessorTerminalSet.add(predecessorNode);
                    predecessorNode.setParent(node);
                }
            }

            for(MyNode successorNode : node.getSuccessorNodeList(queryGraph)) {
                if(!successorTerminalSet.contains(successorNode) && !coreSet.contains(successorNode)) {
                    successorTerminalSet.add(successorNode);
                    successorNode.setParent(node);
                }
            }

            if(predecessorTerminalSet.contains(node)) {
                predecessorTerminalSet.remove(node);
            }

            if(successorTerminalSet.contains(node)) {
                successorTerminalSet.remove(node);
            }

            predecessorTerminalSetLevelMap.put(i, predecessorTerminalSet);
            successorTerminalSetLevelMap.put(i, successorTerminalSet);
            i = i + 1;
        }
    }

    private void createVisitedNodeMap(Set<MyNode> nodeExplorationSequence) {
        nodeExplorationSequence.forEach(node -> visitedNodeMap.put(node, new ArrayList<>()));
    }

    private boolean match(Map<MyNode, Node> state, Query queryGraph, Set<MyNode> nodeExplorationSequence, Set<Map<MyNode, Node>> solutionMap) {
        if(isGoal(state)) {
            solutionMap.add(state);
            return true;
        }
        Candidate candidate = new Candidate();
        //Candidate nextCandidate = getNextCandidate(state, candidate, nodeExplorationSequence, queryGraph);
        Candidate nextCandidate = getNextCandidateWithAlsoBasedOnProperties(state, candidate, nodeExplorationSequence, queryGraph);
        boolean result = false;
        while (nextCandidate.getGraphNode() != null && nextCandidate.getQueryNode() != null) {
            //if(isFeasible(state, nextCandidate.getQueryNode(), nextCandidate.getGraphNode(), queryGraph)) {
            if(isFeasibleBasedOnProperty(state, nextCandidate.getQueryNode(), nextCandidate.getGraphNode(), queryGraph)) {
                Map<MyNode, Node> nextState = new HashMap<>();
                nextState.putAll(state);
                nextState.put(nextCandidate.getQueryNode(), nextCandidate.getGraphNode());
                if(match(nextState, queryGraph, nodeExplorationSequence, solutionMap)) {
                    result = true;
                }
            }
            clearVisitedNodeMap(nextCandidate.getQueryNode(), nodeExplorationSequence);
            //nextCandidate = getNextCandidate(state, nextCandidate, nodeExplorationSequence, queryGraph);
            nextCandidate = getNextCandidateWithAlsoBasedOnProperties(state, nextCandidate, nodeExplorationSequence, queryGraph);
        }
        return result;
    }

    private boolean isFeasible(Map<MyNode, Node> state, MyNode nextQueryNode, Node nextGraphNode, Query queryGraph) {
        return isFeasibleSemantically(nextQueryNode, nextGraphNode) && isFeasibleStructurally(state, nextQueryNode, nextGraphNode, queryGraph);
    }

    private boolean isFeasibleBasedOnProperty(Map<MyNode, Node> state, MyNode nextQueryNode, Node nextGraphNode, Query queryGraph) {
        return isFeasibleSemanticallyBasedOnProperty(nextQueryNode, nextGraphNode) && isFeasibleStructurally(state, nextQueryNode, nextGraphNode, queryGraph);
    }

    private boolean isFeasibleStructurally(Map<MyNode, Node> state, MyNode nextQueryNode, Node nextGraphNode, Query queryGraph) {
        if(!state.isEmpty()) {
            Set<MyNode> successorNodeListForNextQueryNode = nextQueryNode.getSuccessorNodeList(queryGraph);
            Set<MyNode> predecessorNodeListForNextQueryNode = nextQueryNode.getPredecessorNodeList(queryGraph);
            getInAnOutRelationshipsOfDataVertex(nextGraphNode);
            List<Node> predecessorNodeListForNextGraphNode = getPredecessorGraphNodeList();
            List<Node> successorNodeListForNextGraphNode = getSuccessorGraphNodeList();
            return isFc(state, successorNodeListForNextQueryNode, predecessorNodeListForNextQueryNode, predecessorNodeListForNextGraphNode, successorNodeListForNextGraphNode, nextQueryNode, nextGraphNode);
        }
        return true;
    }

    private boolean isFc(Map<MyNode, Node> state, Set<MyNode> successorNodeListForNextQueryNode, Set<MyNode> predecessorNodeListForNextQueryNode, List<Node> predecessorNodeListForNextGraphNode, List<Node> successorNodeListForNextGraphNode, MyNode nextQueryNode, Node nextGraphNode) {
        List<MyNode> intersectForTheFirstRule = intersectOfTwoMyNodeLists(successorNodeListForNextQueryNode, state.keySet());
        List<MyNode> intersectForTheSecondRule = intersectOfTwoMyNodeLists(predecessorNodeListForNextQueryNode, state.keySet());
        List<Node> intersectForTheThirdRule = intersectOfTwoNodeLists(successorNodeListForNextGraphNode, state.values());
        List<Node> intersectForTheForthRule = intersectOfTwoNodeLists(predecessorNodeListForNextGraphNode, state.values());

        if(isFcFirstRule(intersectForTheFirstRule, successorNodeListForNextGraphNode, state, nextQueryNode, nextGraphNode)
                && isFcSecondRule(intersectForTheSecondRule, predecessorNodeListForNextGraphNode, state, nextQueryNode, nextGraphNode)
                && isFcThirdAndForthRuleGenerator(intersectForTheThirdRule, successorNodeListForNextQueryNode, state)
                && isFcThirdAndForthRuleGenerator(intersectForTheForthRule, predecessorNodeListForNextQueryNode, state)) {
            return true;
        }
        return false;
    }

    // TODO: rulelar tek bir tanesi için mi true dönmeli yoksa hepsini kontrol ettikten sonra mı?
    // TODO: relation bilgisini de kontrol edebilir miyiz?
    private boolean isFcFirstAndSecondRuleGenerator(List<MyNode> intersectForTheRule, List<Node> nodeList, Map<MyNode, Node> state, MyNode nextQueryNode, Node nextGraphNode) {
        if(!intersectForTheRule.isEmpty()) {
            for (MyNode myNode : intersectForTheRule) {
                if (!nodeList.contains(state.get(myNode))) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private boolean isFcFirstRule(List<MyNode> intersectForTheRule, List<Node> nodeList, Map<MyNode, Node> state, MyNode nextQueryNode, Node nextGraphNode) {
        if(!intersectForTheRule.isEmpty()) {
            for (MyNode myNode : intersectForTheRule) {
                List<Node> sameNodeList = nodeList.stream().filter(node -> node.equals(state.get(myNode))
                        && myNode.getIncomingRelationsWithNeighbor(nextQueryNode).stream().map(x->x.getType().name()).collect(Collectors.toList()).containsAll(getOutgoingRelationsWithNeighbor(node, nextGraphNode).stream().map(z->z.getType().name()).collect(Collectors.toList()))).collect(Collectors.toList());;
                if(sameNodeList.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private boolean isFcSecondRule(List<MyNode> intersectForTheRule, List<Node> nodeList, Map<MyNode, Node> state, MyNode nextQueryNode, Node nextGraphNode) {
        if(!intersectForTheRule.isEmpty()) {
            for (MyNode myNode : intersectForTheRule) {
                List<Node> sameNodeList = nodeList.stream().filter(node -> node.equals(state.get(myNode))
                        && myNode.getOutgoingRelationsWithNeighbor(nextQueryNode).stream().map(x->x.getType().name()).collect(Collectors.toList()).containsAll(getIncomingRelationsWithNeighbor(node, nextGraphNode).stream().map(z->z.getType().name()).collect(Collectors.toList()))).collect(Collectors.toList());
                if(sameNodeList.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    public List<Relationship> getIncomingRelationsWithNeighbor(Node node, Node neighborNode) {
        return incomingRelationListOfDataVertex.stream().filter(relationship -> relationship.getStartNode().equals(node) && relationship.getEndNode().equals(neighborNode)).collect(Collectors.toList());
    }

    public List<Relationship> getOutgoingRelationsWithNeighbor(Node node, Node neighborNode) {
        return outgoingRelationListOfDataVertex.stream().filter(relationship -> relationship.getEndNode() == node && relationship.getStartNode() == neighborNode).collect(Collectors.toList());
    }

    private boolean isFcThirdAndForthRuleGenerator(List<Node> intersectForTheRule, Set<MyNode> nodeList, Map<MyNode, Node> state) {
        if(!intersectForTheRule.isEmpty()) {
            for (Node node : intersectForTheRule) {
                if (!nodeList.contains(getKeyFromValue(state, node))) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    private List<Node> intersectOfTwoNodeLists(Collection<Node> firstList, Collection<Node> secondList) {
        return firstList.stream().filter(firstElement -> secondList.contains(firstElement)).collect(Collectors.toList());
    }

    private List<MyNode> intersectOfTwoMyNodeLists(Collection<MyNode> firstList, Collection<MyNode> secondList) {
        return firstList.stream().filter(firstElement -> secondList.contains(firstElement)).collect(Collectors.toList());
    }

    private boolean isFeasibleSemantically(MyNode queryNode, Node graphNode) {
        if(queryNode.getLabel().toString().equals(graphNode.getLabels().iterator().next().toString())) {
            return true;
        }
        return false;
    }

    private boolean isFeasibleSemanticallyBasedOnProperty(MyNode queryNode, Node graphNode) {
        Set<String> propertyKeySet = queryNode.getPropertyMap().keySet();
        boolean isPropertyTheSame = true;
        if(!propertyKeySet.isEmpty()) {
            String firstPropertyKey = propertyKeySet.iterator().next();
            String firstPropertyValue = queryNode.getProperty(firstPropertyKey);
            isPropertyTheSame = graphNode.hasProperty(firstPropertyKey) && firstPropertyValue.equals(graphNode.getProperty(firstPropertyKey.toString()));
        }
        boolean isLabeltheSame = queryNode.getLabel().toString().equals(graphNode.getLabels().iterator().next().toString());
        if(isLabeltheSame && isPropertyTheSame) {
            return true;
        }
        return false;
    }

    private boolean isGoal(Map<MyNode, Node> state) {
        return state.size() == queryNodes.size();
    }

    private MyNode getNextInSequence(Set<MyNode> nodeExplorationSequence, Map<MyNode, Node> currentState) {
        if(currentState.isEmpty()) {
            return nodeExplorationSequence.iterator().next();
        }
        for(MyNode myNode : nodeExplorationSequence) {
            if(currentState.get(myNode) == null) {
                return myNode;
            }
        }
        return null;
    }

    private Candidate getNextCandidate(Map<MyNode, Node> currentState, Candidate candidate, Set<MyNode> nodeExplorationSequence, Query queryGraph) {
        Candidate nextCandidate = new Candidate();
        if(candidate.getQueryNode() == null) {
            nextCandidate.setQueryNode(getNextInSequence(nodeExplorationSequence, currentState));
            if(nextCandidate.getQueryNode() == null) {
                return candidate;
            }
        } else {
            nextCandidate.setQueryNode(candidate.getQueryNode());
        }

        MyNode nextQueryNode = nextCandidate.getQueryNode();
        MyNode parentQueryNode = nextQueryNode.getParent();
        if(parentQueryNode == null) {
            nextCandidate.setGraphNode(getNextNodeBasedOnNotConnectedNodes(currentState, nextQueryNode.getLabel(), nextQueryNode));
            return nextCandidate;
        } else {
            Node parentGraphNode = currentState.get(parentQueryNode);
            Set<MyNode> predecessorNodeListofParentQueryNode = parentQueryNode.getPredecessorNodeList(queryGraph);
            Set<MyNode> successorNodeListofParentQueryNode = parentQueryNode.getSuccessorNodeList(queryGraph);
            getInAnOutRelationshipsOfDataVertex(parentGraphNode);
            if(predecessorNodeListofParentQueryNode.contains(nextQueryNode)) {
                nextCandidate.setGraphNode(getNextNodeBasedOnPredecessor(currentState, nextQueryNode.getLabel(), nextQueryNode));
                return nextCandidate;
            }
            if(successorNodeListofParentQueryNode.contains(nextQueryNode)) {
                nextCandidate.setGraphNode(getNextNodeBasedOnSuccessor(currentState, nextQueryNode.getLabel(), nextQueryNode));
                return nextCandidate;
            }
        }
        return new Candidate();
    }

    private Candidate getNextCandidateWithAlsoBasedOnProperties(Map<MyNode, Node> currentState, Candidate candidate, Set<MyNode> nodeExplorationSequence, Query queryGraph) {
        Candidate nextCandidate = new Candidate();
        if(candidate.getQueryNode() == null) {
            nextCandidate.setQueryNode(getNextInSequence(nodeExplorationSequence, currentState));
            if(nextCandidate.getQueryNode() == null) {
                return candidate;
            }
        } else {
            nextCandidate.setQueryNode(candidate.getQueryNode());
        }

        MyNode nextQueryNode = nextCandidate.getQueryNode();
        MyNode parentQueryNode = nextQueryNode.getParent();
        if(parentQueryNode == null) {
            nextCandidate.setGraphNode(getNextNodeBasedOnNotConnectedNodesAndProperties(currentState, nextQueryNode.getLabel(), nextQueryNode));
            return nextCandidate;
        } else {
            Node parentGraphNode = currentState.get(parentQueryNode);
            Set<MyNode> predecessorNodeListofParentQueryNode = parentQueryNode.getPredecessorNodeList(queryGraph);
            Set<MyNode> successorNodeListofParentQueryNode = parentQueryNode.getSuccessorNodeList(queryGraph);
            getInAnOutRelationshipsOfDataVertex(parentGraphNode);
            if(predecessorNodeListofParentQueryNode.contains(nextQueryNode)) {
                nextCandidate.setGraphNode(getNextNodeBasedOnPredecessorAndProperties(currentState, nextQueryNode));
                return nextCandidate;
            }
            if(successorNodeListofParentQueryNode.contains(nextQueryNode)) {
                nextCandidate.setGraphNode(getNextNodeBasedOnSuccessorAndProperties(currentState, nextQueryNode));
                return nextCandidate;
            }
        }
        return new Candidate();
    }

    private Node getNextNodeBasedOnNotConnectedNodes(Map<MyNode, Node> currentState, Label label, MyNode nextQueryNode) {
        Set<Node> nodesInGraphDBWithGivenLabel = findNodesInGraphDBWithGivenLabel(label);
        for(Node node : nodesInGraphDBWithGivenLabel) {
            if(!currentState.containsValue(node) && (visitedNodeMap.get(nextQueryNode) == null ? true : !visitedNodeMap.get(nextQueryNode).contains(node))) {
                visitedNodeMap.get(nextQueryNode).add(node);
                return node;
            }
        }
        return null;
    }

    private Node getNextNodeBasedOnNotConnectedNodesAndProperties(Map<MyNode, Node> currentState, Label label, MyNode nextQueryNode) {
        Set<Node> nodesInGraphDBWithGivenLabel = findNodesInGraphDBWithGivenLabelAndProperties(label, nextQueryNode);
        for(Node node : nodesInGraphDBWithGivenLabel) {
            if(!currentState.containsValue(node) && (visitedNodeMap.get(nextQueryNode) == null ? true : !visitedNodeMap.get(nextQueryNode).contains(node))) {
                visitedNodeMap.get(nextQueryNode).add(node);
                return node;
            }
        }
        return null;
    }

    private Node getNextNodeBasedOnPredecessor(Map<MyNode, Node> currentState, Label label, MyNode nextQueryNode) {
        List<Node> predecessorGraphNodeList = getPredecessorGraphNodeList();
        Node nextGraphNode = predecessorGraphNodeList.stream().filter(node -> (Iterables.asList(node.getLabels()).isEmpty() ? false : node.getLabels().iterator().next().toString().equals(label.toString()))
                && !currentState.containsValue(node)
                && (visitedNodeMap.get(nextQueryNode) == null ? true : !visitedNodeMap.get(nextQueryNode).contains(node))).findFirst().orElse(null);
        visitedNodeMap.get(nextQueryNode).add(nextGraphNode);
        return nextGraphNode;
    }

    private Node getNextNodeBasedOnPredecessorAndProperties(Map<MyNode, Node> currentState, MyNode nextQueryNode) {
        List<Node> predecessorGraphNodeListWithLabelAndProperty = getPredecessorGraphNodeListWithLabelAndProperty(nextQueryNode);
        Node nextGraphNode = predecessorGraphNodeListWithLabelAndProperty.stream().filter(node -> !currentState.containsValue(node)
                && (visitedNodeMap.get(nextQueryNode) == null ? true : !visitedNodeMap.get(nextQueryNode).contains(node))).findFirst().orElse(null);
        visitedNodeMap.get(nextQueryNode).add(nextGraphNode);
        return nextGraphNode;
    }

    private List<Node> getPredecessorGraphNodeList() {
        List<Node> predecessorGraphNodeList = new ArrayList<>();
        incomingRelationListOfDataVertex.forEach(relation -> {
            predecessorGraphNodeList.add(relation.getStartNode());
        });
        return predecessorGraphNodeList;
    }

    private List<Node> getPredecessorGraphNodeListWithLabelAndProperty(MyNode nextQueryNode) {
        Label nextQueryNodeLabel = nextQueryNode.getLabels().iterator().next();
        Set<String> propertyKeySet = nextQueryNode.getPropertyMap().keySet();
        List<Node> predecessorGraphNodeList = new ArrayList<>();

        if (!propertyKeySet.isEmpty()) {
            String firstPropertyKey = propertyKeySet.iterator().next();
            String firstPropertyValue = nextQueryNode.getProperty(firstPropertyKey);
            incomingRelationListOfDataVertex.forEach(relation -> {
                Node startNode = relation.getStartNode();
                if((!Iterables.asList(startNode.getLabels()).isEmpty() && startNode.getLabels().iterator().next().toString().equals(nextQueryNodeLabel.toString())) && (startNode.hasProperty(firstPropertyKey) && firstPropertyValue.equals(startNode.getProperty(firstPropertyKey).toString()))) {
                    predecessorGraphNodeList.add(startNode);
                }
            });
        } else {
            incomingRelationListOfDataVertex.forEach(relation -> {
                Node startNode = relation.getStartNode();
                if(!Iterables.asList(startNode.getLabels()).isEmpty() && startNode.getLabels().iterator().next().toString().equals(nextQueryNodeLabel.toString())) {
                    predecessorGraphNodeList.add(startNode);
                }
            });
        }
        return predecessorGraphNodeList;
    }

    private Node getNextNodeBasedOnSuccessor(Map<MyNode, Node> currentState, Label label, MyNode currentQueryNode) {
        List<Node> successorGraphNodeList = getSuccessorGraphNodeList();
        Node nextGraphNode = successorGraphNodeList.stream().filter(node -> (Iterables.asList(node.getLabels()).isEmpty() ? false : node.getLabels().iterator().next().toString().equals(label.toString()))
                && !currentState.containsValue(node)
                && (visitedNodeMap.get(currentQueryNode) == null ? true : !visitedNodeMap.get(currentQueryNode).contains(node))).findFirst().orElse(null);
        visitedNodeMap.get(currentQueryNode).add(nextGraphNode);
        return nextGraphNode;
    }

    private Node getNextNodeBasedOnSuccessorAndProperties(Map<MyNode, Node> currentState, MyNode nextQueryNode) {
        List<Node> successorGraphNodeListWithLabelAndProperty = getSuccessorGraphNodeListWithLabelAndProperty(nextQueryNode);
        Node nextGraphNode = successorGraphNodeListWithLabelAndProperty.stream().filter(node -> !currentState.containsValue(node)
                && (visitedNodeMap.get(nextQueryNode) == null ? true : !visitedNodeMap.get(nextQueryNode).contains(node))).findFirst().orElse(null);
        visitedNodeMap.get(nextQueryNode).add(nextGraphNode);
        return nextGraphNode;
    }

    private List<Node> getSuccessorGraphNodeList() {
        List<Node> successorGraphNodeList = new ArrayList<>();
        outgoingRelationListOfDataVertex.forEach(relation -> {
            successorGraphNodeList.add(relation.getEndNode());
        });
        return successorGraphNodeList;
    }

    private List<Node> getSuccessorGraphNodeListWithLabelAndProperty(MyNode nextQueryNode) {
        Label nextQueryNodeLabel = nextQueryNode.getLabels().iterator().next();
        Set<String> propertyKeySet = nextQueryNode.getPropertyMap().keySet();
        List<Node> successorGraphNodeList = new ArrayList<>();

        if (!propertyKeySet.isEmpty()) {
            String firstPropertyKey = propertyKeySet.iterator().next();
            String firstPropertyValue = nextQueryNode.getProperty(firstPropertyKey);
            outgoingRelationListOfDataVertex.forEach(relation -> {
                Node endNode = relation.getEndNode();
//                if((!Iterables.asList(endNode.getLabels()).isEmpty() && endNode.getLabels().iterator().next().toString().equals(nextQueryNodeLabel.toString())) && (!StringUtils.isNumeric(firstPropertyValue) ? firstPropertyValue.equals(endNode.getProperty(firstPropertyKey)) : firstPropertyValue.equals(endNode.getProperty(firstPropertyKey).toString()))) {
                if((!Iterables.asList(endNode.getLabels()).isEmpty() && endNode.getLabels().iterator().next().toString().equals(nextQueryNodeLabel.toString())) && (endNode.hasProperty(firstPropertyKey) && firstPropertyValue.equals(endNode.getProperty(firstPropertyKey).toString()))) {
                    successorGraphNodeList.add(endNode);
                }
            });
        } else {
            outgoingRelationListOfDataVertex.forEach(relation -> {
                Node endNode = relation.getEndNode();
                if(!Iterables.asList(endNode.getLabels()).isEmpty() && endNode.getLabels().iterator().next().toString().equals(nextQueryNodeLabel.toString())) {
                    successorGraphNodeList.add(endNode);
                }
            });
        }
        return successorGraphNodeList;
    }

    public void getInAnOutRelationshipsOfDataVertex(Node dataVertex) {
        this.incomingRelationListOfDataVertex = new HashSet<>();
        this.outgoingRelationListOfDataVertex = new HashSet<>();
        this.allTypesOfRelationListOfDataVertex = new HashSet<>();

        Iterable<Relationship> relationships = dataVertex.getRelationships();
        relationships.forEach(relationship -> {
            if(!incomingRelationListOfDataVertex.contains(relationship) && relationship.getEndNode().getId() == dataVertex.getId()) {
                incomingRelationListOfDataVertex.add(relationship);
            }
            if(!outgoingRelationListOfDataVertex.contains(relationship) && relationship.getEndNode().getId() != dataVertex.getId()) {
                outgoingRelationListOfDataVertex.add(relationship);
            }
        });
        allTypesOfRelationListOfDataVertex.addAll(incomingRelationListOfDataVertex);
        allTypesOfRelationListOfDataVertex.addAll(outgoingRelationListOfDataVertex);
    }

    private Set<Node> findNodesInGraphDBWithGivenDegree(MyNode queryVertex, Set<Node> allNodesInGraphDB) {
        Set<Node> dataVertexList = new HashSet<>();
        allNodesInGraphDB.forEach(node -> {
            if(queryVertex.getDegree() == node.getDegree()) {
                dataVertexList.add(node);
            }
        });
        return dataVertexList;
    }

    private void clearVisitedNodeMap(MyNode nextQueryNode, Set<MyNode> nodeExplorationSequence) {
        boolean isNodeFound = false;
        List<MyNode> clearNodeList = new ArrayList<>();

        for(MyNode myNode : nodeExplorationSequence) {
            if(isNodeFound) {
                clearNodeList.add(myNode);
            } else {
                if(myNode == nextQueryNode) {
                    isNodeFound = true;
                }
            }
        }
        clearNodeList.forEach(node -> visitedNodeMap.get(node).clear());
    }

    private Set<MyNode> copySet(Set<MyNode> originalSet) {
        Set<MyNode> copySet = new HashSet<>();
        for (MyNode node : originalSet) {
            copySet.add(node);
        }
        return copySet;
    }

    public static Object getKeyFromValue(Map hm, Object value) {
        for (Object o : hm.keySet()) {
            if (hm.get(o).equals(value)) {
                return o;
            }
        }
        return null;
    }

    private void classifyNodes() {
        MyNode firstQueryNode = queryNodes.get(0);
        int id = 0;
        NodeClass firstNodeClass = new NodeClass(id, firstQueryNode.getDegree(), firstQueryNode.getLabel());
        List<NodeClass> nodeClassList = new ArrayList<>();
        nodeClassList.add(firstNodeClass);

        for(MyNode queryNode : queryNodes) {
            NodeClass nodeClass = new NodeClass(queryNode.getDegree(), queryNode.getLabel());
            if(!nodeClassList.contains(nodeClass)) {
                id++;
                NodeClass newNodeClass = new NodeClass(id, queryNode.getDegree(), queryNode.getLabel());
                nodeClassList.add(newNodeClass);
                queryNode.setNodeClass(newNodeClass);
            } else {
                NodeClass exactNodeClass = nodeClassList.stream().filter(nc -> nc.equals(nodeClass)).findFirst().get();
                queryNode.setNodeClass(exactNodeClass);
            }
        }
    }

    private List<Node> findNodesWithExactClass(NodeClass nodeClass) {
        Set<Node> nodesWithLabel = findNodesInGraphDBWithGivenLabel(nodeClass.getLabel());
        List<Node> nodesWithClass = nodesWithLabel.stream().filter(node -> node.getDegree() == nodeClass.getDegree() && node.getLabels().iterator().next() == nodeClass.getLabel()).collect(Collectors.toList());
        return nodesWithClass;
    }

    private double calculateGraphIsoProbabilityToFindFeasibleCandidiateOfTheNode(MyNode queryVertex) {
        Set<Node> allNodesInGraphDB = findAllNodesInGraphDB();
        Set<Node> nodesInGraphDBWithGivenLabel = findNodesInGraphDBWithGivenLabel(queryVertex.getLabel());
        Set<Node> nodesInGraphDBWithGivenDegree = findNodesInGraphDBWithGivenDegree(queryVertex, allNodesInGraphDB);

        double probabilityToFindACandidateNodeWithNodesLabel = nodesInGraphDBWithGivenLabel.size() / allNodesInGraphDB.size();
        double probabilityToFindANodeWithGivenDegree = nodesInGraphDBWithGivenDegree.size() / allNodesInGraphDB.size();
        double probabilityToFindFeasibleCandidateOfTheNode = probabilityToFindACandidateNodeWithNodesLabel * probabilityToFindANodeWithGivenDegree;
        return probabilityToFindFeasibleCandidateOfTheNode;
    }

    private boolean isDead(Map<MyNode, Node> state) {
        for (Map.Entry<MyNode, Node> entry : state.entrySet()) {
            if (entry.getKey() != null && entry.getValue() == null) {
                return true;
            }
        }
        return false;
    }

        /*
    private MyNode getNextInSequence(Set<MyNode> nodeExplorationSequence, MyNode queryNode) {
        MyNode nextNode = new MyNode();
        boolean exactNodePassed = false;
        if(queryNode == null) {
            return nodeExplorationSequence.iterator().next();
        }
        for(MyNode node : nodeExplorationSequence) {
            if(exactNodePassed) {
                nextNode = node;
                break;
            }
            if(node.equals(queryNode)) {
                exactNodePassed = true;
            }
        }
        return nextNode;
    }*/

//    private MyNode getNextInSequence(Set<MyNode> nodeExplorationSequence) {
//        MyNode nextNode = new MyNode();
//        for(MyNode node : nodeExplorationSequence) {
//            if(node.getVisited() == false) {
//                nextNode = node;
//                nextNode.setVisited(true);
//                break;
//            }
//        }
//        return nextNode;
//    }

}
