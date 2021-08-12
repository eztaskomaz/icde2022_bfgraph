import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Path("/MstGraph")
public class MstGraph {

    private final GraphDatabaseService database;
    private Query queryGraph;
    private FilterCandidatesForBBPlus filteringTool;
    private DetermineMatchOrder orderingTool;
    private Print printingTool = new Print();
    private MeasureSourceConsumption measuringTool = new MeasureSourceConsumption();
    private Iterable<RelationshipType> allRelationTypes;
    private List<MyNode> queryNodes = new ArrayList<MyNode>();
    private Map<Integer, Long> matchedCoupleNodeIds = new HashMap<Integer, Long>();
    private Map<Integer, Long> matchedCoupleRelationIds = new HashMap<Integer, Long>();
    private Stack<Integer> notCheckedQueryNodeIds = new Stack<Integer>();
    private List<Pair<List<Long>, List<Long>>> matchedSubgraphs= new ArrayList<Pair<List<Long>, List<Long>>>();
    private List<Pair<List<Long>, List<Long>>> secondaryMatchedSubgraphs= new ArrayList<Pair<List<Long>, List<Long>>>();
    private List<MyNode> orderedQueryNodeList = new ArrayList<>();
    //private Map<Integer, Integer> notCheckedQueryNodeOrderAndIds = new TreeMap<Integer, Integer>();
    private int order = 0;
    private List<MyRelation> relationsThatAreNotInMST = new ArrayList<>();

    public MstGraph(@Context GraphDatabaseService database) {
        this.database = database;
        try (Transaction tx = database.beginTx()) {
            allRelationTypes = database.getAllRelationshipTypes();
            tx.success();
            tx.close();
        } catch (Exception e) {
            e.getMessage();
        }
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/{query}")
    public void executeQuery(@PathParam("query") String query) {
        MeasureSourceConsumption msc = new MeasureSourceConsumption();
        msc.startCalculateTimeAndCpuConsumption();

        queryGraph = new Query(queryNodes, query, allRelationTypes);
        queryGraph.extractQueryItems();
        filteringTool = new FilterCandidatesForBBPlus(database, queryGraph, queryNodes);

        MyNode rootNode = orderQueryNodeListAndGetFirstOne();
        findMinimumSpanningTreeWithBFS(rootNode);
        findRootCandidates(rootNode);

        msc.endCalculateTimeAndCpuConsumption();
        msc.printConsumptions(secondaryMatchedSubgraphs.size());
    }

    private MyNode orderQueryNodeListAndGetFirstOne() {
        orderedQueryNodeList = orderQueryNodesByCandidateNodeSize();
        return orderedQueryNodeList.get(0);
    }

    private void findRootCandidates(MyNode rootNode) {
        List<Node> rootCandidatesList  = new ArrayList<>();
        filteringTool.filterCandidatesByLabelAndProperty(rootNode, rootCandidatesList);
        filteringTool.filterCandidatesByRelationships(rootNode, rootCandidatesList, Direction.OUTGOING);
        filteringTool.filterCandidatesByRelationships(rootNode, rootCandidatesList, Direction.INCOMING);

        rootCandidatesList.forEach(rootCandidate -> {
            matchedCoupleNodeIds.put(rootNode.getId(), rootCandidate.getId());
            startFromRoot(rootNode, rootCandidate);
            matchedCoupleNodeIds.remove(rootNode.getId());
        });

        secondaryMatchedSubgraphs.addAll(matchedSubgraphs);

        relationsThatAreNotInMST.forEach(relation -> {
            int startNodeId = relation.getStartNodeId();
            MyNode startNode = queryGraph.getNodeById(startNodeId);
            int endNodeId = relation.getEndNodeId();
            MyNode endNode = queryGraph.getNodeById(endNodeId);

            matchedSubgraphs.forEach(matchedSubgraph -> {
                Long startGraphNodeId = matchedSubgraph.first().get(startNode.getOrder());
                Long endGraphNodeId = matchedSubgraph.first().get(endNode.getOrder());

                Node startGraphNode = database.getNodeById(startGraphNodeId);
                Node endGraphNode = database.getNodeById(endGraphNodeId);


                Iterable<Relationship> relationships = startGraphNode.getRelationships(relation.getType());
                relationships.forEach(relationx -> {
                    Node otherNode = relationx.getOtherNode(startGraphNode);
                    if(otherNode != endGraphNode) {
                        secondaryMatchedSubgraphs.remove(matchedSubgraph);
                    }
                });
            });
        });
    }

    public List<MyNode> orderQueryNodesByCandidateNodeSize() {
        List<MyNode> orderedQueryNodes = new ArrayList<>();
        filteringTool = new FilterCandidatesForBBPlus(database, queryGraph, queryNodes);

        queryNodes.forEach(queryNode -> {
            List<Node> rootCandidatesList  = new ArrayList<>();
            filteringTool.filterCandidatesByLabelAndProperty(queryNode, rootCandidatesList);
            filteringTool.filterCandidatesByRelationships(queryNode, rootCandidatesList, Direction.OUTGOING);
            filteringTool.filterCandidatesByRelationships(queryNode, rootCandidatesList, Direction.INCOMING);
            queryNode.setCandidateNodeSize(rootCandidatesList.size());
            orderedQueryNodes.add(queryNode);
        });
        Collections.sort(orderedQueryNodes, (t1, t2) -> t1.getCandidateNodeSize().compareTo(t2.getCandidateNodeSize()));
        orderedQueryNodes.forEach(node -> {
            node.setOrder(generateOrder());
        });
        return orderedQueryNodes;
    }

    private Integer generateOrder() {
        return order++;
    }

    public MyNode findRootQueryNode() {
        calculateSubGraphIsoProbabilityToFindFeasibleCandidiateForAllNodes();
        MyNode minPFeasibleAndMaxDegree = findMinPFeasibleAndMaxDegree();
        return minPFeasibleAndMaxDegree;
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

    private Set<Node> findNodesInGraphDBWithGivenLabel(Label label) {
        Set<Node> dataVertexList = new HashSet<>();
        ResourceIterator<Node> nodeList = database.findNodes(label);
        nodeList.forEachRemaining(dataVertexList::add);
        return dataVertexList;
    }

    private Set<Node> findAllNodesInGraphDB() {
        Set<Node> dataVertexList = new HashSet<>();
        ResourceIterable<Node> allNodes = database.getAllNodes();
        for(Node node : allNodes) {
            dataVertexList.add(node);
        }
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

    public boolean startFromRoot(MyNode rootNode, Node rootCandidate) {
        List<Integer> neighborIds = new ArrayList<Integer>();
        List<Integer> relationIdsSet = new ArrayList<Integer>();
        List<List<Relationship>> candidateRelationsSet = new ArrayList<List<Relationship>>();

        Map<Integer, Integer> neighborIdByOrderMap = new TreeMap<>();
        Map<Integer, Integer> relationIdByOrderMap = new TreeMap<>();
        Map<Integer, List<Relationship>> candidateRelationByOrderMap = new TreeMap<>();

        boolean isSuccessful = prepareCandidateRelations(Direction.OUTGOING, rootNode, rootCandidate, neighborIdByOrderMap, relationIdByOrderMap, candidateRelationByOrderMap);
        if (isSuccessful == false) return false;

        isSuccessful = prepareCandidateRelations(Direction.INCOMING, rootNode, rootCandidate, neighborIdByOrderMap, relationIdByOrderMap, candidateRelationByOrderMap);
        if (isSuccessful == false) return false;

        if(candidateRelationByOrderMap.isEmpty()) return true;

        neighborIds.addAll(neighborIdByOrderMap.values());
        relationIdsSet.addAll(relationIdByOrderMap.values());
        candidateRelationsSet.addAll(candidateRelationByOrderMap.values());


        checkEachPermutation(relationIdsSet, candidateRelationsSet, 0, rootCandidate, neighborIds);
        notCheckedQueryNodeIds.clear();
        return false;
    }

    public boolean prepareCandidateRelations(Direction direction, MyNode rootNode, Node rootCandidate, Map<Integer, Integer> neighborIdByOrderMap, Map<Integer, Integer> relationIdByOrderMap, Map<Integer, List<Relationship>> candidateRelationByOrderMap) {
        int rootNodeId = rootNode.getId();
        for(MyRelation relation : rootNode.getRelations(direction)) {
            if(relation.isPartOfMst()) {
                if(matchedCoupleRelationIds.containsKey(relation.getId())) continue;
                Iterable<Relationship> candidateRelations = rootCandidate.getRelationships(relation.getType(), direction);
                if(candidateRelations == null) return false;
                List<Relationship> candidateRelationList = filteringTool.checkRelationProperties(relation, candidateRelations);
                if(candidateRelationList.isEmpty()) return false;
                int neighborId = relation.getTheOtherNodeId(rootNodeId);
                MyNode neighborNode = queryGraph.getNodeById(neighborId);
                candidateRelationByOrderMap.put(neighborNode.getOrder(), candidateRelationList);
                relationIdByOrderMap.put(neighborNode.getOrder(), relation.getId());
                neighborIdByOrderMap.put(neighborNode.getOrder(), neighborId);
            } else {
                relationsThatAreNotInMST.add(relation);
            }
        }
        return true;
    }

    private void findMinimumSpanningTreeWithBFS(MyNode rootNode) {
        List<MyNode> bfsTreeNodeList = new ArrayList<>();
        bfsTreeNodeList.add(rootNode);

        while(!bfsTreeNodeList.isEmpty()) {
            MyNode nextNode = bfsTreeNodeList.get(0);
            List<MyRelation> allRelations = nextNode.getAllRelations();
            for(MyRelation relation : allRelations) {
                MyNode neighborNode = queryNodes.get(relation.getTheOtherNodeId(nextNode.getId()));
                if(neighborNode.isPartOfMst()) {
                    continue;
                } else {
                    neighborNode.setPartOfMst(true);
                    relation.setPartOfMst(true);
                    bfsTreeNodeList.add(neighborNode);
                }
            }
            bfsTreeNodeList.remove(nextNode);
        }
    }

    public void checkEachPermutation(List<Integer> relationIdsSet, List<List<Relationship>> candidateRelationsSet, int index, Node rootCandidate, List<Integer> neighborIds) {
        int neighborId = neighborIds.get(index);
        MyNode neighborNode = queryNodes.get(neighborId);
        Set<String> neighborNodePropertyKeys = neighborNode.getPropertyMap().keySet();
        int relationId = relationIdsSet.get(index);
        List<Relationship> candidateRelations = candidateRelationsSet.get(index);

        for(Relationship candidateRelation : candidateRelations) {
            boolean isNodeDiscovered = false;
            Node neighborCandidate = candidateRelation.getOtherNode(rootCandidate);
            if (matchedCoupleNodeIds.containsKey(neighborId) && matchedCoupleNodeIds.get(neighborId) != neighborCandidate.getId()) {
                continue;
            } else if (matchedCoupleNodeIds.containsValue(neighborCandidate.getId()) ) {
                continue;
            } else if (filteringTool.checkRestOfNodeProperties(neighborCandidate, neighborNode, neighborNodePropertyKeys.iterator(), false) == false) {
                continue;
            } else if (filteringTool.checkRelationshipCount(neighborNode, neighborCandidate, Direction.OUTGOING) == false) {
                continue;
            } else if (filteringTool.checkRelationshipCount(neighborNode, neighborCandidate, Direction.INCOMING) == false) {
                continue;
            } else {
                matchedCoupleNodeIds.put(neighborId, neighborCandidate.getId());
                notCheckedQueryNodeIds.push(neighborId);
                //notCheckedQueryNodeOrderAndIds.put(neighborOrder, neighborId);
                isNodeDiscovered = true;
            }
            matchedCoupleRelationIds.put(relationId, candidateRelation.getId());
            if (index+1 == neighborIds.size()) {
                checkOtherMatches();
            } else {
                checkEachPermutation(relationIdsSet, candidateRelationsSet, index+1, rootCandidate, neighborIds);
            }

            // Remove the lastly added relationship and node to backtrack one step
            matchedCoupleRelationIds.remove(relationId);
            if (isNodeDiscovered) {
                matchedCoupleNodeIds.remove(neighborId);
                notCheckedQueryNodeIds.pop();
                //notCheckedQueryNodeOrderAndIds.remove(neighborOrder);
            }
        }
    }

    public void checkOtherMatches() {

        Stack<Integer> tempNotCheckedQueryNodeIds = new Stack<Integer>();
        for (int i = 0; i < notCheckedQueryNodeIds.size(); i++) {
            tempNotCheckedQueryNodeIds.push(notCheckedQueryNodeIds.get(i));
        }
        boolean shouldContinue = true;
        while (notCheckedQueryNodeIds.isEmpty() == false) {
            int queryNodeId = notCheckedQueryNodeIds.pop();
            MyNode queryNode = queryNodes.get(queryNodeId);
            Long dbNodeId = matchedCoupleNodeIds.get(queryNodeId);
            Node dbNode = database.getNodeById(dbNodeId);
            shouldContinue = startFromRoot(queryNode, dbNode);
            if (shouldContinue == false) {
                break;
            }
        }
        savePermutation(shouldContinue, tempNotCheckedQueryNodeIds);
    }

    public void savePermutation(boolean isSuccessful, Stack<Integer> tempNotCheckedQueryNodeIds) {
        if (isSuccessful) {
            List<Long> dbNodeIds = new ArrayList<Long>();
            for (Iterator<Long> i = matchedCoupleNodeIds.values().iterator(); i.hasNext(); )
                dbNodeIds.add(i.next());
            List<Long> dbRelationIds = new ArrayList<Long>();
            for (Iterator<Long> i = matchedCoupleRelationIds.values().iterator(); i.hasNext(); )
                dbRelationIds.add(i.next());
            Pair<List<Long>, List<Long>> matchedInstance = Pair.of(dbNodeIds, dbRelationIds);
            matchedSubgraphs.add(matchedInstance);
        }
        notCheckedQueryNodeIds.clear();
        notCheckedQueryNodeIds = tempNotCheckedQueryNodeIds;
    }

}
