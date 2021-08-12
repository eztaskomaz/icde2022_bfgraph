import org.neo4j.graphdb.*;

import java.util.*;
import java.util.stream.Collectors;


public class TurboMethod {

    private final GraphDatabaseService database;
    private Iterable<RelationshipType> allRelationTypes;
    private List<MyNode> queryNodes = new ArrayList<MyNode>();

    public TurboMethod(GraphDatabaseService database) {
        this.database = database;
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

        Iterable<RelationshipType> allRelationTypes = database.getAllRelationshipTypes();

        Query queryGraph = new Query(queryNodes, query, allRelationTypes);
        queryGraph.extractQueryItems();

        StartNodeSelector sns = new StartNodeSelector();
        MyNode startNode = sns.chooseStartNode(queryGraph, database);

        NecTree necTree = new NecTree(startNode);
        NecNode rootOfNecTree = necTree.RewriteToNECTree(queryGraph);

        List<Node> dataVertexList = new ArrayList<>();
        ResourceIterator<Node> nodeList = database.findNodes(startNode.getLabel());
        nodeList.forEachRemaining(dataVertexList::add);

        //List<Node> dataVertexList = (List<Node>) database.findNodes(startNode.getLabel());
        List<Map<MyNode, List<Node>>> allSolutionMap = new ArrayList<>();

        for (Node startDataVertex : dataVertexList) {
            Node parentOfStartDataVertex = database.createNode();
            List<Node> visitedDataVertices = new ArrayList<>();
            List<Node> unvisitedDataVertices = new ArrayList<>();
            unvisitedDataVertices.add(startDataVertex);
            CandidateRegion candidateRegion = new CandidateRegion(rootOfNecTree, parentOfStartDataVertex);
            List<CandidateRegion> candidateRegionList = new ArrayList<>();

            if (!exploreCR(rootOfNecTree, Arrays.asList(startDataVertex), queryGraph, visitedDataVertices, unvisitedDataVertices, candidateRegion, candidateRegionList)) {
                continue;
            }

            int order = 0;
            List<NecNode> orderedNecNodeList = determineMatchingOrder(candidateRegionList, necTree);
            Map<NecNode, List<Combination>> matchedCombinationMap = new HashMap<>();

            Map<MyNode, List<Node>> solutionMap = new HashMap<>();
            Map<MyNode, List<Node>> solutionMap1 = subGraphSearch(orderedNecNodeList, order, candidateRegionList, queryGraph, matchedCombinationMap, solutionMap);
            allSolutionMap.add(solutionMap1);
            parentOfStartDataVertex.delete();
        }

        msc.endCalculateTimeAndCpuConsumption();
        msc.printConsumptions(allSolutionMap.size());
    }

    public Map<MyNode, List<Node>> subGraphSearch(List<NecNode> orderedNecNodeList, int order, List<CandidateRegion> candidateRegionList, Query queryGraph, Map<NecNode, List<Combination>> matchedCombinationMap, Map<MyNode, List<Node>> solutionMap) {
        if (order < orderedNecNodeList.size()) {
            NecNode necNode = orderedNecNodeList.get(order);
            Set<Node> candidateNodeListForNecNode = findCandidateDataVertexListForNecNode(candidateRegionList, necNode);

            List<Combination> combinationList = new ArrayList<>();
            findAllCombinationsForTheNecVertex(new ArrayList<>(candidateNodeListForNecNode), necNode.getNECMembers().size(), 0, combinationList);

            List<Node> alreadyMappedNodes = new ArrayList<>();
            List<MyNode> alreadyMappedQueryNodes = new ArrayList<>();
            Map<List<MyNode>, List<Node>> alreadyMappedDataAndQueryNodes = new HashMap<>();
            for(int combinationSize = 0; combinationSize < combinationList.size(); combinationSize++) {
                Combination nextCombination = combinationList.get(combinationSize);
                boolean fValue = calculateFValue(nextCombination, alreadyMappedNodes);
                if (fValue == true) {
                    continue;
                }
                /* Burası kontrol edilmeli
                if (doBothQueryAndDataVerticesFormClique(necNode, nextCombination.getDataVertexList(), queryGraph)) {
                    continue;
                }*/
                boolean matched = true;
                for (int i = 0; i < necNode.getNECMembers().size(); i++) {
                    if (!isJoinable(necNode.getNECMembers().get(i), nextCombination.getDataVertexList().get(i), matchedCombinationMap, queryGraph)) {
                        matched = false;
                        break;
                    }
                }
                if (matched == false) {
                    continue;
                }

                updateState(matchedCombinationMap, necNode, nextCombination, alreadyMappedQueryNodes, alreadyMappedNodes, alreadyMappedDataAndQueryNodes);
                if ((order+1) == orderedNecNodeList.size()) {
                    //burada solution ı parametre olarak verip içini dolduralım ve dışarda return edelim
                    generatePermutation(matchedCombinationMap, orderedNecNodeList, 0, solutionMap);
                } else {
                    subGraphSearch(orderedNecNodeList, (order + 1), candidateRegionList, queryGraph, matchedCombinationMap, solutionMap);
                }
            }
        }

        return solutionMap;
    }


    // by calling ISJOINABLE, we check if, for each query vertex u in u.NEC, the edges between u and already matched query vertices of q
    // have corresponding edges between V[i] and already matched data vertices of g
    private boolean isJoinable(MyNode necMember, Node dataVertex, Map<NecNode, List<Combination>> matchedCombinationMap, Query queryGraph) {
        boolean isPossible = false;

        boolean isJoinable = true;
        //matchedCombinationMap deki necmemberlar ve combinatıonları gezilerek gelen necMemmber ve datavertex joinable mı diye bakılacak
        for (Map.Entry<NecNode, List<Combination>> map : matchedCombinationMap.entrySet()) {
            for (int i = 0; i < map.getKey().getNECMembers().size(); i++) {
                MyNode mappedQueryVertex = map.getKey().getNECMembers().get(i);
                MyRelation relation = queryGraph.getRelationByNodeIds(necMember.getId(), mappedQueryVertex.getId());
                for (Combination combination : map.getValue()) {
                    Node mappedDataVertex = combination.getDataVertexList().get(i);
                    if (relation != null) {
                        isPossible = isRelationMatchable(relation, necMember.getId(), mappedQueryVertex.getId(), mappedDataVertex.getId(), dataVertex);
                        if (isPossible) {
                            isJoinable = true;
                        }
                    }
                }
            }
        }
        return isJoinable;
    }

    public boolean isRelationMatchable(MyRelation relation, int u_id, int matched_u_id, Long matched_v_id, Node v) {
        boolean exists = false;
        try (Transaction tx = database.beginTx()) {
            int degree_of_v = v.getDegree(relation.getType(), relation.getDirection(u_id));
            int degree_of_matched_v = database.getNodeById(matched_v_id).getDegree(relation.getType(), relation.getDirection(matched_u_id));

            if (degree_of_v <= degree_of_matched_v) {
                Node dbNode = v;
                Direction direction = relation.getDirection(u_id);
                Long otherDbNodeId = matched_v_id;
                exists = doesRelationExist(dbNode, otherDbNodeId, relation.getType(), direction);
            } else {
                Node dbNode = database.getNodeById(matched_v_id);
                Direction direction = relation.getDirection(matched_u_id);
                Long otherDbNodeId = v.getId();
                exists = doesRelationExist(dbNode, otherDbNodeId, relation.getType(), direction);
            }

            tx.success();
            tx.close();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
        return exists;
    }

    public boolean doesRelationExist(Node dbNode, Long otherDbNodeId, RelationshipType type, Direction direction) {
        for (Iterator<Relationship> r = dbNode.getRelationships(type, direction).iterator(); r.hasNext(); ) {
            Relationship dbRelation = r.next();
            if (dbRelation.getOtherNode(dbNode).getId() == otherDbNodeId) {
                return true;
            }
        }
        return false;
    }

    private void updateState(Map<NecNode, List<Combination>> matchedCombinationMap, NecNode necNode, Combination combination, List<MyNode> alreadyMappedQueryNodes, List<Node> alreadyMappedDataNodes, Map<List<MyNode>, List<Node>> alreadyMappedDataAndQueryNodes) {
        if (matchedCombinationMap.containsKey(necNode)) {
            matchedCombinationMap.get(necNode).add(combination);
        } else {
            matchedCombinationMap.put(necNode, Arrays.asList(combination));
        }
        alreadyMappedDataNodes.addAll(combination.getDataVertexList());
        alreadyMappedQueryNodes.addAll(necNode.getNECMembers());
        alreadyMappedDataAndQueryNodes.put(alreadyMappedQueryNodes, alreadyMappedDataNodes);
    }

    private boolean doBothQueryAndDataVerticesFormClique(NecNode necNode, List<Node> dataVertexList, Query queryGraph) {
        boolean queryVerticesFormClique = true;
        boolean dataVerticesFormClique = true;

        for (int i = 0; i < necNode.getNECMembers().size(); i++) {
            MyNode queryNode = necNode.getNECMembers().get(i);
            for (int j = i + 1; j < necNode.getNECMembers().size(); j++) {
                MyNode queryNode2 = necNode.getNECMembers().get(j);
                if (!doTheyFormClique(queryNode, queryNode2, queryGraph)) {
                    queryVerticesFormClique = false;
                }
            }
        }

        if (queryVerticesFormClique) {
            for (int i = 0; i < dataVertexList.size(); i++) {
                Node node1 = dataVertexList.get(i);
                for (int j = i + 1; j < dataVertexList.size(); j++) {
                    Node node2 = dataVertexList.get(j);
                    if (!doTheyFormClique(node1, node2)) {
                        dataVerticesFormClique = false;
                    }
                }
            }
        } else {
            return true;
        }

        if (dataVerticesFormClique) {
            return true;
        } else {
            return false;
        }
    }

    public boolean doTheyFormClique(MyNode node1, MyNode node2, Query queryGraph) {
        MyRelation relation1 = queryGraph.getRelationByNodeIds(node1.getId(), node2.getId());
        MyRelation relation2 = queryGraph.getRelationByNodeIds(node2.getId(), node1.getId());
        if (relation1 == null || relation2 == null)
            return false;
        else if (relation1.getType() == relation2.getType())
            return true;
        else
            return false;
    }

    public boolean doTheyFormClique(Node node1, Node node2) {
        MyRelation myRelation1 = new MyRelation();
        MyRelation myRelation2 = new MyRelation();

        node1.getRelationships().forEach(relationship -> {
            if (relationship.getEndNode() == node2) {
                myRelation1.createMyRelation(relationship.getType(), (int) node1.getId(), (int) node2.getId(), (int) relationship.getId());
            }
        });
        node2.getRelationships().forEach(relationship -> {
            if (relationship.getEndNode() == node1) {
                myRelation2.createMyRelation(relationship.getType(), (int) node2.getId(), (int) node1.getId(), (int) relationship.getId());
            }
        });
        if (myRelation1 == null || myRelation2 == null) {
            return false;
        } else if (myRelation1.getType() == myRelation2.getType()) {
            return true;
        } else {
            return false;
        }
    }

    public boolean calculateFValue(Combination combination, List<Node> alreadyMappedNodes) {
        boolean fValue = false;
        for (Node dataVertex : combination.getDataVertexList()) {
            if (alreadyMappedNodes.contains(dataVertex)) {
                fValue = true;
            }
        }
        return fValue;
    }

//    static void combinations2(String[] arr, int len, int startPosition, String[] result){
//        if (len == 0){
//            System.out.println(Arrays.toString(result));
//            return;
//        }
//        for (int i = startPosition; i <= arr.length-len; i++){
//            result[result.length - len] = arr[i];
//            combinations2(arr, len-1, i+1, result);
//        }
//    }

    public void findAllCombinationsForTheNecVertex(List<Node> candidateDataVertexList, int necMemberSize, int startPosition, List<Combination> combinationList) {
        if (necMemberSize != 0) {
            Combination combination = new Combination();
            for (int i = startPosition; i <=candidateDataVertexList.size() - necMemberSize; i++) {
                combination.getDataVertexList().add(candidateDataVertexList.get(i));
                combinationList.add(combination);
                findAllCombinationsForTheNecVertex(candidateDataVertexList, necMemberSize - 1, i + 1, combinationList);
            }
        }
    }

//    public Combination nextCombination(NecNode necNode, Set<Node> candidateDataVertexList, List<Combination> combinationList) {
//        Combination combination = new Combination();
//        for(MyNode necMember:necNode.getNECMembers()) {
//            for(Node candidateDataVertex:candidateDataVertexList) {
//                Combination newCombination = new Combination();
//                c
//                if(combinationList.contains(combination))
//                    continue;
//                else
//                    combination = newCombination;
//            }
//        }
//        return combinationList;
//    }

    public Set<Node> findCandidateDataVertexListForNecNode(List<CandidateRegion> candidateRegionList, NecNode necNode) {
        List<CandidateRegion> candidateRegionListByNecNode = findCandidateRegionWithQueryVertex(candidateRegionList, necNode);
        Set<Node> candidateNodeListForNecNode = new HashSet<Node>(candidateRegionListByNecNode.iterator().next().getNodesInCandidateRegion());
        candidateRegionListByNecNode.forEach(candidateRegionByNecNode -> candidateNodeListForNecNode.retainAll(candidateRegionByNecNode.getNodesInCandidateRegion()));
        return candidateNodeListForNecNode;
    }

    public List<Node> findCandidateVertexList(List<CandidateRegion> candidateRegionList, NecNode queryVertex) {
        List<Node> candidateVertexList = new ArrayList<>();
        List<CandidateRegion> candidateRegionListOfQueryVertex = findCandidateRegionWithQueryVertex(candidateRegionList, queryVertex);
        candidateRegionListOfQueryVertex.forEach(candidateRegion -> {
            candidateVertexList.addAll(candidateRegion.getNodesInCandidateRegion());
        });
        return candidateVertexList;
    }

    public List<CandidateRegion> findCandidateRegionWithQueryVertex(List<CandidateRegion> candidateRegionList, NecNode queryVertex) {
        List<CandidateRegion> exactCandidateRegion = new ArrayList<>();
        for (CandidateRegion candidateRegion : candidateRegionList) {
            if (candidateRegion.getQueryVertex() == queryVertex) {
                exactCandidateRegion.add(candidateRegion);
            }
        }
        return exactCandidateRegion;
    }


//    public void generatePermutation(Map<NecNode, Node> matchedCombinationMap, NecTree necTree, int necNodeindex) {
//        List<NecNode> necNodeList = necTree.getNecNodeList();
//        if((necNodeindex + 1) == necNodeList.size()) {
//            return;
//        }
//        NecNode necNode = necNodeList.get(necNodeindex);
//        if(necNode.getNECMembers().size() == 1) {
//            generatePermutation(matchedCombinationMap, necTree, (necNodeindex + 1));
//        } else {
//            while(nextPermutation(matchedCombinationMap, necNode) != true) {
//                generatePermutation(matchedCombinationMap, necTree, (necNodeindex + 1));
//            }
//        }
//    }

    public void generatePermutation(Map<NecNode, List<Combination>> matchedCombinationMap, List<NecNode> orderedNecNodeList, int necNodeindex, Map<MyNode, List<Node>> solutionMap) {
        // Index out of bound exception
        if (necNodeindex == orderedNecNodeList.size()) {
            return;
        }
        NecNode necNode = orderedNecNodeList.get(necNodeindex);
        if (necNode.getNECMembers().size() == 1) {
            // Edit: solutionmap e ekleme
            solutionMap.put(necNode.getNECMembers().iterator().next(), matchedCombinationMap.get(necNode).iterator().next().getDataVertexList());
            generatePermutation(matchedCombinationMap, orderedNecNodeList, (necNodeindex + 1), solutionMap);
        } else {
            while (!nextPermutation(matchedCombinationMap, necNode, solutionMap)) {
                generatePermutation(matchedCombinationMap, orderedNecNodeList, (necNodeindex + 1), solutionMap);
            }
        }
    }

    public boolean nextPermutation(Map<NecNode, List<Combination>> matchedCombinationMap, NecNode necNode, Map<MyNode, List<Node>> solutionMap) {
        boolean isDone = false;
        List<Combination> combinationList = matchedCombinationMap.get(necNode);

        for (Combination combination : combinationList) {
            for (int i = 0; i < necNode.getNECMembers().size(); i++) {
                solutionMap.put(necNode.getNECMembers().get(i), combination.getDataVertexList());
                rotate(new ArrayList<>(combination.getDataVertexList()), 1);
            }
        }
        return true;
    }

    public static <T> ArrayList<T> rotate(ArrayList<T> aL, int shift) {
        if (aL.size() == 0)
            return aL;

        T element = null;
        for (int i = 0; i < shift; i++) {
            // remove last element, add it to front of the ArrayList
            element = aL.remove(aL.size() - 1);
            aL.add(0, element);
        }

        return aL;
    }

    public List<NecNode> determineMatchingOrder(List<CandidateRegion> candidateRegionList, NecTree necTree) {
        List<NecNode> necNodeList = necTree.getNecNodeList();

        for (NecNode necNode : necNodeList) {
            List<CandidateRegion> candidateRegionListByNecNode = findCandidateRegionWithQueryVertex(candidateRegionList, necNode);
            Double necNodeOrder = 0.0;
            for (CandidateRegion candidateRegion : candidateRegionListByNecNode) {
                Double order = Double.valueOf(candidateRegion.getNodesInCandidateRegion().size() / candidateRegion.getQueryVertex().getNECMembers().size());
                necNodeOrder += order;
            }
            necNode.setOrder(necNodeOrder);
        }

        Collections.sort(necNodeList, (t1, t2) -> t1.getOrder().compareTo(t2.getOrder()));
        return necNodeList;
    }

    public boolean exploreCR(NecNode queryVertex, List<Node> candidateDataVertexList, Query queryGraph, List<Node> visitedNodes, List<Node> unvisitedNodes, CandidateRegion candidateRegion, List<CandidateRegion> candidateRegionList) {
        unvisitedNodes.addAll(candidateDataVertexList);
        candidateRegionList.add(candidateRegion);

        for (Node candidateDataVertex : candidateDataVertexList) {
            MyNode firstQueryVertex = queryVertex.getNECMembers().get(0);
            if ((firstQueryVertex.getDegree() > candidateDataVertex.getDegree()) || !applyNlfFilter(firstQueryVertex, candidateDataVertex, queryGraph)) {
                continue;
            }
            visitedNodes.add(candidateDataVertex);
            unvisitedNodes.remove(candidateDataVertex);
            candidateRegion.setMatched(true);

            //childrenOfQueryVertex size ının 0 olduğu durumları da düşünmeliyiz
            List<NecNode> childrenOfQueryVertex = queryVertex.getChildren();
            if(childrenOfQueryVertex.size() != 0) {
                //Unique vertexleri getirmeli
                List<Node> adjacentDataVertices = getDataVertexNeighbors(candidateDataVertex);
                Map<Integer, NecNode> sortedMap = sortByTheNumberOfAdjacentDataVerticesThatHasSameLabel(childrenOfQueryVertex, adjacentDataVertices);
                for (Map.Entry<Integer, NecNode> entry : sortedMap.entrySet()) {
                    NecNode childNecNode = entry.getValue();
                    CandidateRegion childCandidateRegion = new CandidateRegion(childNecNode, candidateDataVertex);
                    List<Node> childDataVertexListWithLabel = getDataVertexNeighborsWithLabel(adjacentDataVertices, childNecNode.getNECMembers().get(0).getLabel());
                    if (!exploreCR(childNecNode, childDataVertexListWithLabel, queryGraph, visitedNodes, unvisitedNodes, childCandidateRegion, candidateRegionList)) {
                        for (NecNode grandChildNode : childNecNode.getChildren()) {
                            candidateRegionList.removeAll(findCandidateRegionWithQueryAndDataVertex(candidateRegionList, grandChildNode, childDataVertexListWithLabel));
                        }
                        candidateRegion.setMatched(false);
                        break;
                    }
                }
            }
            unvisitedNodes.add(candidateDataVertex);
            visitedNodes.remove(candidateDataVertex);
            if (!candidateRegion.isMatched()) {
                continue;
            }
            candidateRegion.getNodesInCandidateRegion().add(candidateDataVertex);
        }
        if (candidateRegion.getNodesInCandidateRegion().size() < queryVertex.getNECMembers().size()) {
            candidateRegionList.remove(candidateRegion);
            return false;
        }
        return true;
    }

    public Map<Integer, NecNode> sortByTheNumberOfAdjacentDataVerticesThatHasSameLabel(List<NecNode> childrenOfQueryVertex, List<Node> adjacentDataVertices) {
        Map<Integer, NecNode> sortedNecNodeMap = new TreeMap<>();
        for (NecNode necNode : childrenOfQueryVertex) {
            int numberOfDataVerticesWithSameLabel = getDataVertexNeighborsWithLabel(adjacentDataVertices, necNode.getNECMembers().get(0).getLabel()).size();
            sortedNecNodeMap.put(numberOfDataVerticesWithSameLabel, necNode);
        }
        return sortedNecNodeMap;
    }

    public List<CandidateRegion> findCandidateRegionWithQueryAndDataVertex(List<CandidateRegion> candidateRegionList, NecNode queryVertex, List<Node> dataVertexList) {
        List<CandidateRegion> exactCandidateRegionList = new ArrayList<>();
        for (CandidateRegion candidateRegion : candidateRegionList) {
            for (Node dataVertex : dataVertexList) {
                if (candidateRegion.getQueryVertex() == queryVertex && candidateRegion.getDataVertex() == dataVertex) {
                    exactCandidateRegionList.add(candidateRegion);
                    ;
                }
            }
        }
        return exactCandidateRegionList;
    }

    public List<MyNode> getQueryVertexNeighbors(MyNode queryVertex, Query queryGraph) {
        List<Integer> neighborsIdList = queryVertex.getNeighborsIdList();
        List<MyNode> neighborsList = new ArrayList<>();
        neighborsIdList.forEach(neighborsId -> neighborsList.add(queryGraph.getNodeById(neighborsId)));
        return neighborsList;
    }

    public Map<String, Integer> getQueryVertexNeighborsLabelCount(MyNode queryVertex, Query queryGraph) {
        List<Label> neighborsLabelList = getQueryVertexNeighbors(queryVertex, queryGraph).stream().map(neighbor -> neighbor.getLabel()).collect(Collectors.toList());

        Map<String, Integer> neighborLabelCountMap = new HashMap<String, Integer>();
        neighborsLabelList.forEach(neighborLabel -> {
            if (!neighborLabelCountMap.containsKey(neighborLabel)) {
                int labelFreq = Collections.frequency(neighborsLabelList, neighborLabel);
                neighborLabelCountMap.put(neighborLabel.name(), labelFreq);
            }
        });

        return neighborLabelCountMap;
    }

    //Burada relationshiptype ına bakıp ona göre çocuklarını getirmeli
    public List<Node> getDataVertexNeighbors(Node dataVertex) {
        List<Relationship> relationshipList = new ArrayList<>();
        Iterable<Relationship> relationships = dataVertex.getRelationships();
        relationships.forEach(relationship -> {
            if(!relationshipList.contains(relationship)) {
                relationshipList.add(relationship);
            }
        });

        //List<Relationship> relationshipList = (List<Relationship>) dataVertex.getRelationships();
        List<Node> neighborsList = new ArrayList<>();
        relationshipList.forEach(relationship -> {
            if(!neighborsList.contains(relationship.getOtherNode(dataVertex))) {
                neighborsList.add(relationship.getOtherNode(dataVertex));
            }
        });
        return neighborsList;
    }

    public List<Node> getDataVertexNeighborsWithLabel(List<Node> dataVertexNeighbours, Label label) {
        List<Node> dataVertexNeighboursWithLabel = dataVertexNeighbours.stream().filter(dataVertexNeighbour -> dataVertexNeighbour.getLabels().iterator().hasNext()).collect(Collectors.toList());
        return dataVertexNeighboursWithLabel.stream().filter(dataVertexNeighbor -> dataVertexNeighbor.getLabels().iterator().next().name().equals(label.name())).collect(Collectors.toList());
    }

    public Map<String, Integer> getDataVertexNeighborsLabelCount(Node dataVertex) {
        List<Node> neighborsWithLabelList = getDataVertexNeighbors(dataVertex).stream().filter(neighbor -> neighbor.getLabels().iterator().hasNext()).collect(Collectors.toList());
        List<Label> neighborsLabelList = neighborsWithLabelList.stream().map(neighbor -> neighbor.getLabels().iterator().next()).collect(Collectors.toList());

        Map<String, Integer> neighborLabelCountMap = new HashMap<String, Integer>();
        neighborsLabelList.forEach(neighborLabel -> {
            if (!neighborLabelCountMap.containsKey(neighborLabel)) {
                int labelFreq = Collections.frequency(neighborsLabelList, neighborLabel);
                neighborLabelCountMap.put(neighborLabel.name(), labelFreq);
            }
        });

        return neighborLabelCountMap;
    }

    public boolean applyNlfFilter(MyNode queryVertex, Node dataVertex, Query queryGraph) {
        boolean result = true;

        Map<String, Integer> queryVertexNeighborList = getQueryVertexNeighborsLabelCount(queryVertex, queryGraph);
        Map<String, Integer> dataVertexNeighborList = getDataVertexNeighborsLabelCount(dataVertex);

        for (String queryVertexLabel : queryVertexNeighborList.keySet()) {
            if (dataVertexNeighborList.get(queryVertexLabel) != null) {
                if (!(queryVertexNeighborList.get(queryVertexLabel) <= dataVertexNeighborList.get(queryVertexLabel))) {
                    result = false;
                }
            }
        }

        return result;
    }

/*
    private void writeQueryResults(List<Map<MyNode, List<Node>>> allSolutionMap) {
        for(Map<MyNode, List<Node>> solutionMap:allSolutionMap) {
            for(int i = 0 ; i < solutionMap.size() ; i++) {
                solutionMap.get(i).forEach(solution -> {
                    System.out.print(solutionMap);
                });
            }
        }

        //QueryResult queryResult = new QueryResult();
        //queryResult.addQueryResults("Number of matches: " + matchedSubgraphs.size() + "\n");
        //queryResult.addQueryResults("Total Time Wasted In Filtering For The Initial Query Node: " + filterForInitialQueryNodeTotalTime + " ms" + "\n");
        //queryResult.addQueryResults("Total Time Wasted In Preparation of Candidate Relation Set: " + prepareCandidateRelationsTotalTime + " ms" + "\n");
        //queryResult.addQueryResults("Total Time Wasted In Relationship Permutation Check: " + relationPermutationCheckTotalTime + " ms" + "\n");
        //queryResult.printConsumptions();
    }
    */


}
