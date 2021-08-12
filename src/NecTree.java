import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NecTree {

    private int number_of_NecNodes = 0;

    private NecNode rootNecNode;
    private Map<Integer, NecNode> queryNodeNecNodeMap;
    private List<NecNode> necNodeList;

    public NecNode getRootNecNode() {
        return rootNecNode;
    }

    public void setRootNecNode(NecNode rootNecNode) {
        this.rootNecNode = rootNecNode;
    }

    public Map<Integer, NecNode> getQueryNodeNecNodeMap() {
        return queryNodeNecNodeMap;
    }

    public void setQueryNodeNecNodeMap(Map<Integer, NecNode> queryNodeNecNodeMap) {
        this.queryNodeNecNodeMap = queryNodeNecNodeMap;
    }

    public List<NecNode> getNecNodeList() {
        return necNodeList;
    }

    public void setNecNodeList(List<NecNode> necNodeList) {
        this.necNodeList = necNodeList;
    }

    public NecTree(MyNode startNode) {
        rootNecNode = new NecNode(createNecNodeId(), startNode.getLabel());
        rootNecNode.addMember(startNode);
        queryNodeNecNodeMap = new HashMap<Integer, NecNode>();
        queryNodeNecNodeMap.put(startNode.getId(), rootNecNode);
        necNodeList = new ArrayList<>();
        necNodeList.add(rootNecNode);
    }

    public NecNode RewriteToNECTree(Query queryGraph) {
        List<Integer> visitedQueryNodeIds = new ArrayList<>();
        List<NecNode> currentNecNodes = new ArrayList<>();
        List<NecNode> nextNecNodes = new ArrayList<>();

        visitedQueryNodeIds.add(rootNecNode.getNECMembers().get(0).getId());
        nextNecNodes.add(rootNecNode);

        while(nextNecNodes.size() > 0) {
            currentNecNodes.addAll(nextNecNodes);
            nextNecNodes.clear();

            for(NecNode currentNecNode: currentNecNodes) {
                List<MyNode> necMembers = currentNecNode.getNECMembers();
                List<Integer> neighborSet = new ArrayList<Integer>();
                for(MyNode necMember: necMembers) {
                    takeUnion(necMember.getNeighborsIdList(), neighborSet);
                }
                takeUnvisitedsAndMark(neighborSet, visitedQueryNodeIds);
                if (neighborSet.size() > 0) {
                    List<List<MyNode>> nodeGroupsByLabel = groupByLabel(neighborSet, queryGraph.getQueryNodes());
                    for(List<MyNode> nodeGroup: nodeGroupsByLabel){
                        List<List<MyNode>> listOfNecs = new ArrayList<List<MyNode>>();
                        findNec(nodeGroup, listOfNecs, queryGraph);
                        createNecs(listOfNecs, currentNecNode, nextNecNodes, queryGraph);
                    }
                }
            }
            currentNecNodes.clear();
        }
        visitedQueryNodeIds.clear();
        return rootNecNode;
    }

    private void findNec(List<MyNode> nodeGroup, List<List<MyNode>> listOfNecs, Query queryGraph) {
        List<MyNode> sampleGroup = new ArrayList<MyNode>();
        List<MyNode> tempGroup = new ArrayList<MyNode>();
        MyNode sampleNode = nodeGroup.get(0);
        List<Integer> sampleNodeNeighborIds = sampleNode.getNeighborsIdList();
        for(MyNode queryNode: nodeGroup) {
            List<Integer> queryNodeNeighborIds = queryNode.getNeighborsIdList();
            if (sampleNodeNeighborIds.size() == queryNodeNeighborIds.size()) {
                if (sampleNodeNeighborIds.containsAll(queryNodeNeighborIds))
                    sampleGroup.add(queryNode);
                else {
                    if (doTheyFormClique(sampleNode, queryNode, queryGraph))
                        sampleGroup.add(queryNode);
                    else
                        tempGroup.add(queryNode);
                }
            } else
                tempGroup.add(queryNode);
        }
        nodeGroup.clear();
        if (tempGroup.size() > 0) {
            findNec(tempGroup, listOfNecs, queryGraph);
            tempGroup.clear();
        }
        tempGroup = filterNecOfSampleGroup(sampleGroup, sampleNode, queryGraph);
        listOfNecs.add(sampleGroup);
        if (tempGroup.size() > 0) {
            findNec(tempGroup, listOfNecs, queryGraph);
            tempGroup.clear();
        }
    }

    public void createNecs(List<List<MyNode>> listOfNECs, NecNode parentNode, List<NecNode> childNECNodes, Query queryGraph) {
        for (int i=0; i<listOfNECs.size(); i++) {
            List<MyNode> necMembers = listOfNECs.get(i);
            MyNode sampleNode = necMembers.get(0);
            NecNode necNode = new NecNode(number_of_NecNodes, sampleNode.getLabel());
            for (int j=0; j<necMembers.size(); j++) {
                MyNode queryNode = necMembers.get(j);
                necNode.addMember(queryNode);
                queryNodeNecNodeMap.put(queryNode.getId(), necNode);
            }
            necNode.setParentNecNode(parentNode);

            MyNode sampleParentNode = parentNode.getNECMembers().get(0);
            MyRelation outgoingRelation = queryGraph.getRelationByNodeIds(sampleNode.getId(), sampleParentNode.getId());
            MyRelation incomingRelation = queryGraph.getRelationByNodeIds(sampleParentNode.getId(), sampleNode.getId());
            necNode.setRelations(outgoingRelation, incomingRelation);

            parentNode.addChild(necNode);
            necNodeList.add(necNode);
            number_of_NecNodes++;
            childNECNodes.add(necNode);
        }
    }

    public List<MyNode> filterNecOfSampleGroup(List<MyNode> group, MyNode sampleNode, Query queryGraph) {
        List<MyNode> nec = new ArrayList<MyNode>();
        List<MyNode> tempGroup = new ArrayList<MyNode>();
        List<MyRelation> outgoingRelations = sampleNode.getRelations(Direction.OUTGOING);
        List<MyRelation> incomingRelations = sampleNode.getRelations(Direction.INCOMING);
        for (int i=0; i<group.size(); i++) {
            MyNode queryNode = group.get(i);
            //Kod değişikliği yapıldı
            if (haveTheSameNeighborhood(outgoingRelations, queryNode, Direction.OUTGOING, queryGraph) && haveTheSameNeighborhood(incomingRelations, queryNode, Direction.INCOMING, queryGraph)) {
                nec.add(queryNode);
            } else {
                tempGroup.add(queryNode);
            }
        }
        group.clear();
        group.addAll(nec);
        nec.clear();
        return tempGroup;
    }

    public boolean haveTheSameNeighborhood(List<MyRelation> relations, MyNode queryNode, Direction direction, Query queryGraph) {
        for (int i=0; i<relations.size(); i++) {
            MyRelation relation1 = relations.get(i);
            MyRelation relation2 = null;
            if (direction == Direction.OUTGOING)
                relation2 = queryGraph.getRelationByNodeIds(queryNode.getId(), relation1.getEndNodeId());
            else
                relation2 = queryGraph.getRelationByNodeIds(relation1.getStartNodeId(), queryNode.getId());
            if (relation2 == null || relation1.getType() != relation2.getType())
                return false;
        }
        return true;
    }

    // bakılmalı
    private boolean doTheyFormClique(MyNode node1, MyNode node2, Query queryGraph) {
        MyRelation relation1 = queryGraph.getRelationByNodeIds(node1.getId(), node2.getId());
        MyRelation relation2 = queryGraph.getRelationByNodeIds(node2.getId(), node1.getId());
        if (relation1 == null || relation2 == null)
            return false;
        else if (relation1.getType() == relation2.getType())
            return true;
        else
            return false;
    }

    private List<List<MyNode>> groupByLabel(List<Integer> neighborSet, List<MyNode> queryNodes) {
        List<List<MyNode>> nodeGroupsByLabel = new ArrayList<List<MyNode>>();
        List<Label> observedLabels = new ArrayList<Label>();
        for(int i=0; i<neighborSet.size(); i++) {
            int node_id = neighborSet.get(i);
            MyNode queryNode = queryNodes.get(node_id);
            Label nodeLabel = queryNode.getLabel();
            if (observedLabels.contains(nodeLabel)) {
                int index = observedLabels.indexOf(nodeLabel);
                nodeGroupsByLabel.get(index).add(queryNode);
            }
            else {
                observedLabels.add(nodeLabel);
                List<MyNode> newGroup = new ArrayList<MyNode>();
                newGroup.add(queryNode);
                nodeGroupsByLabel.add(newGroup);
            }
        }
        observedLabels.clear();
        return nodeGroupsByLabel;
    }

    private void takeUnvisitedsAndMark(List<Integer> neighborSet, List<Integer> visitedQueryNodeIds) {
        List<Integer> unvisitedNeighborSet = new ArrayList<Integer>();
        for (int i=0; i<neighborSet.size(); i++) {
            int node_id = neighborSet.get(i);
            if (visitedQueryNodeIds.contains(node_id))
                continue;
            unvisitedNeighborSet.add(node_id);
            visitedQueryNodeIds.add(node_id);
        }
        neighborSet.clear();
        neighborSet.addAll(unvisitedNeighborSet);
        unvisitedNeighborSet.clear();
    }

    private void takeUnion(List<Integer> set1, List<Integer> set2) {
        if (set2.size() == 0)
            set2.addAll(set1);
        else
            for (int i=0; i<set1.size(); i++) {
                int element = set1.get(i);
                if (set2.contains(element))
                    continue;
                set2.add(element);
            }
    }

    private Integer createNecNodeId() {
        number_of_NecNodes++;
        return number_of_NecNodes;
    }

}
