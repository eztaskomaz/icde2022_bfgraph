import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterCandidatesForBBPlus {

    private final GraphDatabaseService database;
    private Query queryGraph;
    private List<List<Node>> candidateNodes;
    private List<MyNode> queryNodes;

    public FilterCandidatesForBBPlus(GraphDatabaseService database, Query queryGraph, List<MyNode> queryNodes) {
        this.database = database;
        this.queryGraph = queryGraph;
        this.queryNodes = queryNodes;
    }

    public void filterCandidatesByLabelAndProperty(MyNode queryNode, List<Node> candidateNodesList) {
        List<Label> labelList = queryNode.getLabels();
        Label firstLabel = labelList.get(0);
        Set<String> propertyKeySet = queryNode.getPropertyMap().keySet();
        Iterator<Node> candidateNodesIterator = null;

        if (queryNode.hasAnyProperty()) {
            String firstPropertyKey = propertyKeySet.iterator().next();
            String firstPropertyValue = queryNode.getProperty(firstPropertyKey);
            if (StringUtils.isNumeric(firstPropertyValue)) {
                candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, Double.parseDouble(firstPropertyValue));
                candidateNodesIterator.forEachRemaining(candidateNodesList::add);
                if(candidateNodesList.isEmpty()) {
                    candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, Integer.parseInt(firstPropertyValue));
                    candidateNodesIterator.forEachRemaining(candidateNodesList::add);
                }
            } else {
                candidateNodesIterator = database.findNodes(firstLabel, firstPropertyKey, firstPropertyValue);
                candidateNodesIterator.forEachRemaining(candidateNodesList::add);
            }
        } else {
            candidateNodesIterator = database.findNodes(firstLabel);
            candidateNodesIterator.forEachRemaining(candidateNodesList::add);
        }

    }

    public void filterCandidatesByRelationships(MyNode queryNode, List<Node> candidateNodesList, Direction direction) {
        List<Object> relationTypeCountList = queryNode.getRelationTypesWithCount(direction);
        for(Iterator<Object> i = relationTypeCountList.iterator(); i.hasNext(); ) {
            RelationshipType type = (RelationshipType) i.next();
            int count = (int) i.next();
            candidateNodesList.stream().filter(node -> node.getDegree(type, direction) >= count).collect(Collectors.toList());
        }
    }

    public void checkRelationProperties(MyRelation relation, Iterable<Relationship> candidateRelations, List<Relationship> candidateRelationList) {
        for (Iterator<Relationship> crIterator = candidateRelations.iterator(); crIterator.hasNext(); ) {
            Relationship candidateRelation = crIterator.next();
            boolean satisfied = true;
            for (Iterator<String> keyIterator = relation.getPropertyMap().keySet().iterator(); keyIterator.hasNext(); ){
                String propertyKey = keyIterator.next();
                if ( candidateRelation.hasProperty(propertyKey) )
                    if ( analyzePropertyByType(candidateRelation.getProperty(propertyKey), relation.getProperty(propertyKey)) )
                        continue;
                satisfied = false;
                break;
            }
            if (satisfied)
                candidateRelationList.add(candidateRelation);
        }
    }

    public List<Relationship> checkRelationProperties(MyRelation relation, Iterable<Relationship> candidateRelations) {
        List<Relationship> candidateRelationList = new ArrayList<>();
        candidateRelations.forEach(candidateRelation -> {
            boolean satisfied = true;
            Set<String> propertyKeyMap = relation.getPropertyMap().keySet();
            for(String propertyKey : propertyKeyMap) {
                if(candidateRelation.hasProperty(propertyKey)) {
                    if (analyzePropertyByType(candidateRelation.getProperty(propertyKey), relation.getProperty(propertyKey)))
                        continue;
                    satisfied = false;
                    break;
                }
            }
            if (satisfied) {
                candidateRelationList.add(candidateRelation);
            }
        });
        return candidateRelationList;
    }

    public boolean analyzePropertyByType(Object dbPropertyValue, String queryPropertyValue) {
        if(queryPropertyValue.equals(dbPropertyValue.toString())) {
            return true;
        } else {
            return false;
        }
    }

    public boolean checkRestOfNodeProperties(Node node, MyNode queryNode, Iterator<String> propertyKeyIterator, boolean skipFirst) {
        if (skipFirst) {
            propertyKeyIterator.next();
        }

        for ( ; propertyKeyIterator.hasNext(); ) {
            String propertyKey = propertyKeyIterator.next();
            String propertyValue = queryNode.getProperty(propertyKey);
            if (node.hasProperty(propertyKey))
                if ( analyzePropertyByType(node.getProperty(propertyKey), propertyValue) )
                    continue;
            return false;
        }

        return true;
    }

    public boolean checkRelationshipCount(MyNode queryNode, Node candidateDbNode, Direction direction) {
        List<Object> relationTypeCountList = queryNode.getRelationTypesWithCount(direction);
        for(Iterator<Object> i = relationTypeCountList.iterator(); i.hasNext(); ) {
            RelationshipType type = (RelationshipType) i.next();
            int count = (int) i.next();
            if (candidateDbNode.getDegree(type, direction) < count)
                return false;
        }
        return true;
    }

}
