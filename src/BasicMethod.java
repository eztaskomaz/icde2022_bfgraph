import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Path("/BasicMethod")
public class BasicMethod {
	
	private final GraphDatabaseService database;
	private Query queryGraph;
	private FilterCandidates filteringTool;
	private DetermineMatchOrder orderingTool;
	private Print printingTool = new Print();
	private MeasureSourceConsumption measuringTool = new MeasureSourceConsumption();
	private Iterable<RelationshipType> allRelationTypes;
	private List<MyNode> queryNodes = new ArrayList<MyNode>();
	private Map<Integer, Long> matchedCoupleNodeIds = new HashMap<Integer, Long>();
	private Map<Integer, Long> matchedCoupleRelationIds = new HashMap<Integer, Long>();
	private Stack<Integer> notCheckedQueryNodeIds = new Stack<Integer>();
	private List<Pair<List<Long>, List<Long>>> matchedSubgraphs= new ArrayList<Pair<List<Long>, List<Long>>>();
	// Temporary Variables
	Long startTime, endTime, timeDifference;
	Long filterForInitialQueryNodeTotalTime = (long) 0;
	Long prepareCandidateRelationsTotalTime = (long) 0;
	int recursiveCallCount = 0;
	int matchCount;
	List<Long> ids = new ArrayList<Long> ();
	String response = "";
	
	/**
	 * Connects to the database
	 */
	
	public BasicMethod(@Context GraphDatabaseService database) {
		this.database = database;
		// No need to construct the inverse vertex label list cause we can reach it by GlobalGraphOperations...getAllNodesWithLabel() function
		// No need to construct the adjacency list cause we can reach it by Node...getRelationships() function
		try ( Transaction tx = database.beginTx() )
		{
			allRelationTypes = database.getAllRelationshipTypes();
			tx.success();
			tx.close();
		}
		catch (Exception e) {
			e.getMessage();
		}
	}
	
	/**
	 * This function reads the input query from browser and analyzes the query graph
	 */

	/*
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/{query}")
	public Response executeQuery( @PathParam("query") String query) {

		// Initialize measurements
		int cpuCount = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
		Long startCPUTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
		Long startTime = System.nanoTime();

		// Run the algorithm
		queryGraph = new Query(queryNodes, query, allRelationTypes);
		queryGraph.extractQueryItems();
		filteringTool = new FilterCandidates(database, queryGraph, null, queryNodes, null);
		findRootCandidates();

		// End up measurements
		Long endTime = System.nanoTime();
    	Long totalProcessTime = (endTime - startTime) / 1000000;

    	// Print Total Wasted Times Part By Part
    	//response += "Number of matches: " + matchedSubgraphs.size() + "\n";
		response += "Number of matches (match count): " + 		matchCount + "\n";
    	response += "Total Memory Consumption: \n" + measuringTool.measureMemoryConsumption(Runtime.getRuntime()) + "\n";
    	response += "Total CPU Consumption: " + measuringTool.measureCPUConsumption(startCPUTime, startTime, cpuCount) + "%" + "\n";
    	response += "Number of Recursive Calls: " + recursiveCallCount + "\n";
    	response += "Total Process Time: " + totalProcessTime + " ms" + "\n";
    	response += "Total Time Wasted In Filtering For The Initial Query Node: " + filterForInitialQueryNodeTotalTime + " ms" + "\n";
    	response += "Total Time Wasted In Preparation of Candidate Relation Set: " + prepareCandidateRelationsTotalTime + " ms" + "\n";
    	//alttaki satir count query için gerekli değil
		//response += printingTool.printMatch(0, matchedSubgraphs);

		System.out.println(response);

		return Response
				.status(Status.OK)
				.entity(("!!!HERE THE RESULTS ARE:" + "\n" + response).getBytes(Charset.forName("UTF-8"))).build();

	}
*/

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/{query}")
	public Response executeQuery( @PathParam("query") String query) throws InterruptedException {

		MeasureSourceConsumption msc = new MeasureSourceConsumption();
		msc.startCalculateTimeAndCpuConsumption();

		// Run the algorithm
		queryGraph = new Query(queryNodes, query, allRelationTypes);
		queryGraph.extractQueryItems();
		filteringTool = new FilterCandidates(database, queryGraph, null, queryNodes, null);
		findRootCandidates();
		TimeUnit.SECONDS.sleep(3);

		msc.endCalculateTimeAndCpuConsumption();
		msc.printConsumptions(matchedSubgraphs.size());


		return Response
				.status(Status.OK)
				.entity(("!!!HERE THE RESULTS ARE:" + "\n" + response).getBytes(Charset.forName("UTF-8"))).build();

	}

	/**
	 * This function finds all the candidate nodes for the first query node (root node) by using the 
	 * first 2 filtering techniques used in GraphQL. Then it starts a search for each candidate to find 
	 * a subgraph isomorphism for the query graph.
	 */
	
	public void findRootCandidates() {
		
		MyNode rootNode = queryNodes.get(0);
		Iterator<Node> rootCandidatesIterator  = null;
		try ( Transaction tx = database.beginTx() ) {
			startTime = System.nanoTime();		// Measure Time
			rootCandidatesIterator = filteringTool.filterCandidatesByLabelAndProperty(rootNode, rootCandidatesIterator);
			rootCandidatesIterator = filteringTool.filterCandidatesByRelationships(rootNode, rootCandidatesIterator, Direction.OUTGOING);
			rootCandidatesIterator = filteringTool.filterCandidatesByRelationships(rootNode, rootCandidatesIterator, Direction.INCOMING);
			endTime = System.nanoTime();		// Measure Time
	    	timeDifference = (endTime - startTime) / 1000000;	// Measure Time
	    	filterForInitialQueryNodeTotalTime += timeDifference;	// Measure Time
	    	startTime = (long) 0;		// Measure Time
		} 
		catch (Exception e) {
		    	response += e.getMessage();
		}
		int x =0;

		for ( ; rootCandidatesIterator.hasNext(); ) {
			//System.out.println("root canditates " + x);
			Node rootCandidate = rootCandidatesIterator.next();
			matchedCoupleNodeIds.put(rootNode.getId(), rootCandidate.getId());
			try ( Transaction tx = database.beginTx() ) {
				startFromRoot(rootNode, rootCandidate);
			}
			catch (Exception e) {
			   	response += e.getMessage();
			}
			matchedCoupleNodeIds.remove(rootNode.getId());
			x++;
		}
	}
	
	/**
	 * This function finds candidate relations for relations of the node rootCandidate.
	 * Then it continues according to all possible permutations of the candidate relations.
	 * It returns true only for the case where it needs to continue matching with an other 
	 * not-checked node by popping one more item from notCheckedQueryNodeIds stack in 
	 * checkOtherMatches(). If it returns false, checkOtherMatches() function understands 
	 * that it must break while loop without checking notCheckedQueryNodeIds stack is empty or
	 * not.
	 */
	
	public boolean startFromRoot(MyNode rootNode, Node rootCandidate) {
		List<Integer> neighborIds = new ArrayList<Integer>();
		List<Integer> relationIdsSet = new ArrayList<Integer>();
		List<List<Relationship>> candidateRelationsSet = new ArrayList<List<Relationship>>();
		recursiveCallCount++;
		
		startTime = System.nanoTime();		// Measure Time	
		boolean isSuccessful = prepareCandidateRelations(Direction.OUTGOING, rootNode, rootCandidate, neighborIds, relationIdsSet, candidateRelationsSet);
		endTime = System.nanoTime();		// Measure Time
    	timeDifference = (endTime - startTime) / 1000000;	// Measure Time
    	prepareCandidateRelationsTotalTime += timeDifference;	// Measure Time
    	startTime = (long) 0;		// Measure Time
		if (isSuccessful == false)
			return false;
		
		startTime = System.nanoTime();		// Measure Time	
		isSuccessful = prepareCandidateRelations(Direction.INCOMING, rootNode, rootCandidate, neighborIds, relationIdsSet, candidateRelationsSet);
		endTime = System.nanoTime();		// Measure Time
    	timeDifference = (endTime - startTime) / 1000000;	// Measure Time
    	prepareCandidateRelationsTotalTime += timeDifference;	// Measure Time
    	startTime = (long) 0;		// Measure Time
		if (isSuccessful == false)
			return false;
		
		if (candidateRelationsSet.isEmpty())
			return true;
		checkEachPermutation(relationIdsSet, candidateRelationsSet, 0, rootCandidate, neighborIds);
		notCheckedQueryNodeIds.clear();
		return false;
	}
	
	/**
	 * This function finds the candidate relations for the query graph relations given in the 
	 * function parameter. It returns false if there is no candidate.
	 */
	
	public boolean prepareCandidateRelations(Direction direction, MyNode rootNode, Node rootCandidate, List<Integer> neighborIds, List<Integer> relationIdsSet, List<List<Relationship>> candidateRelationsSet) {
		List<MyRelation> relationList = rootNode.getRelations(direction);
		int rootNodeId = rootNode.getId();
		for (int i=0; i < relationList.size(); i++) {
			MyRelation relation = relationList.get(i);
			if ( matchedCoupleRelationIds.containsKey(relation.getId()) )
				continue; // This edge was already matched during the check of neighborId
			Iterable<Relationship> candidateRelations = rootCandidate.getRelationships(relation.getType(), direction);
			if (candidateRelations == null)
				return false;
			List<Relationship> candidateRelationList = new ArrayList<Relationship>();
			filteringTool.checkRelationProperties(relation, candidateRelations, candidateRelationList);
			if (candidateRelationList.isEmpty())
				return false;
			candidateRelationsSet.add(candidateRelationList);
			relationIdsSet.add(relation.getId());
			int neighborId = relation.getTheOtherNodeId(rootNodeId);
			neighborIds.add( neighborId );
		}
		return true;
	}
	

	
	/**
	 * This function creates all possible permutations of relations which are candidates for 
	 * the neighbor relations of root. For each possible permutation, it calls checkOtherMatches() 
	 * function in order to continue match operation of the not-matched (actually not-checked)
	 * nodes and relations. This function works with the principle of backtracking algorithm.
	 */
	
	public void checkEachPermutation(List<Integer> relationIdsSet, List<List<Relationship>> candidateRelationsSet, int index, Node rootCandidate, List<Integer> neighborIds) {
		int neighborId = neighborIds.get(index);
		MyNode neighborNode = queryNodes.get(neighborId);
		Set<String> neighborNodePropertyKeys = neighborNode.getPropertyMap().keySet();
		int relationId = relationIdsSet.get(index);
		List<Relationship> candidateRelations = candidateRelationsSet.get(index);
		for (Iterator<Relationship> r = candidateRelations.iterator(); r.hasNext(); ) {
			boolean isNodeDiscovered = false;
			Relationship candidateRelation = r.next();
			Node neighborCandidate = candidateRelation.getOtherNode(rootCandidate);
			if ( matchedCoupleNodeIds.containsKey(neighborId) ) {
				if ( matchedCoupleNodeIds.get(neighborId) != neighborCandidate.getId() )
					continue;
			}
			else if ( matchedCoupleNodeIds.containsValue(neighborCandidate.getId()) )
				continue;
			else if ( filteringTool.checkRestOfNodeProperties(neighborCandidate, neighborNode, neighborNodePropertyKeys.iterator(), false) == false )
				continue;
			else if ( filteringTool.checkRelationshipCount(neighborNode, neighborCandidate, Direction.OUTGOING) == false)
				continue;
			else if ( filteringTool.checkRelationshipCount(neighborNode, neighborCandidate, Direction.INCOMING) == false)
				continue;
			else {
				matchedCoupleNodeIds.put(neighborId, neighborCandidate.getId());
				notCheckedQueryNodeIds.push(neighborId);
				isNodeDiscovered = true;
			}
			matchedCoupleRelationIds.put(relationId, candidateRelation.getId());
			
			if ( index+1 == neighborIds.size() )
				checkOtherMatches();
			else
				checkEachPermutation(relationIdsSet, candidateRelationsSet, index+1, rootCandidate, neighborIds);
			// Remove the lastly added relationship and node to backtrack one step
			matchedCoupleRelationIds.remove(relationId);
			if (isNodeDiscovered) {
				matchedCoupleNodeIds.remove(neighborId);
				notCheckedQueryNodeIds.pop();
			}
		}
	}
	
	/**
	 * This function pops one not-checked node from stack and send it to startFromRoot()
	 * to continue matching from that node.
	 */
	
	public void checkOtherMatches() {
		Stack<Integer> tempNotCheckedQueryNodeIds = new Stack<Integer>();
		for (int i = 0; i < notCheckedQueryNodeIds.size(); i++)
			tempNotCheckedQueryNodeIds.push(notCheckedQueryNodeIds.get(i));
		boolean shouldContinue = true;
		while (notCheckedQueryNodeIds.isEmpty() == false) {
			int queryNodeId = notCheckedQueryNodeIds.pop();
			MyNode queryNode = queryNodes.get(queryNodeId);
			Long dbNodeId = matchedCoupleNodeIds.get(queryNodeId);
			Node dbNode = database.getNodeById(dbNodeId);
			shouldContinue = startFromRoot(queryNode, dbNode);
			if (shouldContinue == false)
				break;
		}
		savePermutation(shouldContinue, tempNotCheckedQueryNodeIds);
	}
	
	/**
	 * This function saves the just found match which is isomorphic to query graph.
	 * Also it brings the notCheckedQueryNodeIds stack to the old condition that is 
	 * the situation before calling the startFromRoot() inside checkOtherMatches().
	 * This is done in order to look for other possible matches rooting from the same 
	 * previously-matched partial subgraph structure.
	 */
	
	public void savePermutation(boolean isSuccessful, Stack<Integer> tempNotCheckedQueryNodeIds) {
		if (isSuccessful) {
			List<Long> dbNodeIds = new ArrayList<Long>();
			for (Iterator<Long> i = matchedCoupleNodeIds.values().iterator(); i.hasNext(); )
				dbNodeIds.add(i.next());
			List<Long> dbRelationIds = new ArrayList<Long>();
			for (Iterator<Long> i = matchedCoupleRelationIds.values().iterator(); i.hasNext(); )
				dbRelationIds.add(i.next());
			Pair<List<Long>, List<Long>> matchedInstance = Pair.of(dbNodeIds, dbRelationIds);
			//alttaki satır yorum satırı hale getilirse count query olur
			matchedSubgraphs.add(matchedInstance);
			matchCount++;
		}
		notCheckedQueryNodeIds.clear();
		notCheckedQueryNodeIds = tempNotCheckedQueryNodeIds;
	}
	


}
