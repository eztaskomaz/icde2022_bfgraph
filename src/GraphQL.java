import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


@Path("/GraphQL")
public class GraphQL {

	private final GraphDatabaseService database;
	private List<MyNode> queryNodes = new ArrayList<MyNode>();
	private List<List<Node>> candidateNodes = new ArrayList<List<Node>>();
	private LinkedList<Integer> candidateSizeOrder = new LinkedList<Integer>();
	private Query queryGraph;
	private FilterCandidates filteringTool;
	private RefineCandidates refiningTool;
	private FindIsomorphicPatterns findingTool;
	private Print printingTool = new Print();
	private MeasureSourceConsumption measuringTool = new MeasureSourceConsumption();
	private Iterable<RelationshipType> allRelationTypes;
	private List<Pair<List<Long>, List<Long>>> matchedSubgraphs;

	String response = "";
	
	/**
	 * Connects to the database
	 */
	
	public GraphQL(@Context GraphDatabaseService database) {
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
	
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/{query}")
	public Response executeQuery( @PathParam("query") String query) {

		System.out.print("We are executing the query \n");
		
		// Initialize measurements
		int cpuCount = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
		Long startCPUTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
		Long startTime = System.nanoTime();
		
		// Run the algorithm
		queryGraph = new Query(queryNodes, query, allRelationTypes);
		queryGraph.extractQueryItems();
		Long startFilteringTime = System.nanoTime();

		System.out.print("We are executing the query 1 \n");


		filteringTool = new FilterCandidates(database, queryGraph, candidateNodes, queryNodes, candidateSizeOrder);
		filteringTool.filterCandidates();
		Long breakingTime = System.nanoTime();

		System.out.print("We are executing the query 2 \n");

		refiningTool = new RefineCandidates(database, candidateNodes, queryNodes, queryGraph);
		refiningTool.refineSearchSpace(queryNodes.size());
		Long endRefiningTime = System.nanoTime();

		System.out.print("We are executing the query 3 \n");


		findingTool = new FindIsomorphicPatterns(database, queryNodes, candidateNodes, queryGraph, candidateSizeOrder, response);
	    findingTool.findIsomorphicSubgraphs();
			
		// End up measurements
		Long endTime = System.nanoTime();
    	Long totalProcessTime = (endTime - startTime) / 1000000;
    	matchedSubgraphs = findingTool.getMatchedSubgraphs();

		// Find Isomorphic Patterns
    	response += "Number of matches: " + matchedSubgraphs.size() + "\n";
    	response += "Total Memory Consumption: \n" + measuringTool.measureMemoryConsumption(Runtime.getRuntime()) + "\n";
    	response += "Total CPU Consumption: " + measuringTool.measureCPUConsumption(startCPUTime, startTime, cpuCount) + "%" + "\n";
    	response += "Number of Recursive Calls: " + findingTool.getRecursiveCallCount() + "\n";
    	response += "Total Process Time: " + totalProcessTime + " ms" + "\n";
    	response += "Total Time Wasted In Filtering For All The Query Nodes: " + (breakingTime - startFilteringTime) / 1000000 + " ms" + "\n";
    	response += "Total Time Wasted In Refining of Candidate DB Nodes: " + (endRefiningTime - breakingTime) / 1000000 + " ms" + "\n";
		response += "Number of matches (match count): " + 		 findingTool.getMatchedSubgraphCount() + "\n";

		//count query için silinmeli
		//response += printingTool.printMatch(0, matchedSubgraphs);

    	//for (int i = 0; i < candidateNodes.size(); i++)
			//response += "AFTER REFINEMENT: " + candidateNodes.get(i).size() + " candidates for QUERY NODE-" + i + "\n";

		System.out.println("GraphQL Query Results: " + response);

		return Response
				.status(Status.OK)
				.entity(("!!!HERE THE RESULTS ARE:" + "\n" + response).getBytes(Charset
						.forName("UTF-8"))).build();
	}

}
