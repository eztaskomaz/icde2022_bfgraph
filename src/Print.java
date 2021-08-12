import org.neo4j.helpers.collection.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

public class Print {
	
	private String response;
	private int matchCount;
	private long previousNodeId;
	private int uniqueFamilyCount;
	private int matchCountPerFamily;
	
	
	/**
	 * Constructor of the class
	 */
	
	public Print() {
		this.response = "";
		this.matchCount = 0;
		this.previousNodeId = (long) 0;
		this.uniqueFamilyCount = 0;
		this.matchCountPerFamily = 1;
	}
	
	/**
	 * This function prints the matched instance whose rank is given as the function parameter.
	 */
	
	public String printMatch(int rank, List<Pair<List<Long>, List<Long>>> matchedSubgraphs) {
		response += "********** MATCH NO - " + rank + ": **********" + "\n";
		Pair<List<Long>, List<Long>> matchedInstance = matchedSubgraphs.get(rank);
		List<Long> nodeIds = matchedInstance.first();
		List<Long> relationIds = matchedInstance.other();
		response += "Node Ids:    ";
		for (int i=0; i < nodeIds.size(); i++)
			response += nodeIds.get(i) + "  ";
		response += "\nRelation Ids: ";
		for (int i=0; i < relationIds.size(); i++)
			response += relationIds.get(i) + "  ";
		response += "\n****************************************\n";
		return response;
	}
	
	/**
	 * This function prints the node ids and relationship ids to check whether the 
	 * match found is correct. It prints the content of matchedSubgraphs.
	 */
	
	public Integer printResults(List<Pair<List<Long>, List<Long>>> matchedSubgraphs, Map<Integer, Long> matchedCoupleNodeIds) {
		Long motherNodeId = matchedCoupleNodeIds.get(0);
		if (motherNodeId != previousNodeId) {
//			if (previousNodeId > 0) {
//				response += "********** " + matchCount + " **********" + "\n"; 
//				response += previousNodeId + " x " + matchCountPerFamily + "\n";
//				int lastMatchIndex = matchedSubgraphs.size() - 1;
//				Pair<List<Long>, List<Long>> matchedInstance = matchedSubgraphs.get(lastMatchIndex);
//				List<Long> nodeIds = matchedInstance.first();
//				List<Long> relationIds = matchedInstance.other();
//				response += "Node Ids:    ";
//				for (int i=0; i < nodeIds.size(); i++)
//					response += nodeIds.get(i) + "  ";
//				response += "\nRelation Ids: ";
//				for (int i=0; i < relationIds.size(); i++)
//					response += relationIds.get(i) + "  ";
//				response += "\n *************************** \n";
//			}
			previousNodeId = motherNodeId;
			uniqueFamilyCount++;
			matchCountPerFamily = 1;
		}
		else
			matchCountPerFamily++;
		matchCount++;
		return matchCount;
		//return response;
	}	
	
	/**
	 * This function prints the node ids and relationship ids to check whether the 
	 * match found is correct. It prints the content of matchedCoupleNodeIds and 
	 * matchedCoupleRelationIds.
	 */
	
	public String printResults2(Map<Integer, Long> matchedCoupleNodeIds, Map<Integer, Long> matchedCoupleRelationIds, Stack<Integer> notCheckedQueryNodeIds) {
		response += "******* " + matchCount + " ********\n";
		response += "MATCHED NODE IDS:\n";
		for (Iterator<Entry<Integer, Long>> k = matchedCoupleNodeIds.entrySet().iterator(); k.hasNext(); ) {
			Entry<Integer, Long> entry = k.next();
			response += "Key: " + entry.getKey() + " - Value: " + entry.getValue() + "\n";
		}
		response += "MATCHED RELATION IDS:\n";
		for (Iterator<Entry<Integer, Long>> k = matchedCoupleRelationIds.entrySet().iterator(); k.hasNext(); ) {
			Entry<Integer, Long> entry = k.next();
			response += "Key: " + entry.getKey() + " - Value: " + entry.getValue() + "\n";
		}
		response += "NOT CHECKED QUERY NODE IDS\n";
		for (int k=0; k < notCheckedQueryNodeIds.size(); k++)
			response += notCheckedQueryNodeIds.get(k) + "\n";
		response += "******************\n";
		matchCount++;
		return response;
	}
	
	/**
	 * This function prints the results into a file
	 */
	
	public void printIntoFile() {
//		Writer writer = null;
//
//		try {
//		    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\merve.asiler\\workspace\\unmanaged\\filename.txt"), "utf-8"));
//		    writer.write("something");
//		} catch (IOException ex) {
//		  // report
//		} finally {
//		   try {writer.close();} catch (Exception ex) {/*ignore*/}
//		}
		
		File file = new File("C:\\Users\\merve.asiler\\workspace\\unmanaged\\filename.txt");
        if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
		try {
			FileWriter fileWriter = new FileWriter(file, false);
			BufferedWriter bWriter = new BufferedWriter(fileWriter);
			bWriter.write("Hello");
			bWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
