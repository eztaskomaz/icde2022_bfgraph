
public class QueryResult {

    private String queryResults;

    public QueryResult() {
        this.queryResults = "Query Results: " + "\n";
    }

    public void addQueryResults(String result) {
        queryResults += result + "\n";
    }

    public void printConsumptions() {
        System.out.println(queryResults);
    }

}
