package micro.main;

import java.util.List;
import java.util.Map;

public class QueryResponse {

    Map<String,List<Map>> queryResponse;

    public void setQueryResponse(Map<String, List<Map>> queryResponse) {
        this.queryResponse = queryResponse;
    }

    public Map<String, List<Map>> getQueryResponse() {
        return queryResponse;
    }
}
