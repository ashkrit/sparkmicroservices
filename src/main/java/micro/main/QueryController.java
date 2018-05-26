package micro.main;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Controller
@EnableAutoConfiguration
public class QueryController {

    private static final Logger _log = LoggerFactory.getLogger(QueryController.class);

    private ExecutorService queryExecutor = Executors.newFixedThreadPool(2);

    @RequestMapping("/")
    @ResponseBody
    String ping() {
        return "Pong";
    }

    @RequestMapping("/spark")
    @ResponseBody
    List<Map> spark(@RequestParam String text) throws Exception {
        SparkSession sqlSession = SparkSQLDataStore.vehicleTable();
        _log.info("SQL {} {}", sqlSession, text);
        List<Map> data = execute(sqlSession, text);
        return data;

    }

    @RequestMapping(value = "/multiplequery", method = RequestMethod.POST)
    @ResponseBody
    QueryResponse multiSpark(@RequestBody QueryRequest request) throws Exception {
        SparkSession sparkSession = SparkSQLDataStore.vehicleTable();

        List<Future<Pair<String, List<Map>>>> result = executeQuery(request, sparkSession);
        List<Pair<String, List<Map>>> queryReply = waitForResult(result);
        return buildResponse(queryReply);
    }

    private QueryResponse buildResponse(List<Pair<String, List<Map>>> queryReply) {
        final QueryResponse response = new QueryResponse();
        response.setQueryResponse(new LinkedHashMap<>());
        queryReply.stream()
                .forEach(data -> response.getQueryResponse().put(data.getLeft(), data.getRight()));

        return response;
    }

    private List<Pair<String, List<Map>>> waitForResult(List<Future<Pair<String, List<Map>>>> result) {
        return result
                .stream()
                .map(data -> unsafeBlock(data))
                .collect(toList());
    }

    private List<Future<Pair<String, List<Map>>>> executeQuery(QueryRequest request, SparkSession sql) {
        return request.getQuery()
                .stream()
                .map(query -> queryExecutor.submit(() -> Pair.of(query, execute(sql, query))))
                .collect(toList());
    }

    private Pair<String, List<Map>> unsafeBlock(Future<Pair<String, List<Map>>> data) {
        try {
            return data.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<Map> execute(SparkSession sql, String text) {
        Dataset<Row> content = sql.sql(text);
        return content.takeAsList(100).stream().map(row -> {
            Map<String, Object> rowData = new LinkedHashMap<>();
            rowData.put("year", row.getAs(0));
            rowData.put("make", row.getAs(1));
            rowData.put("number", row.getAs(1));
            return rowData;
        }).collect(toList());
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(QueryController.class, args);
    }

}
