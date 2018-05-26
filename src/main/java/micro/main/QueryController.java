package micro.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Controller
@EnableAutoConfiguration
public class QueryController {

    private static final Logger _log = LoggerFactory.getLogger(QueryController.class);

    @RequestMapping("/")
    @ResponseBody
    String ping() {
        return "Pong";
    }

    @RequestMapping("/spark")
    @ResponseBody
    List<Map> spark(@RequestParam String text) throws Exception {
        SparkSession sql = SparkSQLDataStore.vehicleTable();
        _log.info("SQL {} {}", sql, text);

        Dataset<Row> content = sql.sql(text);
        List<Map> data = content.takeAsList(5).stream().map(row -> {
            Map<String, Object> rowData = new LinkedHashMap<>();
            rowData.put("year", row.getAs(0));
            rowData.put("make", row.getAs(1));
            rowData.put("number", row.getAs(1));
            return rowData;
        }).collect(toList());

        return data;

    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(QueryController.class, args);
    }

}
