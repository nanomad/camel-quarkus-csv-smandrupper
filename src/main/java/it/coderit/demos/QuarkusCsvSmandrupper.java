package it.coderit.demos;

import org.apache.camel.Exchange;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.ExpressionBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvConstants;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.camel.language.simple.SimpleExpressionBuilder;
import org.apache.commons.csv.CSVFormat;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class QuarkusCsvSmandrupper extends RouteBuilder {

    private static final int PRODUCTS_PER_OUTPUT_FILE = 50;

    @Override
    public void configure() throws Exception {
        CsvDataFormat csv = new CsvDataFormat()
                .setFormat(CSVFormat.EXCEL)
                .setCaptureHeaderRecord(true)
                .setUseOrderedMaps(true)
                .setDelimiter(';');

        from("file://data?include=.*\\.csv")
                .log("Received file: ${file:name}")
                .streamCaching()
                .unmarshal(csv)
                .split(SimpleExpressionBuilder.collateExpression("${body}", PRODUCTS_PER_OUTPUT_FILE)).streaming().aggregationStrategy(AggregationStrategies.useOriginal())
                    .to("direct:process-bucket")
                .end()
                .log("Completed processing file: ${file:name}");

        from("direct:process-bucket")
                .setHeader(Exchange.FILE_NAME, simple("${file:onlyname.noext}-${header.CamelSplitIndex}.csv"))
                .split(body(), AggregationStrategies.flexible().accumulateInCollection(ArrayList.class))
                .process(x -> {
                    Map<String, Object> body = x.getMessage().getBody(Map.class);
                    float total = Float.parseFloat((String) body.get("price")) * Float.parseFloat((String) body.get("qty"));
                    body.put("total", total);
                })
                .end()
                .process(x -> {
                    List<String> header = x.getMessage().getHeader(CsvConstants.HEADER_RECORD, List.class);
                    Map<String, Object> headerAsMap = new LinkedHashMap<>(header.size());
                    header.forEach(e -> headerAsMap.put(e, e));
                    headerAsMap.put("total", "total");
                    List<Map<String, Object>> rows = x.getMessage().getBody(List.class);
                    rows.add(0, headerAsMap);
                })
                .marshal(csv)
                .to("file://data/out")
                .log("Generated ${header.CamelFileName}");
    }
}
