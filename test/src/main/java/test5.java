import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Jack on 2015/12/22.
 */
public class test5 {


    Client client;
    @Before
    public void before() throws UnknownHostException {
        client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法
    }


    /**
     * Avg Aggregation
     * @throws Exception
     */
    @Test
    public void test1() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .avg("agg")
                        .field("height");

        Avg agg = new SearchResponse().getAggregations().get("agg");
        double value = agg.getValue();
    }

    /**
     *  Cardinality Aggregation
     * @throws Exception
     */
    @Test
    public void test2() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .cardinality("agg")
                        .field("tags");

        // sr is here your SearchResponse object
        Cardinality agg = new SearchResponse().getAggregations().get("agg");
        long value = agg.getValue();
    }


    /**
     * Extended Stats Aggregation
     */
    @Test
    public void test3() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .extendedStats("agg")
                        .field("height");

        // sr is here your SearchResponse object
        ExtendedStats agg = new SearchResponse().getAggregations().get("agg");
        double min = agg.getMin();
        double max = agg.getMax();
        double avg = agg.getAvg();
        double sum = agg.getSum();
        long count = agg.getCount();
        double stdDeviation = agg.getStdDeviation();
        double sumOfSquares = agg.getSumOfSquares();
        double variance = agg.getVariance();
    }

    /**
     * Cardinality Aggregation

     * @throws Exception
     */
    @Test
    public void test4() throws Exception{
        GeoBoundsBuilder aggregation =
                AggregationBuilders
                        .geoBounds("agg")
                        .field("address.location")
                        .wrapLongitude(true);

// sr is here your SearchResponse object
        GeoBounds agg;
        agg = new SearchResponse().getAggregations().get("agg");
        GeoPoint bottomRight = agg.bottomRight();
        GeoPoint topLeft = agg.topLeft();
//        logger.info("bottomRight {}, topLeft {}", bottomRight, topLeft);
    }

    /**
     * Max Aggregation
     */
    @Test
    public void test5() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .max("agg")
                        .field("height");

        // sr is here your SearchResponse object
        Max agg = new SearchResponse().getAggregations().get("agg");
        double value = agg.getValue();
    }

    /**
     *  Min Aggregation
     * @throws Exception
     */
    @Test
    public void test6() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .min("agg")
                        .field("height");

        // sr is here your SearchResponse object
        Min agg = new SearchResponse().getAggregations().get("agg");
        double value = agg.getValue();
    }

    /**
     * Percentile Aggregation
     */
    @Test
    public void test7() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .percentiles("agg")
                        .field("height");

        MetricsAggregationBuilder aggregation1 =
                AggregationBuilders
                        .percentiles("agg")
                        .field("height")
                        .percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0);

// sr is here your SearchResponse object
        Percentiles agg = new SearchResponse().getAggregations().get("agg");
// For each entry
        for (Percentile entry : agg) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

//            logger.info("percent [{}], value [{}]", percent, value);
        }
    }

    /**
     *  Percentile Ranks Aggregation
     * @throws Exception
     */
    @Test
    public void test8() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .percentileRanks("agg")
                        .field("height")
                        .percentiles(1.24, 1.91, 2.22);


        // sr is here your SearchResponse object
        PercentileRanks agg = new SearchResponse().getAggregations().get("agg");
// For each entry
        for (Percentile entry : agg) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

//            logger.inffo("percent [{}], value [{}]", percent, value);
        }
    }

    /**
     * Stats Aggregation： 多值统计-返回值包括最大值，最小值，求和，计数，均值
     */
    @Test
    public void test9() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .stats("agg")
                        .field("height");

        // sr is here your SearchResponse object
        Stats agg = new SearchResponse().getAggregations().get("agg");
        double min = agg.getMin();
        double max = agg.getMax();
        double avg = agg.getAvg();
        double sum = agg.getSum();
        long count = agg.getCount();
    }

    /**
     * Sum Aggregation
     * @throws Exception
     */
    @Test
    public void test10() throws Exception{
        MetricsAggregationBuilder aggregation =
                AggregationBuilders
                        .sum("agg")
                        .field("height");

        // sr is here your SearchResponse object
        Sum agg = new  SearchResponse().getAggregations().get("agg");
        double value = agg.getValue();
    }

    /**
     * Top Hits Aggregation
     * @throws Exception
     */
    @Test
    public void test11() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("agg").field("gender")
                        .subAggregation(
                                AggregationBuilders.topHits("top")
                        );

        AggregationBuilder aggregatio1n =
                AggregationBuilders
                        .terms("agg").field("gender")
                        .subAggregation(
                                AggregationBuilders.topHits("top")
                                        .setExplain(true)
                                        .setSize(1)
                                        .setFrom(10)
                        );

        // sr is here your SearchResponse object
        Terms agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (Terms.Bucket entry : agg.getBuckets()) {
//            String key = entry.getKey();                    // bucket key
            long docCount = entry.getDocCount();            // Doc count
//            longgger.info("key [{}], doc_count [{}]", key, docCount);

            // We ask for top_hits for each bucket
            TopHits topHits = entry.getAggregations().get("top");
            for (SearchHit hit : topHits.getHits().getHits()) {
//                logger.infnfo(" -> id [{}], _source [{}]", hit.getId(), hit.getSourceAsString());
            }
        }
    }

    /**
     * Value Count Aggregation
     */
    @Test
    public void test12() throws Exception{
// sr is here your SearchResponse object
        ValueCount agg = new SearchResponse().getAggregations().get("agg");
        long value = agg.getValue();
    }

}
