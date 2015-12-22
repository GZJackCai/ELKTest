import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Jack on 2015/12/22.
 *
 * aggregations
 * bucket :嵌套统计
 */
public class test4 {
    Client client;
    @Before
    public void before() throws UnknownHostException {
        client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法
    }

    /**
     * children-aggregation
     */
    @Test
    public void test1() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .children("agg")
                        .childType("reseller");

        // sr is here your SearchResponse object
        Children agg = new SearchResponse().getAggregations().get("agg");
        agg.getDocCount(); // Doc count
    }

    /**
     * Date Histogram Aggregation：增强型时间
     */
    @Test
    public void test2() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("dateOfBirth")
                        .interval(DateHistogramInterval.YEAR);

        AggregationBuilder aggregation1 =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("dateOfBirth")
                        .interval(DateHistogramInterval.days(10));

        // sr is here your SearchResponse object
//        Histogram agg = sr.getAggregations().get("agg");

        // sr is here your SearchResponse object
        Histogram agg = new SearchResponse().getAggregations().get("agg");
// For each entry
        for (Histogram.Bucket entry : agg.getBuckets()) {
            ValueFormat.DateTime key = (ValueFormat.DateTime) entry.getKey();    // Key
            String keyAsString = entry.getKeyAsString(); // Key as String
            long docCount = entry.getDocCount();         // Doc count

//            logger.info("key [{}], date [{}], doc_count [{}]", keyAsString, key.getYear(), docCount);
        }

    }


    /**
     * Date Range Aggregation  时间范围聚合
     * @throws Exception
     */
    @Test
    public void test3() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateRange("agg")
                        .field("dateOfBirth")
                        .format("yyyy")
                        .addUnboundedTo("1950")    // from -infinity to 1950 (excluded)
                        .addRange("1950", "1960")  // from 1950 to 1960 (excluded)
                        .addUnboundedFrom("1960"); // from 1960 to +infinity


        // sr is here your SearchResponse object
        Range agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();                // Date range as key
            ValueFormat.DateTime fromAsDate = (ValueFormat.DateTime) entry.getFrom();   // Date bucket from as a Date
            ValueFormat.DateTime toAsDate = (ValueFormat.DateTime) entry.getTo();       // Date bucket to as a Date
            long docCount = entry.getDocCount();                // Doc count

//            logger.info("key [{}], from [{}], to [{}], doc_count [{}]", key, fromAsDate, toAsDate, docCount);
        }
    }

    /**
     * Filter Aggregation: 过滤统计
     */
    @Test
    public void test4() throws Exception{
        AggregationBuilders
                .filter("agg")
                .filter(QueryBuilders.termQuery("gender", "male"));

        // sr is here your SearchResponse object
        Filter agg = new SearchResponse().getAggregations().get("agg");
        agg.getDocCount(); // Doc count
    }

    /**
     * Filters Aggregation
     * @throws Exception
     */
    @Test
    public void test5() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .filters("agg")
                        .filter("men", QueryBuilders.termQuery("gender", "male"))
                        .filter("women", QueryBuilders.termQuery("gender", "female"));


        Filters agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
//            logger.info("key [{}], doc_count [{}]", key, docCount);
        }
    }


    /**
     * geodistance-aggregation
     */
    @Test
    public void test6() throws Exception{
//        AggregationBuilder aggregation =
//                AggregationBuilders
//                        .geoDistance("agg")
//                        .field("address.location")
//                        .point(new ValuesSource.GeoPoint(48.84237171118314,2.33320027692004))
//                        .unit(DistanceUnit.KILOMETERS)
//                        .addUnboundedTo(3.0)
//                        .addRange(3.0, 10.0)
//                        .addRange(10.0, 500.0);

        // sr is here your SearchResponse object
        Range agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();    // key as String
            Number from = (Number) entry.getFrom(); // bucket from value
            Number to = (Number) entry.getTo();     // bucket to value
            long docCount = entry.getDocCount();    // Doc count

//            logger.info("key [{}], from [{}], to [{}], doc_count [{}]", key, from, to, docCount);
        }
    }

    /**
     *  Geo Hash Grid Aggregation
     */
    @Test
    public void test7() throws Exception{
// sr is here your SearchResponse object
        GeoHashGrid agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (GeoHashGrid.Bucket entry : agg.getBuckets()) {
            String keyAsString = entry.getKeyAsString(); // key as String
            GeoPoint key = (GeoPoint) entry.getKey();    // key as geo point
            long docCount = entry.getDocCount();         // Doc count

//            logger.info("key [{}], point {}, doc_count [{}]", keyAsString, key, docCount);
        }
    }

    /**
     * Global Aggregation
     * @throws Exception
     */
    @Test
    public void test8() throws Exception{
        AggregationBuilders
                .global("agg")
                .subAggregation(AggregationBuilders.terms("genders").field("gender"));


        // sr is here your SearchResponse object
        Global agg = new SearchResponse().getAggregations().get("agg");
        agg.getDocCount(); // Doc count
    }

    /**
     *  Histogram Aggregation: 可以根据其返回值生成可以用于柱状图的统计数据
     * @throws Exception
     */
    @Test
    public void test9() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .histogram("agg")
                        .field("height")
                        .interval(1);

        // sr is here your SearchResponse object
        Histogram agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (Histogram.Bucket entry : agg.getBuckets()) {
            Long key = (Long) entry.getKey();       // Key
            long docCount = entry.getDocCount();    // Doc count

//            logger.info("key [{}], doc_count [{}]", key, docCount);
        }
    }

    /**
     * Ip Range Aggregation : IP 统计
     * @throws Exception
     */
    @Test
    public void test10() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .ipRange("agg")
                        .field("ip")
                        .addUnboundedTo("192.168.1.0")             // from -infinity to 192.168.1.0 (excluded)
                        .addRange("192.168.1.0", "192.168.2.0")    // from 192.168.1.0 to 192.168.2.0 (excluded)
                        .addUnboundedFrom("192.168.2.0");          // from 192.168.2.0 to +infinity

        // sr is here your SearchResponse object
        Range agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // Ip range as key
            String fromAsString = entry.getFromAsString();  // Ip bucket from as a String
            String toAsString = entry.getToAsString();      // Ip bucket to as a String
            long docCount = entry.getDocCount();            // Doc count

//            logger.info("key [{}], from [{}], to [{}], doc_count [{}]", key, fromAsString, toAsString, docCount);
        }
    }

    /**
     * Missing Aggregation
     */
    @Test
    public void test11() throws Exception{
        AggregationBuilders.missing("agg").field("gender");

        // sr is here your SearchResponse object
        Missing agg = new SearchResponse().getAggregations().get("agg");
        agg.getDocCount(); // Doc count
    }

    /**
     *  Nested Aggregation
     */
    @Test
    public void test12() throws Exception{
        AggregationBuilders
                .nested("agg")
                .path("resellers");

        // sr is here your SearchResponse object
        Nested agg = new SearchResponse().getAggregations().get("agg");
        agg.getDocCount(); // Doc count
    }

    /**
     * Range Aggregation  ： 普通随机统计
     */
    @Test
    public void test13() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .range("agg")
                        .field("height")
                        .addUnboundedTo(1.0f)               // from -infinity to 1.0 (excluded)
                        .addRange(1.0f, 1.5f)               // from 1.0 to 1.5 (excluded)
                        .addUnboundedFrom(1.5f);            // from 1.5 to +infinity

        // sr is here your SearchResponse object
        Range agg = new SearchResponse().getAggregations().get("agg");

// For each entry
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();             // Range as key
            Number from = (Number) entry.getFrom();          // Bucket from
            Number to = (Number) entry.getTo();              // Bucket to
            long docCount = entry.getDocCount();    // Doc count

//            logger.info("key [{}], from [{}], to [{}], doc_count [{}]", key, from, to, docCount);
        }
    }

    /**
     * Reverse Nested Aggregation
     */
    @Test
    public void test14() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .nested("agg").path("resellers")
                        .subAggregation(
                                AggregationBuilders
                                        .terms("name").field("resellers.name")
                                        .subAggregation(
                                                AggregationBuilders
                                                        .reverseNested("reseller_to_product")
                                        )
                        );


        // sr is here your SearchResponse object
        Nested agg = new SearchResponse().getAggregations().get("agg");
        Terms name = agg.getAggregations().get("name");
        for (Terms.Bucket bucket : name.getBuckets()) {
            ReverseNested resellerToProduct = bucket.getAggregations().get("reseller_to_product");
            resellerToProduct.getDocCount(); // Doc count
        }
    }

    /**
     *  Significant Terms Aggregation
     * @throws Exception
     */
    @Test
    public void test15() throws Exception{
        AggregationBuilder aggregation =
                AggregationBuilders
                        .significantTerms("significant_countries")
                        .field("address.country");

// Let say you search for men only
        SearchResponse sr = client.prepareSearch()
                .setQuery(QueryBuilders.termQuery("gender", "male"))
                .addAggregation(aggregation)
                .get();

        // sr is here your SearchResponse object
        SignificantTerms agg = sr.getAggregations().get("significant_countries");

// For each entry
        for (SignificantTerms.Bucket entry : agg.getBuckets()) {
            entry.getKey();      // Term
            entry.getDocCount(); // Doc count
        }
    }

    /**
     *  Terms Aggregation:  用于对指定字段的内容进行分布统计
     * @throws Exception
     */
    @Test
    public void test16() throws Exception{
        AggregationBuilders
                .terms("genders")
                .field("gender");


        // sr is here your SearchResponse object
        Terms genders = new SearchResponse().getAggregations().get("genders");

// For each entry
        for (Terms.Bucket entry : genders.getBuckets()) {
            entry.getKey();      // Term
            entry.getDocCount(); // Doc count
        }

        AggregationBuilders
                .terms("genders")
                .field("gender")
                .order(Terms.Order.count(true));

        AggregationBuilders
                .terms("genders")
                .field("gender")
                .order(Terms.Order.term(true));

        AggregationBuilders
                .terms("genders")
                .field("gender")
                .order(Terms.Order.aggregation("avg_height", false))
                .subAggregation(
                        AggregationBuilders.avg("avg_height").field("height")
                );
    }

    @Test
    public void test17() throws Exception{

    }

    @Test
    public void test18() throws Exception{

    }

    @Test
    public void test19() throws Exception{

    }

    @Test
    public void test20() throws Exception{
    }
}
