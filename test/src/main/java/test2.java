import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by Jack on 2015/12/22.
 * query-dsl
 */
public class test2 {
    Client client;
    @Before
    public void before() throws UnknownHostException {
        client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法
    }

    /**
     * Bool Query:  由其他类型组合而成的文档匹配类型
     * @throws Exception
     */
    @Test
    public void test1() throws Exception{
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("content", "test1"))   //必须查询
        .must(QueryBuilders.termQuery("content", "test4"))
        .mustNot(QueryBuilders.termQuery("content", "test2")) //非必须查询,必须都为非真
        .should(QueryBuilders.termQuery("content", "test3"))  //可以匹配也可以不匹配
        .filter(QueryBuilders.termQuery("content", "test5")); //匹配文档中必须出现的查询，但对命中得分没有贡献
    }

    /**
     * Boosting Query
     * @throws Exception
     */
    @Test
    public void test2() throws Exception{
//        QueryBuilder qb = QueryBuilders.boostingQuery(
//                QueryBuilders.termQuery("name","kimchy"),    //query that will promote documents
//                QueryBuilders.termQuery("name","dadoonet"))  //query that will demote documents
//                .negativeBoost(0.2f);              //negative boost
    }

    /**
     * Common Terms Query
     */
    @Test
    public void test3() throws Exception{
        QueryBuilder qb = QueryBuilders.commonTermsQuery("name",
                "kimchy");
    }

    /**
     * Constant Score Query
     * @throws Exception
     */
    @Test
    public void test4() throws Exception{
        QueryBuilder qb = QueryBuilders.constantScoreQuery(
                QueryBuilders.termQuery("name","kimchy")
        ).boost(2.0f);
    }

    /**
     * Dis Max Query
     * @throws Exception
     */
    @Test
    public void test5() throws Exception{
        QueryBuilder qb = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("name", "kimchy"))
        .add(QueryBuilders.termQuery("name", "elasticsearch"))
        .boost(1.2f)  //boost factor
        .tieBreaker(0.7f);      //tie breaker
    }

    /**
     * Exists Query
     * @throws Exception
     */
    @Test
    public void test6() throws Exception{
        QueryBuilder qb = QueryBuilders.existsQuery("name");
    }

    /**
     * Function Score Query
     */
    @Test
    public void test7() throws Exception{
//        FilterFunctionBuilder[] functions = {
//                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
//                        QueryBuilders.matchQuery("name", "kimchy"),                //Add a first function based on a query
//                        randomFunction("ABCDEF")),                    //And randomize the score based on a given seed
//                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
//                        exponentialDecayFunction("age", 0L, 1L))      //Add another function based on the age field
//        };
    }

    /**
     * Fuzzy Query: 其查询是选择模糊串并选择其产生最好的区分词项
     * @throws Exception
     */
    @Test
    public void test8() throws Exception{
        QueryBuilder qb = QueryBuilders.fuzzyQuery(
                "name",
                "kimzhy"
        );
    }

    /**
     * Geo Bounding Box Query
     * @throws Exception
     */
    @Test
    public void test9() throws Exception{
//        QueryBuilder qb = QueryBuilders.geoBoundingBoxQuery("pin.location").setCorners(40.73, -74.1,                         //bounding box top left point
//                40.717, -73.99);                       //bounding box bottom right point
    }

    /**
     * Geo Distance Query
     * @throws Exception
     */
    @Test
    public void test10() throws Exception{
        QueryBuilder qb =  QueryBuilders.geoDistanceQuery("pin.location")
        .point(40, -70)                                //center point
        .distance(200, DistanceUnit.KILOMETERS)         //distance from center point
        .optimizeBbox("memory")                         //optimize bounding box: `memory`, `indexed` or `none`
        .geoDistance(GeoDistance.ARC);                  //distance computation mode: `GeoDistance.SLOPPY_ARC` (default), `GeoDistance.ARC` (slightly more precise but
        //significantly slower) or `GeoDistance.PLANE` (faster, but inaccurate on long distances and close to the poles)
    }

    /**
     *  Geo Distance Range Query
     */
    @Test
    public void test11() throws Exception{
//        QueryBuilder qb = QueryBuilders.geoDistanceRangeQuery("pin.location",new GeoPoint(40, -70))
//        .from("200km")
//        .to("400km")
//        .includeLower(true)                                         //include lower value means that `from` is `gt` when `false` or `gte` when `true`
//        .includeUpper(false)                                        //include upper value means that `to` is `lt` when `false` or `lte` when `true`
//        .optimizeBbox("memory")                                     //optimize bounding box: `memory`, `indexed` or `none`
//        .geoDistance(GeoDistance.ARC);                               //distance computation mode: `GeoDistance.SLOPPY_ARC` (default), `GeoDistance.ARC` (slightly more precise but
//        significantly slower) or `GeoDistance.PLANE` (faster, but inaccurate on long distances and close to the poles)
    }

    /**
     * Geohash Cell Query
     */
    @Test
    public void test12() throws Exception{
        QueryBuilder qb = QueryBuilders.geoHashCellQuery("pin.location",
                new GeoPoint(13.4080, 52.5186))
        .neighbors(true)
        .precision(3);
    }

    /**
     *  Geo Polygon Query
     * @throws Exception
     */
    @Test
    public void test13() throws Exception{
//        List<GeoPoint> points = new ArrayList<GeoPoint>();
//                points.add(new GeoPoint(40, -70));
//        points.add(new GeoPoint(30, -80));
//        points.add(new GeoPoint(20, -90));
//
//        QueryBuilder qb =
//                QueryBuilders.geoPolygonQuery("pin.location", points);
    }

    /**
     * GeoShape Query
     * @throws Exception
     */
    @Test
    public void test14() throws Exception{
        GeoShapeQueryBuilder qb = QueryBuilders.geoShapeQuery(
                "pin.location",
                ShapeBuilder.newMultiPoint()
        .point(0, 0)
                .point(0, 10)
                .point(10, 10)
                .point(10, 0)
                .point(0, 0));
        qb.relation(ShapeRelation.WITHIN);
    }

    /**
     * Has Child Query
     * @throws Exception
     */
    @Test
    public void test15() throws Exception{
        QueryBuilder qb = QueryBuilders.hasChildQuery(
                "blog_tag",
                QueryBuilders.termQuery("tag","something")
        );
    }


    /**
     * Has Parent Query
     * @throws Exception
     */
    @Test
    public void test16() throws Exception{
        QueryBuilder qb = QueryBuilders.hasParentQuery(
                "blog",
                QueryBuilders.termQuery("tag","something")
        );
    }

    /**
     *  Ids Query
     * @throws Exception
     */
    @Test
    public void test17() throws Exception{
        QueryBuilder qb = QueryBuilders.idsQuery("my_type", "type2")
                .addIds("1", "4", "100");

        QueryBuilder qb1 = QueryBuilders.idsQuery()
        .addIds("1", "4", "100");
    }

    /**
     * Indices Query
     * @throws Exception
     */
    @Test
    public void test18() throws Exception{
        // Using another query when no match for the main one
        QueryBuilder qb = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("tag", "wow"),
                "index1", "index2"
        ).noMatchQuery(QueryBuilders.termQuery("tag", "kow"));

        // Using all (match all) or none (match no documents)
        QueryBuilder qb1 = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("tag", "wow"),
                "index1", "index2"
        ).noMatchQuery("all");
    }

    /**
     * Match All Query   匹配所有字段,相当于select * from
     * @throws Exception
     */
    @Test
    public void test19() throws Exception{
        QueryBuilder qb = QueryBuilders.matchAllQuery();
    }

    /**
     * Match Query: 可接受文字、数字和日期等类型的数据
     * @throws Exception
     */
    @Test
    public void test20() throws Exception{
        QueryBuilder qb = QueryBuilders.matchQuery(
                "name",
                "kimchy elasticsearch"
        );
    }

    /**
     * More Like This Query (mlt)  查询得到与所提供的文本相似的文档
     * @throws Exception
     */
    @Test
    public void test21() throws Exception{
        String[] fields = {"name.first", "name.last"};
                String[] texts = {"text like this one"};
                MultiGetRequest.Item[] items = null;

//        QueryBuilder qb = QueryBuilders.moreLikeThisQuery(fields, texts, items)
//                .minTermFreq(1)
//        .maxQueryTerms(12);
    }

    /**
     *  Multi Match Query  ： 在多个字段中检索
     * @throws Exception
     */
    @Test
    public void test22() throws Exception{
        QueryBuilder qb = QueryBuilders.multiMatchQuery(
                "kimchy elasticsearch",
                "user", "message"
        );
    }

    /**
     * Nested Query
     * @throws Exception
     */
    @Test
    public void test23() throws Exception{
//        QueryBuilder qb = QueryBuilders.nestedQuery(
//                "obj1",
//                QueryBuilders.boolQuery()
//        .must(QueryBuilders.matchQuery("obj1.name", "blue"))
//                .must(QueryBuilders.rangeQuery("obj1.count").gt(5))
//        )
//        .scoreMode(QueryBuilders.ScoreMode.Avg);
    }

    /**
     * Prefix Query: 查询找到某个字段前缀开头的
     * @throws Exception
     */
    @Test
    public void test24() throws Exception{
        QueryBuilder qb = QueryBuilders.prefixQuery(
                "brand",
                "heine"
        );
    }

    /**
     * Query String Query  : 查询对查询文本分析后构建一个短语查询 其中的slop参数定义了在查询文本的词项之间应该隔几个才成功
     * @throws Exception
     */
    @Test
    public void test25() throws Exception{
        QueryBuilder qb = QueryBuilders.queryStringQuery("+kimchy -elasticsearch");
    }


    /**
     * Range Query:指定查询范围
     * @throws Exception
     */
    @Test
    public void test26() throws Exception{
        QueryBuilder qb = QueryBuilders.rangeQuery("price")
        .from(5)
        .to(10)
        .includeLower(true)//是否包含边界
        .includeUpper(false);

        // A simplified form using gte, gt, lt or lte
        QueryBuilder qb1 = QueryBuilders.rangeQuery("age")
        .gte("10")
        .lt("20");
    }

    /**
     * Regexp Query
     * @throws Exception
     */
    @Test
    public void test27() throws Exception{
        QueryBuilder qb = QueryBuilders.regexpQuery(
                "name.first",
                "s.*y");
    }

    /**
     * Script Query
     * @throws Exception
     */
    @Test
    public void test28() throws Exception{
        QueryBuilder qb = QueryBuilders.scriptQuery(
                new Script("doc['num1'].value > 1")
        );

        QueryBuilder qb1 = QueryBuilders.scriptQuery(
                new Script(
                        "mygroovyscript",
                        ScriptService.ScriptType.FILE,
                "groovy",
                ImmutableMap.of("param1", 5))
        );
    }

    /**
     * Simple Query String Query
     */
    @Test
    public void test29() throws Exception{
        QueryBuilder qb = QueryBuilders.simpleQueryStringQuery("+kimchy -elasticsearch");
    }

    /**
     * Span Containing Query
     * @throws Exception
     */
    @Test
    public void test30() throws Exception{
//        QueryBuilder qb = QueryBuilders.spanContainingQuery(
//                QueryBuilders.spanNearQuery(QueryBuilders.spanTermQuery("field1","bar"), 5)
//                .clause( QueryBuilders.spanTermQuery("field1","baz"))
//                .inOrder(true),
//                QueryBuilders.spanTermQuery("field1","foo"));
    }

    /**
     * Span First Query
     * @throws Exception
     */
    @Test
    public void test31() throws Exception{
        QueryBuilder qb = QueryBuilders.spanFirstQuery(
                QueryBuilders.spanTermQuery("user", "kimchy"),3
        );
    }

    /**
     * Span Multi Term Query
     * @throws Exception
     */
    @Test
    public void test32() throws Exception{
        QueryBuilder qb = QueryBuilders.spanMultiTermQueryBuilder(
                QueryBuilders.prefixQuery("user", "ki")
        );
    }

    /**
     * Span Near Query
     * @throws Exception
     */
    @Test
    public void test33() throws Exception{
//        QueryBuilder qb = QueryBuilders.spanNearQuery(
//                QueryBuilders.spanTermQuery("field","value1"),12)
//        .clause(QueryBuilders.spanTermQuery("field","value2"))
//        .clause(QueryBuilders.spanTermQuery("field","value3"))
//        .inOrder(false)
//        .collectPayloads(false);
    }

    /**
     * Span Not Query
     */
    @Test
    public void test34() throws Exception{
//        QueryBuilder qb = QueryBuilders.spanNotQuery(
//                QueryBuilders.spanTermQuery("field","value1"),
//                QueryBuilders.spanTermQuery("field","value2"));
    }

    /**
     * Span Or Query
     */
    @Test
    public void test35() throws Exception{
//        QueryBuilder qb = QueryBuilders.spanOrQuery(
//                QueryBuilders.spanTermQuery("field","value1"))
//        .clause(QueryBuilders.spanTermQuery("field","value2"))
//        .clause(QueryBuilders.spanTermQuery("field","value3"));
    }


    /**
     * Span Term Query
     */
    @Test
    public void test36() throws Exception{
        QueryBuilder qb = QueryBuilders.spanTermQuery(
                "user",
                "kimchy"
        );
    }

    /**
     * Span Within Query
     */
    @Test
    public void test37() throws Exception{
//        QueryBuilder qb = QueryBuilders.spanWithinQuery(
//                QueryBuilders.spanNearQuery(QueryBuilders.spanTermQuery("field1", "bar"), 5)
//                .clause(QueryBuilders.spanTermQuery("field1", "baz"))
//                .inOrder(true),
//        QueryBuilders.spanTermQuery("field1", "foo"));
    }

    /**
     *  Template Query
     */
    @Test
    public void test38() throws Exception{
        client.preparePutIndexedScript("mustache", "template_gender",
                "{\n" +
                        "    \"template\" : {\n" +
                        "        \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "                \"gender\" : \"{{param_gender}}\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}").get();


//        QueryBuilder qb = QueryBuilders.templateQuery(
//                "gender_template",
//                ScriptService.ScriptType.INDEXED,
//                template_params);
    }

    /**
     *  Term Query: term 查询仅匹配给定字段有某个词项的文档
     *  若要提升重要性，要加boost
     */
    @Test
    public void test39() throws Exception{
        QueryBuilder qb = QueryBuilders.termQuery(
                "name",
                "kimchy"
        );
    }

    /**
     * Terms Query: terms 查询允许包含某些词
     */
    @Test
    public void test40() throws Exception{
        QueryBuilder qb = QueryBuilders.termsQuery("tags",
                "blue", "pill");
    }

    /**
     * Type Query
     * @throws Exception
     */
    @Test
    public void test41() throws Exception{
        QueryBuilder qb = QueryBuilders.typeQuery("my_type");
    }

    /**
     * Wildcard Query： 允许查询内容使用通配符
     */
    @Test
    public void test42() throws Exception{
        QueryBuilder qb = QueryBuilders.wildcardQuery("user", "k?mc*");
    }

    public XContentBuilder jsonBuilder()throws Exception{
        XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
        docBuilder.field("doc").startObject(); //This is needed to designate the document
        docBuilder.field("content", "This is amazing!");
        docBuilder.endObject(); //End of the doc field
        docBuilder.endObject(); //End of the JSON root object
//Percolate
//        PercolateResponse response = client.preparePercolate()
//                .setIndices("myIndexName")
//                .setDocumentType("myDocumentType")
//                .setSource(docBuilder).execute().actionGet();
//Iterate over the results
//        for(PercolateResponse.Match match : response) {
//            //Handle the result which is the name of
//            //the query in the percolator
//        }
        return  docBuilder;
    }


}
