import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.Before;
import org.junit.Test;
//import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by Jack on 2015/12/22.
 */
public class test3 {

    Client client;
    @Before
    public void before() throws UnknownHostException {
        client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法
    }
    /**
     *  Bulk API
     * @throws Exception
     */
    @Test
    public void test1() throws Exception{
        BulkRequestBuilder bulkRequest = client.prepareBulk();

// either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new FieldStats.Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
        );

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "another post")
                        .endObject()
                )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
    }

    /**
     * Delete API
     * @throws Exception
     */
    @Test
    public void test2() throws Exception{
//        DeleteResponse response = client.prepareDelete("twitter", "tweet", "1")
//                .setOperationThreaded(false)
//                .get();
    }

    /**
     * Get API
     * @throws Exception
     */
    @Test
    public void test3() throws Exception{
        GetResponse response = client.prepareGet("twitter", "tweet", "1")
                .setOperationThreaded(false)
                .get();
    }

    /**
     * Index API
     * @throws Exception
     */
    @Test
    public void test4() throws Exception{
        IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
                .get();

        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        IndexResponse response1 = client.prepareIndex("twitter", "tweet")
                .setSource(json)
                .get();
    }

    /**
     *  Multi Get API
     */
    @Test
    public void test5() throws Exception{
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("twitter", "tweet", "1")
        .add("twitter", "tweet", "2", "3", "4")
        .add("another", "type", "foo")
        .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                    String json = response.getSourceAsString();
            }
        }
    }


    /**
     * Update API
     */
    @Test
    public void test6() throws Exception{
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("index");
        updateRequest.type("type");
        updateRequest.id("1");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("gender", "male")
                .endObject());
        client.update(updateRequest).get();

        client.prepareUpdate("ttl", "doc", "1")
                .setScript(new Script("ctx._source.gender = \"male\""  , ScriptService.ScriptType.INLINE, null, null))
                .get();

        client.prepareUpdate("ttl", "doc", "1")
                .setDoc(jsonBuilder()
                .startObject()
                .field("gender", "male")
                .endObject())
        .get();

        UpdateRequest updateRequest1 = new UpdateRequest("ttl", "doc", "1")
                .script(new Script("ctx._source.gender = \"male\""));
        client.update(updateRequest).get();

        UpdateRequest updateRequest3 = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject());
        client.update(updateRequest).get();

        IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "Joe Smith")
                        .field("gender", "male")
                        .endObject());
        UpdateRequest updateRequest4 = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);
                client.update(updateRequest).get();


    }

}
