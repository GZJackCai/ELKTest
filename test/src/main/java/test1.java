import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

/**
 * Created by Jack on 2015/12/21.
 */
public class test1 {


    /**
     * 索引json数据,2.x新语法
     */
    @Test
    public void test1() throws Exception{

    }


    /**
     * 基于transport方式初始化
     */
    @Test
    public void test2() throws Exception{
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法
        Map<String,Object> map= Maps.newHashMap();
        map.put("name","hebust");
        map.put("age",23);
        map.put("content","hello ,world");
        map.put("haa",new String[]{"big data","minig","information retrival"});
        String s=new Gson().toJson(map);
        System.out.println(s);
        IndexResponse response=client.prepareIndex("myweibo1","my1").setSource(s).execute().actionGet();

        String _index=response.getIndex();//得到index名称
        String _type=response.getType();//得到type 名称
        String _id=response.getId();//获取文档id
        long _version=response.getVersion();//如果首次检索该文档，则值为1

        System.out.println(_id+":"+_index+":"+_type+":"+_version);
    }

    //获取文档信息
    @Test
    public void test3()throws Exception{
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法

        GetResponse response=client.prepareGet("myweibo1","my1","AVHE3qVDs7-P1naLpriP").execute().actionGet();
        System.out.println(response.getSource());//显示针对该条的数据细节

    }

    //删除文档信息
    @Test
    public  void test4() throws Exception{
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法

        DeleteResponse response=client.prepareDelete("myweibo1","my1","AVHE1Vons7-P1naLprVE").execute().actionGet();
        boolean isFound=response.isFound();
        System.out.println(isFound);
        System.out.println(response.getHeaders());//返回响应头
        System.out.println("指定的id已经被删除");
    }

    //更新索引文档信息,没通过测试
    @Test
    public  void test5() throws Exception{
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法

        UpdateRequest updateRequest=new UpdateRequest();
        updateRequest.index("myweibo1");//索引名
        updateRequest.type("my1");//类型名称
        updateRequest.id("AVHE3qVDs7-P1naLpriP");

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                .startObject();
        updateRequest.doc(contentBuilder.startObject().field("name","h").endObject());
        client.update(updateRequest).get();
    }

    //简单的统计
    @Test
    public  void test6() throws Exception{
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));//初始化es,2.x语法

        CountResponse response=client.prepareCount("myweibo1")
                .setQuery(QueryBuilders.termQuery("name","hebust")).execute().actionGet();
        System.out.println(response.getCount());
    }
}
