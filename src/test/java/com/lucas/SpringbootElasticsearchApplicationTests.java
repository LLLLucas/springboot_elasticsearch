package com.lucas;

import com.alibaba.fastjson.JSON;
import com.lucas.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.omg.CORBA.ServerRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.naming.directory.SearchResult;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * es7.8的高级客户端测试api
 */
@SpringBootTest
class SpringbootElasticsearchApplicationTests {
    @Autowired
    @Qualifier("restHighLevelClient")        //写上对应的client的方法名称
    private RestHighLevelClient client;


    //测试索引的创建
    @Test
    public void testCreateIndex()throws Exception{
        //1、创建索引请求
        CreateIndexRequest lucas_index = new CreateIndexRequest("lucas_index");
        //2、执行请求,请求后获得响应
        CreateIndexResponse createIndexResponse = client.indices().create(lucas_index, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    //测试索引是否存在
    @Test
    public void testExistIndex()throws Exception{
        GetIndexRequest request=new GetIndexRequest("lucas_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);//返回true即存在
    }
    //测试删除索引
    @Test
    public void testDeleteIndex()throws Exception{
        DeleteIndexRequest request = new DeleteIndexRequest("lucas_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //测试添加文档
    @Test
    public void testAddDocument()throws Exception{
        //创建对象
        User user = new User("卢航", 23);
        //创建请求
        IndexRequest request = new IndexRequest("lucas_index");
        //规则 put/lucas_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        //将数据放入请求
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }

    //测试是否存在文档1
    @Test
    public void testGetDocument()throws Exception{
        GetRequest getRequest = new GetRequest("lucas_index","1");

        //不获取返回的_source的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);

    }

    //获取文档的信息
    @Test
    void getDocument()throws Exception{
        GetRequest getRequest=new GetRequest("lucas_index","1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        String sourceAsString = getResponse.getSourceAsString();
        System.out.println(sourceAsString);     //打印 文档
        System.out.println(getResponse);        //打印获得的所有内容
    }
    //更新文档内容
    @Test
    void updateDocument()throws Exception{
        UpdateRequest request = new UpdateRequest("lucas_index", "1");
        request.timeout("1s");
        User user = new User("卢航学java", 1);
        request.doc(JSON.toJSONString(user),XContentType.JSON);

        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update.status());

    }
    //删除文档
    @Test
    void deleteDocument()throws Exception{
        DeleteRequest request = new DeleteRequest("lucas_index", "1");
        request.timeout("1s");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }
    //特殊的，真项目一般会批量插入数据！
    @Test
    void testBulkRequest()throws Exception{
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("lucas1",1));
        users.add(new User("lucas2",2));
        users.add(new User("lucas3",3));
        users.add(new User("lucas4",4));
        users.add(new User("lucas5",5));
        users.add(new User("lucas6",6));
        //批量处理请求
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("lucas_index")
                            .id(""+(i+1))
                            .source(JSON.toJSONString(users.get(i)),XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures()); //是否失败，如果false就是成功了

    }

    //查询
    @Test
    void testSearch()throws Exception{
        SearchRequest request=new SearchRequest("lucas_index");
        //构建搜索的条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //我们可以用QueryBuilders工具来实现
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "lucas1");//匹配的查询条件
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        request.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        String string = JSON.toJSONString(hits);
        System.out.println(string);
        System.out.println("==============");
        for (SearchHit hit : response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());

        }

    }

}
