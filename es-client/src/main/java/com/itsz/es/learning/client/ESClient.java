package com.itsz.es.learning.client;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

public class ESClient {

    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";

    public RestHighLevelClient getClient(String host, int port) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port));
        return new RestHighLevelClient(builder);
    }

    public Settings createSettings(int shards, int replicas) {
        return Settings.builder()
                .put(NUMBER_OF_SHARDS, shards)
                .put(NUMBER_OF_REPLICAS, replicas)
                .build();
    }

    public XContentBuilder createMappings() throws IOException {
        return JsonXContent.contentBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "text")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .startObject("birthday")
                .field("type", "date")
                .field("format", "yyyy-MM-dd")
                .endObject()
                .endObject();
    }

    public CreateIndexRequest createIndexRequest(String index, String type, int shards, int replica) throws IOException {
        return new CreateIndexRequest(index)
                .settings(createSettings(shards, replica))
                .mapping(type, createMappings());

    }

    public CreateIndexResponse createIndex(String index, String type, int shards, int replica, String host, int port) throws IOException {
        RestHighLevelClient client = getClient(host, port);
        CreateIndexRequest indexRequest = createIndexRequest(index, type, shards, replica);
        return client.indices().create(indexRequest, RequestOptions.DEFAULT);
    }

    public GetIndexRequest createGetIndexRequest(String index) {
        return new GetIndexRequest().indices(index);
    }

    public boolean indexExist(String index, String host, int port) throws IOException {
        return getClient(host, port).indices().exists(new GetIndexRequest().indices(index), RequestOptions.DEFAULT);
    }

    public AcknowledgedResponse deleteIndex(String host, int port, String index) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        deleteIndexRequest.indices(index);

        RestHighLevelClient client = getClient(host, port);
        return client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
    }

    public IndexResponse createDoc(String host, int port, String index, String json) throws IOException {
        IndexRequest request = new IndexRequest(index, "man", "1");
        request.source(json, XContentType.JSON);

        RestHighLevelClient client = getClient(host, port);
        return client.index(request, RequestOptions.DEFAULT);

    }

    public UpdateResponse updateDoc(String host, int port, String index, String type, Map<String, Object> doc) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id("1");
        updateRequest.doc(doc);

        RestHighLevelClient client = getClient(host, port);
        return client.update(updateRequest, RequestOptions.DEFAULT);
    }

    public DeleteResponse deleteDoc(String host, int port, String index, String type, String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);

        RestHighLevelClient client = getClient(host, port);

        return client.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse searchDoc(String host, int port, String index, String type, String searchValue) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(0);
        builder.size(5);
        builder.query(QueryBuilders.termQuery("name", searchValue));
        //Terms查询
        //builder.query(QueryBuilders.termsQuery("name", searchValue));

        //匹配查询
        // builder.query(QueryBuilders.matchAllQuery());

        //布尔 match查询
        // builder.query(QueryBuilders.matchQuery("name", "红楼 梦").operator(Operator.AND));

        //multi match
        // builder.query(QueryBuilders.multiMatchQuery(searchValue, "name", "author"));

        //query by ids
        // builder.query(QueryBuilders.idsQuery().addIds("1", "m_3_d3QB-KIjfuldzqA7"));

        //query by prefix
        // builder.query(QueryBuilders.prefixQuery("author", searchValue));

        //fuzzy query
        // builder.query(QueryBuilders.fuzzyQuery("author", searchValue));

        //wildCard query
        //builder.query(QueryBuilders.wildcardQuery("author", "曹??"));

        //range query
        // builder.query(QueryBuilders.rangeQuery("count").gte(10).lte(30));

        //regexp query
        //builder.query(QueryBuilders.regexpQuery("mobile","182[0-9](8)"));

        //geo query
        builder.query(QueryBuilders.geoDistanceQuery("location").point(49.12312, 23.23423).distance("1222"));
        searchRequest.source(builder);
        RestHighLevelClient client = getClient(host, port);

        return client.search(searchRequest, RequestOptions.DEFAULT);

    }

    public GetResponse searchById(String host, int port, String index, String type, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id);

        RestHighLevelClient client = getClient(host, port);

        return client.get(getRequest, RequestOptions.DEFAULT);


    }

    public SearchResponse scrollSearch(String index, String type, String host, int port) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(2);
        builder.sort("on_sale", SortOrder.DESC);
        builder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(builder);
        searchRequest.scroll(TimeValue.timeValueMinutes(5));

        RestHighLevelClient client = getClient(host, port);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse scrollSearchById(String host, int port, String scrollId) throws IOException {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);

        scrollRequest.scroll(TimeValue.timeValueMinutes(5));

        RestHighLevelClient client = getClient(host, port);

        return client.scroll(scrollRequest, RequestOptions.DEFAULT);

    }

    public ClearScrollResponse clearScrollId(String host, int port, String scrollId) throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);

        RestHighLevelClient client = getClient(host, port);
        return client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse boolSearch(String host, int port, String index, String type) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(QueryBuilders.termQuery("name", "红楼梦"));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("author", "曹雪芹"));
        boolQueryBuilder.must(QueryBuilders.termQuery("count", 1));
        sourceBuilder.query(boolQueryBuilder);
        searchRequest.source(sourceBuilder);

        RestHighLevelClient client = getClient(host, port);
        return client.search(searchRequest, RequestOptions.DEFAULT);

    }

    public SearchResponse boostingSearch(String host, int port, String index, String type) throws IOException {

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        QueryBuilder positiveQueryBuilder = QueryBuilders.matchQuery("name", "jeremy");
        QueryBuilder nativeQueryBuilder = QueryBuilders.matchQuery("author", "honey");


        BoostingQueryBuilder boostingQueryBuilder = new BoostingQueryBuilder(positiveQueryBuilder, nativeQueryBuilder);
        boostingQueryBuilder.negativeBoost(0.5f);

        sourceBuilder.query(boostingQueryBuilder);
        searchRequest.source(sourceBuilder);

        RestHighLevelClient client = getClient(host, port);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse filterSearch(String host, int port, String index, String type) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(QueryBuilders.termQuery("name", "红楼梦"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("count").gt(1).lt(10));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);

        searchRequest.source(sourceBuilder);

        RestHighLevelClient client = getClient(host, port);

        return client.search(searchRequest, RequestOptions.DEFAULT);

    }

    public SearchResponse highLightSearch(String host, int port, String index, String type) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("desc", "华莱");

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("desc");
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.fragmentSize(100);

        sourceBuilder.query(matchQueryBuilder);
        sourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(sourceBuilder);
        return getClient(host, port).search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse aggregateSearch(String host, int port, String index, String type) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.cardinality("agg").field("author");

        //max, min , avg
        AggregationBuilder extendedStatsAggregationBuilder = AggregationBuilders.extendedStats("agg").field("count");

        searchSourceBuilder.aggregation(aggregationBuilder);

        searchRequest.source(searchSourceBuilder);

        RestHighLevelClient client = getClient(host, port);

        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse aggregateRangeSearch(String host, int port, String index, String type) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.range("agg")
                .field("count")
                .addUnboundedTo(10)
                .addRange(11, 15)
                .addUnboundedTo(90);
        //data range
        AggregationBuilder dateAggregationBuilder = AggregationBuilders.dateRange("agg")
                .format("yyyy-MM-dd")
                .addRange("1987-01-01", "2020-09-11");

        //ip range
        AggregationBuilder ipRangeAggregationBuilder = AggregationBuilders.ipRange("egg")
                .addRange("192.168.1.1", "192.168.1.12");


        searchSourceBuilder.aggregation(aggregationBuilder);

        searchRequest.source(searchSourceBuilder);

        RestHighLevelClient client = getClient(host, port);

        return client.search(searchRequest, RequestOptions.DEFAULT);
    }
}
