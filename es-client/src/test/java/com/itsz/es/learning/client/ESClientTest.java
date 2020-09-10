package com.itsz.es.learning.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ESClientTest {

    ESClient esClient = new ESClient();

    @Test
    void testGetClient() {
        String host = "192.168.33.12";
        int port = 9200;
        RestHighLevelClient restHighLevelClient = esClient.getClient(host, port);
        assertNotNull(restHighLevelClient);
    }

    @Test
    void testCreateSettings() {
        int shards = 1;
        int replicas = 1;
        Settings settings = esClient.createSettings(shards, replicas);
        assertNotNull(settings);
    }

    @Test
    void testCreateMappings() throws IOException {
        XContentBuilder mappings = esClient.createMappings();
        assertNotNull(mappings);
    }

    @Test
    void testCreateIndexRequest() throws IOException {
        String index = "person";
        String type = "man";
        CreateIndexRequest indexRequest = esClient.createIndexRequest(index, type, 1, 1);
        assertNotNull(indexRequest);
    }

    @Test
    void testCreateIndex() throws IOException {
        String index = "person";
        String type = "man";
        CreateIndexResponse response = esClient.createIndex(index, type, 1, 1, "192.168.33.12", 9200);
        assertNotNull(response);
        assertTrue(response.isAcknowledged());
        assertEquals("person", response.index());
    }

    @Test
    void testIndexExist() throws IOException {
        String index = "person";
        String host = "192.168.33.12";
        int port = 9200;
        assertTrue(esClient.indexExist(index, host, port));
    }

    @Test
    void testDeleteIndex() throws IOException {
        String index = "person";
        String host = "192.168.33.12";
        int port = 9200;

        AcknowledgedResponse acknowledgedResponse = esClient.deleteIndex(host, port, index);
        assertTrue(acknowledgedResponse.isAcknowledged());
    }


    @Test
    void testCreateDoc() throws IOException {
        String index = "person";
        String type = "man";
        String host = "192.168.33.12";
        int port = 9200;

        Person person = new Person("1", "zhangsan", 12, new Date());
        String json = JacksonUtil.object2String(person);

        IndexResponse response = esClient.createDoc(host, port, index, json);
        assertEquals(200, response.status().getStatus());
    }

    @Test
    void updateDoc() throws IOException {
        String index = "person";
        String type = "man";
        String host = "192.168.33.12";
        int port = 9200;

        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "zhang xiao san");

        UpdateResponse updateResponse = esClient.updateDoc(host, port, index, type, doc);
        assertEquals(200, updateResponse.status().getStatus());
    }

    @Test
    void deleteDoc() throws IOException {
        String index = "person";
        String type = "man";
        String host = "192.168.33.12";
        int port = 9200;
        String id = "1";

        DeleteResponse deleteResponse = esClient.deleteDoc(host, port, index, type, id);
        assertEquals(200, deleteResponse.status().getStatus());
    }

    @Test
    void testSearchDoc() throws IOException {
        String index = "book";
        String type = "novel";

        String host = "192.168.33.12";
        int port = 9200;
        String searchValue = "红";

        SearchResponse searchResponse = esClient.searchDoc(host, port, index, type, searchValue);
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.totalHits;
        assertEquals(1, totalHits);
    }

    @Test
    void testSearchById() throws IOException {
        String index = "book";
        String type = "novel";

        String host = "192.168.33.12";
        int port = 9200;
        String id = "m_3_d3QB-KIjfuldzqA7";

        GetResponse getResponse = esClient.searchById(host, port, index, type, id);
        assertEquals("{\"name\":\"红楼梦\",\"author\":\"曹雪芹\",\"count\":1000023,\"on_sale\":\"1988-01-01\",\"desc\":\"奥手动阀手动阀撒地方撒旦分为啊多发点是否为零杰拉德\"}\n",
                getResponse.getSourceAsString());
    }


    @Test
    void testSearchByScroll() throws IOException {
        String index = "book";
        String type = "novel";

        String host = "192.168.33.12";
        int port = 9200;

        SearchResponse searchResponse = esClient.scrollSearch(index, type, host, port);
        assertEquals(200, searchResponse.status().getStatus());

        String scrollId = searchResponse.getScrollId();
        System.out.println(scrollId);
        assertNotNull(scrollId);
    }

    @Test
    void testScrollSearchById() throws IOException {
        String scrollId = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAEYkWanNEQjNZNjRUZlNnLTFaYmxpMXUwUQ==";
        String host = "192.168.33.12";
        int port = 9200;

        SearchResponse searchResponse = esClient.scrollSearchById(host, port, scrollId);
        assertEquals(200, searchResponse.status().getStatus());
    }

    @Test
    void testClearScrollId() throws IOException {
        String scrollId = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAEqIWanNEQjNZNjRUZlNnLTFaYmxpMXUwUQ==";
        String host = "192.168.33.12";
        int port = 9200;

        ClearScrollResponse clearScrollResponse = esClient.clearScrollId(host, port, scrollId);
        assertEquals(200, clearScrollResponse.status().getStatus());
    }
}