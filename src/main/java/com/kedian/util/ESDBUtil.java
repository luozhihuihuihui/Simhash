package com.kedian.util;


import net.sf.json.JSONObject;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;


/**
 * mongoDB数据库公共方法
 *
 * @author caobw
 */
@SuppressWarnings("unchecked")
public class ESDBUtil {
    private String ES_IP = EsPool.getEsPool().getEsIp();
    private int ES_PORT = 9300;
    public static TransportClient client = getClient();
    public static String index = "simhashindex";

    private synchronized static TransportClient getClient() {
        if (client == null) {
            try {
                new ESDBUtil();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return client;
    }

    private ESDBUtil() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "caskd-cluster")
                .build();

        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(ES_IP), ES_PORT));
    }

    private String changeEsIp() {
        ES_IP = EsPool.getEsPool().getEsIp();
        return ES_IP;
    }

    public static void updateSimhash(String id, long simhash, String similarity, String index) throws IOException, ExecutionException, InterruptedException {
        TransportClient client = getClient();
        UpdateRequest updateRequest = new UpdateRequest();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if ("".equals(similarity)) {
            updateRequest.index(index);
            updateRequest.id(id);
            updateRequest.doc(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("simhash", String.valueOf(simhash))
                    .field("similarity", "")
                    .field("has_similarity", "false")
                    .endObject());
            client.update(updateRequest).get();

        } else {
            updateRequest.index(index);
            updateRequest.id(id);
            updateRequest.doc(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("simhash", String.valueOf(simhash))
                    .field("similarity", similarity)
                    .field("has_similarity", "true")
                    .endObject());
            client.update(updateRequest).get();


            updateRequest.index(index);
            updateRequest.id(similarity);
            updateRequest.doc(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("has_similarity", "true")
                    .endObject());
            client.update(updateRequest).get();
//            System.out.println("update------>articlesId:" + id + "            similarity:" + similarity + "    " + df.format(System.currentTimeMillis()));
        }
    }


    public static String dateToStamp(String s) throws Exception {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }


    public static void addSimhah(XContentBuilder xContentBuilder, String id) {
        IndexResponse response = null;
        response = client.prepareIndex(index, "doc")
                .setSource(xContentBuilder)
                .get();
        int status = response.status().getStatus();
        if (status != 201) {
            System.out.println("addSimhash wrong ------> " + id + TimeUtil.MINUTE.getTime());
        }
    }

    public static List<Map<String, Object>> getSimhash(long hash, String fieldName) {
        List<Map<String, Object>> result = new ArrayList<>();
        BoolQueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(fieldName + ".keyword", Long.toString(hash)));
        SearchResponse searchResponse = client.prepareSearch(index)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setSize(20)
                .setFetchSource(new String[]{fieldName, "articlesId"}, null)
                .setExplain(false)
                .get();
        SearchHits searchHits = searchResponse.getHits();
        for (SearchHit hit : searchHits) {
            Map<String, Object> map = new HashMap<>(4);
            JSONObject obj = JSONObject.fromObject(hit.getSourceAsString());
            map.put("hash", obj.get(fieldName));
            map.put("articlesId", obj.get("articlesId"));
            result.add(map);
        }
        return result;
    }

    /**
     * scrollID查询
     */
    public static List<SearchHit> searchByScrollId(Client client, String ScrollID) {
        TimeValue timeValue = new TimeValue(60000);
        SearchScrollRequestBuilder searchScrollRequestBuilder;
        SearchResponse response;
        List<SearchHit> result = new ArrayList<>();
        while (true) {
            searchScrollRequestBuilder = client.prepareSearchScroll(ScrollID);
            searchScrollRequestBuilder.setScroll(timeValue);
            response = searchScrollRequestBuilder.get();
            if (response.getHits().getHits().length == 0) {
                break;
            }
            SearchHit[] searchHits = response.getHits().getHits();
            result.addAll(Arrays.asList(searchHits));
            ScrollID = response.getScrollId();
        }

        return result;
    }

    public static Long monthTimeInMillis(int amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.YEAR, 0);
        calendar.add(Calendar.MONTH, amount);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Long time = calendar.getTimeInMillis();
        return time;
    }

    /**
     * 清除滚动ID
     *
     * @param client
     * @param scrollId
     * @return
     */
    public static boolean clearScroll(Client client, String scrollId) {
        ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll();
        clearScrollRequestBuilder.addScrollId(scrollId);
        ClearScrollResponse response = clearScrollRequestBuilder.get();
        return response.isSucceeded();
    }

}
