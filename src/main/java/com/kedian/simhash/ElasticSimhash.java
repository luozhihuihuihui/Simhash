package com.kedian.simhash;


import com.kedian.util.ESDBUtil;
import net.sf.json.JSONObject;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @Author: luozhihui
 * @Description:
 * @Date: Create in 14:40 2019/9/6
 */
public class ElasticSimhash {
    private final static int BIT_LENGTH = 64;
    private final static long MASK0 = 0xFFFF;
    private final static long MASK1 = 0xFFF;
    private String INDEX;
    private long startTime;
    private long endTime;
    private long interval;

    /**
     * @description startTime:开始时间 endTime:结束时间
     * @author zhihuiLuo
     * @date 2019/9/24 13:56
     */
    public ElasticSimhash(String index, long startTime, long endTime, long interval) {
        this.INDEX = index;
        this.startTime = startTime;
        this.endTime = endTime;
        this.interval = interval;
    }

    public long start() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long gtTime = startTime;
        long lteTime = gtTime + interval;
        try {
            while (lteTime <= endTime) {
                long time = System.currentTimeMillis();
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("insert_time").gt(gtTime).lte(lteTime);
                SearchRequestBuilder searchRequestBuilder = ESDBUtil.client.prepareSearch(INDEX)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setQuery(rangeQueryBuilder)
                        .setSize(10000)
                        .setFetchSource(new String[]{"content"}, null);
                searchRequestBuilder.setScroll(new TimeValue(60000));
                SearchResponse searchResponse = searchRequestBuilder.get();
                String scrollId = searchResponse.getScrollId();
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                List<SearchHit> hitsList = new ArrayList<>();
                hitsList.addAll(Arrays.asList(searchHits));
                hitsList.addAll(ESDBUtil.searchByScrollId(ESDBUtil.client, scrollId));
                System.out.println(df.format(gtTime) + "--->" + df.format(lteTime) + "  size:" + hitsList.size() + "   timstamp:" + gtTime);
                System.out.println(INDEX + ":  Read es cast time------>" + (System.currentTimeMillis() - time));
                ESDBUtil.clearScroll(ESDBUtil.client, scrollId);
                time = System.currentTimeMillis();
                for (SearchHit hit : hitsList) {
                    JSONObject doc = JSONObject.fromObject(hit.getSourceAsString());
                    String articlesId = hit.getId();
                    String content = (String) doc.get("content");
                    try {
                        long simhash = SimHash.getSimHash(content);
                        ArrayList<String> strings = doFilte(simhash);
                        if (strings.size() != 0) {
                            String similarity = strings.get(0);
                            if (articlesId.compareTo(similarity) == 0) {
                                continue;
                            }
                            ESDBUtil.updateSimhash(articlesId, simhash, similarity, INDEX);
                        } else {
                            ESDBUtil.updateSimhash(articlesId, simhash, "", INDEX);
                            addDocument(content, articlesId);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(INDEX + ":  addDocument cast time------>" + (System.currentTimeMillis() - time));
                gtTime = lteTime;
                lteTime += interval;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return startTime;
        }
    }

    public void addDocument(String text, String id) throws IOException {
        XContentBuilder xContentBuilder = jsonBuilder().startObject();
        long hash = SimHash.getSimHash(text);
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                String filedName = "hash" + i + j;
                final long mask = getMask(i, j);
                xContentBuilder.field(filedName, Long.toString(hash & mask));
            }
        }
        xContentBuilder.field("hash", Long.toString(hash));
        xContentBuilder.field("articlesId", id);
        xContentBuilder.endObject();
        ESDBUtil.addSimhah(xContentBuilder, id);
    }

    public ArrayList<String> doFilte(long hash) {
        ArrayList<String> matchPool = new ArrayList<String>();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                String fieldName = "hash" + i + j;
                long mask = getMask(i, j);
                final long newHash = hash & mask;
                try {
                    List<Map<String, Object>> exactMatch = ESDBUtil.getSimhash(newHash, fieldName);
                    for (Map<String, Object> doc : exactMatch) {
                        String content = (String) doc.get("hash");
                        long h = Long.parseLong(content);
                        if (SimHash.hamming(hash, h) <= 3) {
                            String temp = doc.get("articlesId").toString();
                            if (!matchPool.contains(temp)) {
                                matchPool.add(temp);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return matchPool;
    }

    private long getMask(int i, int j) {
        long mask = MASK0 << (i * 16);
        int r = (BIT_LENGTH - (i + 1) * 16) % 12;
        int pos = (i + 1) * 16 + j * 12;
        if (pos <= BIT_LENGTH) {
            if (pos + 12 <= BIT_LENGTH) {
                mask |= MASK1 << ((i + 1) * 16 + j * 12);
            } else {
                final long L1 = 1;
                for (int k = BIT_LENGTH - 1; k > BIT_LENGTH - 1 - r; k--) {
                    mask |= L1 << k;
                }
                assert (r < 12);
                for (int k = 0; k < 12 - r; k++) {
                    mask |= L1 << k;
                }
            }
        } else {
            int p = pos - BIT_LENGTH;
            mask |= MASK1 << p;
        }
        return mask;
    }
}
