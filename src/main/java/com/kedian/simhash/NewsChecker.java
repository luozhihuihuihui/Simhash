package com.kedian.simhash;


import com.kedian.util.ESDBUtil;
import net.sf.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: luozhihui
 * @Description:
 * @Date: Create in 14:40 2019/9/6
 */
public class NewsChecker {
    private static final long MINUTE = 900000L;
    private static final long EIGHT = 28800000L;

    private static String ES_IP = "192.168.4.171";
    private static TransportClient client;
    private final static String STORE_PATH = "F:\\newssimhash";
    private final static int BIT_LENGTH = 64;
    private final static long MASK0 = 0xFFFF;
    private final static long MASK1 = 0xFFF;
    private static IndexWriter writer = null;
    private Directory dir = null;
    private IndexWriterConfig config = null;
    private Analyzer analyzer = new StandardAnalyzer();

    public NewsChecker() throws IOException {
        dir = FSDirectory.open(Paths.get(STORE_PATH));
        config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writer = new IndexWriter(dir, config);
    }

    public static void newsSimhash() throws IOException {
        while (true) {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long lteTime = System.currentTimeMillis() + EIGHT;
            long gtTime = lteTime - MINUTE - 5000L;
            long time = System.currentTimeMillis();
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("insert_time").gt(gtTime).lte(lteTime);
            SearchRequestBuilder searchRequestBuilder = ESDBUtil.client.prepareSearch("news")
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
            System.out.println("Read es cast time------>" + (System.currentTimeMillis() - time));
            time = System.currentTimeMillis();
            NewsChecker newsChecker = null;
            try {
                newsChecker = new NewsChecker();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (SearchHit hit : hitsList) {
                JSONObject doc = JSONObject.fromObject(hit.getSourceAsString());
                String articlesId = hit.getId();
                String content = (String) doc.get("content");
                try {
                    long simhash = SimHash.getSimHash(content);
                    if (newsChecker != null) {
                        ArrayList<String> strings = newsChecker.doFilte(simhash);

                        if (strings.size() != 0) {
                            String similarity = strings.get(0).split(":")[1].replace(">", "");
                            if (articlesId.compareTo(similarity) == 0) {
                                continue;
                            }
                            ESDBUtil.updateSimhash(articlesId, simhash, similarity, "news");
                        } else {
                            ESDBUtil.updateSimhash(articlesId, simhash, "", "news");
                            newsChecker.addDocument(content, articlesId);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    writer.close();
                    return;
                }
            }
            System.out.println("addDocument cast time------>" + (System.currentTimeMillis() - time));
            writer.close();
            try {
                System.out.println(df.format(lteTime) + "  start to  sleep");
                Thread.sleep(MINUTE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        newsSimhash();
//
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        long gtTime = ESDBUtil.monthTimeInMillis(0);
//        long lteTime = gtTime + EIGHT * 6;
//        while (lteTime <= ESDBUtil.monthTimeInMillis(1) + EIGHT) {
//            long time = System.currentTimeMillis();
//            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("insert_time").gt(gtTime).lte(lteTime);
//            SearchRequestBuilder searchRequestBuilder = ESDBUtil.client.prepareSearch("news")
//                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                    .setQuery(rangeQueryBuilder)
//                    .setSize(10000)
//                    .setFetchSource(new String[]{"content"}, null);
//            searchRequestBuilder.setScroll(new TimeValue(60000));
//            SearchResponse searchResponse = searchRequestBuilder.get();
//            String scrollId = searchResponse.getScrollId();
//            SearchHit[] searchHits = searchResponse.getHits().getHits();
//            List<SearchHit> hitsList = new ArrayList<>();
//            hitsList.addAll(Arrays.asList(searchHits));
//            hitsList.addAll(ESDBUtil.searchByScrollId(ESDBUtil.client, scrollId));
//            System.out.println(df.format(gtTime) + "--->" + df.format(lteTime) + "  size:" + hitsList.size() + "   timstamp:" + gtTime);
//            System.out.println("Read es cast time------>" + (System.currentTimeMillis() - time));
//            time = System.currentTimeMillis();
//            NewsChecker newsChecker = null;
//            try {
//                newsChecker = new NewsChecker();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            for (SearchHit hit : hitsList) {
//                JSONObject doc = JSONObject.fromObject(hit.getSourceAsString());
//                String articlesId = hit.getId();
//                String content = (String) doc.get("content");
//                try {
//                    long simhash = SimHash.getSimHash(content);
//                    if (newsChecker != null) {
//                        ArrayList<String> strings = newsChecker.doFilte(simhash);
//
//                        if (strings.size() != 0) {
//                            String similarity = strings.get(0).split(":")[1].replace(">", "");
//                            if (articlesId.compareTo(similarity) == 0) {
//                                continue;
//                            }
//                            ESDBUtil.updateSimhash(articlesId, simhash, similarity, "news");
//                        } else {
//                            ESDBUtil.updateSimhash(articlesId, simhash, "", "news");
//                            newsChecker.addDocument(content, articlesId);
//                        }
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    writer.close();
//                    return;
//                }
//            }
//            System.out.println("addDocument cast time------>" + (System.currentTimeMillis() - time));
//            writer.close();
//            gtTime = lteTime;
//            lteTime += EIGHT * 8;      }
    }


    public void addDocument(String text, String id) throws IOException {
        long hash = SimHash.getSimHash(text);
        Document doc = new Document();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                String filedName = "hash" + i + j;
                final long mask = getMask(i, j);
                doc.add(new StringField(filedName, Long.toString(hash & mask), Field.Store.NO));
            }
        }
        doc.add(new StoredField("hash", Long.toString(hash)));
        doc.add(new StoredField("articlesId", id));
        writer.addDocument(doc);
    }

    public ArrayList<String> doFilte(long hash) {
        ArrayList<String> matchPool = new ArrayList<String>();
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                String fieldName = "hash" + i + j;
                long mask = getMask(i, j);
                final long newHash = hash & mask;
                try {
                    ArrayList<Document> exactMatch = match(newHash, fieldName);
                    for (Document doc : exactMatch) {
                        String content = doc.getField("hash").stringValue();
                        long h = Long.parseLong(content);
                        if (SimHash.hamming(hash, h) <= 3) {
                            String temp = doc.getField("articlesId").toString();
                            if (!matchPool.contains(temp)) {
                                matchPool.add(temp);
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return matchPool;
    }

    public ArrayList<Document> match(long hash, String fieldName) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(STORE_PATH));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new TermQuery(new Term(fieldName, Long.toString(hash)));
        TopDocs topDocs = searcher.search(query, 10);
        long hitCount = topDocs.totalHits.value;
        //int hitCount = 50;hitCount

        topDocs = searcher.search(query, 20);
        ArrayList<Document> result = new ArrayList<Document>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            result.add(searcher.doc(scoreDoc.doc));
        }
        return result;
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
