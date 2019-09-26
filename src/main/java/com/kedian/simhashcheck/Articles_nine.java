package com.kedian.simhashcheck;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kedian.simhash.ElasticSimhash;
import com.kedian.util.ESDBUtil;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;


/**
 * @Author: luozhihui
 * @Description:
 * @Date: Create in 13:52 2019/9/24
 */
public class Articles_nine {

    private static final long MINUTE = 900000L;
    private static final long EIGHT = 28800000L;
    private static final long DAY = 86400000L;

    public static void main(String[] args) {
        Articles_nine articles_nine = new Articles_nine();
        articles_nine.start();
    }

    public void start() {
        String index = "articles201909";
        long startTime = ESDBUtil.monthTimeInMillis(0)+EIGHT;
        long endTime = startTime + DAY;
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("simhash-pool-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<Runnable>(20), factory, new ThreadPoolExecutor.AbortPolicy());
        List<Future> futures = new LinkedList<>();
        for (int i = 5; i < 10; i++) {
            Callable myTast = new Task(index, startTime + i * DAY, endTime + i * DAY, MINUTE);
            try {
                futures.add(pool.submit(myTast));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        List<String> result = new ArrayList<>();
        futures.stream().forEach(future -> {
            try {
                result.add((String) future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        System.out.println(result);
    }

    class Task implements Callable {
        private String index;
        private long startTime;
        private long endTime;
        private long interval;

        public Task(String index, long startTime, long endTime, long interval) {
            this.index = index;
            this.startTime = startTime;
            this.endTime = endTime;
            this.interval = interval;
        }

        @Override
        public Object call() throws Exception {
            System.out.println(Thread.currentThread().getName() + "------>start");
            ElasticSimhash elasticSimhash = new ElasticSimhash("articles201909", startTime, endTime, MINUTE);
            long timstamp = elasticSimhash.start();
            return Thread.currentThread().getName() + ":" + timstamp;
        }

    }

}
