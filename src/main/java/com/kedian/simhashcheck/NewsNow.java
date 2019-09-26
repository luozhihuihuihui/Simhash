package com.kedian.simhashcheck;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kedian.simhash.ElasticSimhash;
import com.kedian.util.ESDBUtil;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: luozhihui
 * @Description:
 * @Date: Create in 15:31 2019/9/24
 */
public class NewsNow {
    private static final long MINUTE = 900000L;
    private static final long EIGHT = 28800000L;
    private static final long DAY = 86400000L;

    public static void main(String[] args) {
        NewsNow news = new NewsNow();
        news.start();
    }

    public void start() {
        String index = "news";
        long startTime = ESDBUtil.monthTimeInMillis(-3);
        ESDBUtil.index = "simhashnews";
        long endTime = startTime + DAY * 30;
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("simhash-pool-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<Runnable>(20), factory, new ThreadPoolExecutor.AbortPolicy());
        List<Future> futures = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            Callable myTast = new Task(index, startTime + i * DAY * 30, endTime + i * DAY * 30, DAY);
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
            ElasticSimhash elasticSimhash = new ElasticSimhash(index, startTime, endTime, interval);
            long timstamp = elasticSimhash.start();
            return Thread.currentThread().getName() + ":" + timstamp;
        }

    }

    class TaskThread implements Runnable {
        private String index;
        private long startTime;
        private long endTime;
        private long interval;

        public TaskThread(String index, long startTime, long endTime, long interval) {
            this.index = index;
            this.startTime = startTime;
            this.endTime = endTime;
            this.interval = interval;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName() + "------>start");
            ElasticSimhash elasticSimhash = new ElasticSimhash(index, startTime, endTime, interval);
            long timstamp = elasticSimhash.start();
            System.out.println(startTime + "-" + endTime + ":" + timstamp);
        }
    }

}
