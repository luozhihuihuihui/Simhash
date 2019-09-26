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
public class News {
    private static final long MINUTE = 900000L;
    private static final long EIGHT = 28800000L;
    private static final long DAY = 86400000L;

    public static void main(String[] args) {
        News news = new News();
        news.start();
    }

    public void start() {
        String index = "news";
        ESDBUtil.index = "simhashnews";
        while (true) {
            long endTime = System.currentTimeMillis() + EIGHT + MINUTE / 5;
            long startTime = endTime - MINUTE / 5 - MINUTE / 5;
            TaskThread taskThread = new TaskThread(index, startTime, endTime, MINUTE / 5 * 2);
            Thread thread = new Thread(taskThread);
            thread.start();
            System.out.println("start to sleep!");
            try {
                Thread.sleep(MINUTE / 15 * 2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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
