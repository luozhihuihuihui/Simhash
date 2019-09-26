package com.kedian.util;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @Author: luozhihui
 * @Description:
 * @Date: Create in 11:29 2019/9/16
 */
public class EsPool {
    private Queue<String> ips = new LinkedList<String>();
    private static EsPool esPool;
    private String ip;

    public synchronized static EsPool getEsPool() {
        if (esPool == null) {
            return new EsPool();
        }
        return esPool;
    }

    private EsPool() {
        ips.offer("192.168.4.172");
        ips.offer("192.168.4.173");
        ips.offer("192.168.4.174");
    }

    public synchronized String getEsIp() {
        ip = ips.peek();
        ips.offer(ip);
        return ip;
    }


    public static void main(String[] args) {
        long DAYTIME = 86400000L;
        long EIGHT = 28800000L;
        long endTime = System.currentTimeMillis() + EIGHT;
        long startTime = endTime - endTime % DAYTIME;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(dateFormat.format(startTime));
        System.out.println(dateFormat.format(endTime));

    }
}
