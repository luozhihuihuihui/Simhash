package com.kedian.util;

import java.text.SimpleDateFormat;

/**
 * @Author: luozhihui
 * @Description:
 * @Date: Create in 10:17 2019/9/20
 */
public enum TimeUtil {

    MINUTE;
    static final long C0 = 1L;
    static final long C1 = C0 * 1000L;
    static final long C2 = C1 * 1000L;
    static final long C3 = C2 * 1000L;
    static final long C4 = C3 * 60L;
    static final long C5 = C4 * 60L;
    static final long C6 = C5 * 24L;


    public long toMills(long d) {
        throw new AbstractMethodError("No choose");
    }

    public long now() {
        return System.currentTimeMillis();
    }

    public String getTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(now());
    }
}
