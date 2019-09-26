package com.kedian.simhash;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;

/**
 * @Author: luozhihui
 * @Description:
 * @Date: Create in 14:04 2019/9/6
 */
public class SimHash {
    private static final int HASH_BITS = 64;

    public SimHash() {

    }

    public static int hamming(long lhs, long rhs) {
        int cnt = 0;
        lhs ^= rhs;
        while (lhs != 0) {
            lhs &= (lhs - 1);
            cnt++;
        }
        return cnt;
    }

    static class MurmurHash {
        public static long hash(byte[] key) {
            ByteBuffer buf = ByteBuffer.wrap(key);
            int seed = 0x1234ABCD;

            ByteOrder byteOrder = buf.order();
            buf.order(ByteOrder.LITTLE_ENDIAN);

            long m = 0xc6a4a7935bd1e995L;
            int r = 47;

            long h = seed ^ (buf.remaining() * m);

            long k;
            while ((buf.remaining() >= 8)) {
                k = buf.getLong();

                k *= m;
                k ^= k >>> r;
                k *= m;
                h ^= k;
                h *= m;
            }
            if (buf.remaining() > 0) {
                ByteBuffer finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                finish.put(buf).rewind();
                h ^= finish.getLong();
                h *= m;
            }
            h ^= h >>> r;
            h *= m;
            h ^= h >>> r;
            buf.order(byteOrder);
            return h;
        }

        public static long hash(String text) {
            return hash(text.getBytes());
        }

    }

    private static long hash(String token) {
        if (token == null || token.length() == 0) {
            return 0;
        }
        char[] sourceArray = token.toCharArray();
        BigInteger x = BigInteger.valueOf((long) sourceArray[0] << 7);
        BigInteger m = new BigInteger("1000003");
        BigInteger mask = new BigInteger("2").pow(HASH_BITS).subtract(new BigInteger("1"));
        for (char item : sourceArray) {
            BigInteger temp = BigInteger.valueOf((long) item);
            x = x.multiply(m).xor(temp).and(mask);
        }
        x = x.xor(new BigInteger(String.valueOf(token.length())));
        if (x.equals(new BigInteger("-1"))) {
            x = new BigInteger("-2");
        }
        return x.longValue();
    }

    public static long getSimHash(String text) throws IOException {
        int[] vecHash = new int[HASH_BITS];
        Analyzer analyzer = new StandardAnalyzer();
        TokenStream ts = analyzer.tokenStream("", text);
        ts.reset();
        CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
        HashMap<String, Integer> tokenMap = new HashMap<String, Integer>();
        while (ts.incrementToken()) {
            String token = term.toString();
            if (tokenMap.containsKey(token)) {
                int weight = tokenMap.get(token);
                tokenMap.put(token, ++weight);
            } else {
                tokenMap.put(token, 1);
            }
        }
        final long L64_l = 1;
        for (String k : tokenMap.keySet()) {
            long h = MurmurHash.hash(k);
            for (int c = 0; c < HASH_BITS; c++) {
                int weight = tokenMap.get(k);
                vecHash[c] = ((h & (L64_l << c)) == (L64_l << c)) ? vecHash[c] + weight : vecHash[c] - weight;
            }
        }
        long fingerprint = 0;
        for (int i = 0; i < HASH_BITS; i++) {
            if (vecHash[i] > 0) {
                fingerprint |= L64_l << i;
            }
        }
        return fingerprint;
    }

    public static void main(String[] args) {
        String simhash1 = new BigInteger("9561184231124639954", 10).toString(2);
        String simhash2 = new BigInteger("1486194888570586244", 10).toString(2);

        System.out.println(simhash1 + "   " + simhash1.length());
        System.out.println(simhash2 + "   " + simhash2.length());
        String hm = new BigInteger(simhash1, 2).xor(new BigInteger(simhash2, 2)).toString(2);
        System.out.println(hm);
        BigInteger hmbin=new BigInteger(hm,2);
        hmbin=hmbin.shiftRight(1).and(new BigInteger("1"));
        System.out.println(hmbin);
        System.out.println(hmbin.xor(new BigInteger("1")).toString(2));
    }
}
