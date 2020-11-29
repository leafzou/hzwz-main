package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.ClientDataThread;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    public static final int NUMBER_OF_THREAD = 4;
    public static final Object thread_lock = new Object();
    private Thread[] threads = new ClientDataThread[NUMBER_OF_THREAD];

    // an list of trace map,like ring buffe.  key is traceId, value is spans ,  r
    public static Vector<Map<String, List<String>>> BATCH_TRACE_LIST = new Vector<>();
    // make 50 bucket to cache traceData
    public static int BATCH_COUNT = 16;

    public static void init() {
        for (int i = 0; i < BATCH_COUNT; i++) {
            BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
        }
    }

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {
        try {
            ExecutorService threadPool = Executors.newFixedThreadPool(NUMBER_OF_THREAD);
            Future<?>[] futures = new Future<?>[NUMBER_OF_THREAD];

            long dataSize = getSourceDataSize();
            long currentPartSize = dataSize / NUMBER_OF_THREAD + 1;

            for (int i = 0; i < NUMBER_OF_THREAD; i++) {
                long startPos = i * currentPartSize;
                threads[i] = new ClientDataThread(startPos, currentPartSize, getSourceDataInputStream(), i);
                futures[i] = threadPool.submit(threads[i]);
            }

            long startTime = System.currentTimeMillis();
            threadPool.awaitTermination(2, TimeUnit.SECONDS);
            for (Future<?> future : futures) {
                future.get();
            }


            LOGGER.info("Time used(second): " + (double) ((System.currentTimeMillis() - startTime) / 1000));
        } catch (Exception e) {
            LOGGER.warn("fail to process data", e);
        } finally {
            callFinish();
        }
    }

    // notify backend process when client process has finished.
    public void callFinish() {
        try {
            Request request = new Request.Builder().url("http://localhost:8002/finish").build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("fail to callFinish");
        }
    }

    public static String getWrongTracing(String wrongTraceIdList, int batchPos) {
        LOGGER.info(String.format("getWrongTracing, batchPos:%d, wrongTraceIdList:\n %s",
                batchPos, wrongTraceIdList));
        List<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<List<String>>() {
        });
        Map<String, List<String>> wrongTraceMap = new HashMap<>();
        int pos = batchPos % BATCH_COUNT;
        int previous = pos - 1;
        if (previous == -1) {
            previous = BATCH_COUNT - 1;
        }
        int next = pos + 1;
        if (next == BATCH_COUNT) {
            next = 0;
        }

        synchronized (thread_lock) {
            getWrongTraceWithBatch(previous, pos, traceIdList, wrongTraceMap);
            getWrongTraceWithBatch(pos, pos, traceIdList, wrongTraceMap);
            getWrongTraceWithBatch(next, pos, traceIdList, wrongTraceMap);
            // to clear spans, don't block client process thread. TODO to use lock/notify
            BATCH_TRACE_LIST.get(previous).clear();
        }
        return JSON.toJSONString(wrongTraceMap);
    }

    private static void getWrongTraceWithBatch(int batchPos, int pos, List<String> traceIdList, Map<String, List<String>> wrongTraceMap) {
        // donot lock traceMap,  traceMap may be clear anytime.
        Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(batchPos);
        for (String traceId : traceIdList) {
            List<String> spanList = traceMap.get(traceId);
            if (spanList != null) {
                // one trace may cross to batch (e.g batch size 20000, span1 in line 19999, span2 in line 20001)
                List<String> existSpanList = wrongTraceMap.get(traceId);
                if (existSpanList != null) {
                    existSpanList.addAll(spanList);
                } else {
                    wrongTraceMap.put(traceId, spanList);
                }
                // output spanlist to check
                String spanListString = spanList.stream().collect(Collectors.joining("\n"));
                LOGGER.info(String.format("getWrongTracing, batchPos:%d, pos:%d, traceId:%s, spanList:\n %s",
                        batchPos, pos, traceId, spanListString));
            }
        }
    }

    private String getPath() {
        String port = System.getProperty("server.port", "8080");
        if ("8000".equals(port)) {
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
        } else if ("8001".equals(port)) {
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
        } else {
            return null;
        }
    }


    private boolean isDev() {
        return Boolean.valueOf(System.getProperty("is.dev", "false"));
    }

    private InputStream getSourceDataInputStream() throws IOException {
        InputStream input = null;
        if (isDev()) {
            if ("8000".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace1.data");
                input = new FileInputStream(file);
            }

            if ("8001".equals(System.getProperty("server.port", "8000"))) {
                File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace2.data");
                input = new FileInputStream(file);
            }

        } else {
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                LOGGER.warn("path is empty");
                throw new IOException("path is empty");
            }

            URL url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            input = httpConnection.getInputStream();
        }
        return input;
    }

    private long getSourceDataSize() throws IOException {
        long size;
        if (isDev()) {
            File file = new File("C:\\Users\\55733\\Desktop\\doc\\demo_data\\trace1.data");
            size = file.length();
        } else {
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                LOGGER.warn("path is empty");
                throw new IOException("path is empty");
            }

            URL url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            size = httpConnection.getContentLengthLong();
        }
        return size;
    }
}
