package com.lxing.util;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;

/**
 * Created by lxing on 2017/4/10.
 * httpclient 下载网页信息
 */
public class Crawler {
    public static Logger logger = LoggerFactory.getLogger(Crawler.class);
    public static String crawl(String url) {
        String ip = "127.0.0.1";
        int port = 9998;
        HttpClientBuilder build = HttpClients.custom();
        HttpHost proxy = new HttpHost(ip, port);
        CloseableHttpClient httpClient = build.setProxy(proxy).build();
        CloseableHttpResponse response = null;
        BufferedReader br = null;
        try {
            HttpGet httpGet = new HttpGet(url);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(2000).setConnectTimeout(2000).build();//设置请求和传输超时时间
            httpGet.setConfig(requestConfig);
            httpGet.setHeader("User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");
            response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                InputStreamReader isr = new InputStreamReader(entity.getContent(), "utf-8");
                br = new BufferedReader(isr);
                String line;
                StringBuilder stringBuilder = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    stringBuilder.append(line);
                }
                return stringBuilder.toString();
            }
        }catch (SocketTimeoutException e1){
            logger.error(url+": 请求超时");
        }
        catch (Exception e) {
            e.printStackTrace();

        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }

        }
        return null;
    }

    public static void main(String[] args) {
        String url = "http://blog.csdn.net/wgyscsf/article/list/10000";
        Crawler crawler = new Crawler();
        String s = crawler.crawl(url);
        System.out.print(s);
    }
}
