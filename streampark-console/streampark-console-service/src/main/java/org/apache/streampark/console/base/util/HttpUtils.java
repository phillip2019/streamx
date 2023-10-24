package org.apache.streampark.console.base.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HttpUtils {

  private Logger logger = LoggerFactory.getLogger(HttpUtils.class);

  private static PoolingHttpClientConnectionManager pcm; // httpclient连接池
  private CloseableHttpClient httpClient = null; // http连接
  private int connectTimeout = 120000; // 连接超时时间
  private int connectionRequestTimeout = 10000; // 从连接池获取连接超时时间
  private int socketTimeout = 300000; // 获取数据超时时间
  private String charset = "utf-8";
  private RequestConfig requestConfig = null; // 请求配置
  private Builder requestConfigBuilder = null; // build requestConfig

  private List<NameValuePair> nvps = new ArrayList<NameValuePair>();
  private List<Header> headers = new ArrayList<Header>();
  private String requestParam = "";

  static {
    pcm = new PoolingHttpClientConnectionManager();
    pcm.setMaxTotal(50); // 整个连接池最大连接数
    pcm.setDefaultMaxPerRoute(50); // 每路由最大连接数，默认值是2
  }

  /** 默认设置 */
  private static HttpUtils defaultInit() {
    HttpUtils httpUtils = new HttpUtils();
    if (httpUtils.requestConfig == null) {
      httpUtils.requestConfigBuilder =
          RequestConfig.custom()
              .setConnectTimeout(httpUtils.connectTimeout)
              .setConnectionRequestTimeout(httpUtils.connectionRequestTimeout)
              .setSocketTimeout(httpUtils.socketTimeout);
      httpUtils.requestConfig = httpUtils.requestConfigBuilder.build();
    }
    return httpUtils;
  }

  /** 初始化 HttpUtils */
  public static HttpUtils init() {
    HttpUtils httpUtils = defaultInit();
    if (httpUtils.httpClient == null) {
      httpUtils.httpClient = HttpClients.custom().setConnectionManager(pcm).build();
    }
    return httpUtils;
  }

  /** 初始化 HttpUtils */
  public static HttpUtils init(Map<String, String> paramMap) {
    HttpUtils httpUtils = init();
    httpUtils.setParamMap(paramMap);
    return httpUtils;
  }

  /** 验证初始化 */
  public static HttpUtils initWithAuth(String ip, int port, String username, String password) {
    HttpUtils httpUtils = defaultInit();
    if (httpUtils.httpClient == null) {
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          new AuthScope(ip, port, AuthScope.ANY_REALM),
          new UsernamePasswordCredentials(username, password));
      httpUtils.httpClient =
          HttpClients.custom()
              .setDefaultCredentialsProvider(credentialsProvider)
              .setConnectionManager(pcm)
              .build();
    }
    return httpUtils;
  }

  /** 设置请求头 */
  public HttpUtils setHeader(String name, String value) {
    Header header = new BasicHeader(name, value);
    headers.add(header);
    return this;
  }

  /** 设置请求头 */
  public HttpUtils setHeaderMap(Map<String, String> headerMap) {
    for (Entry<String, String> param : headerMap.entrySet()) {
      Header header = new BasicHeader(param.getKey(), param.getValue());
      headers.add(header);
    }
    return this;
  }

  /** 设置请求参数 */
  public HttpUtils setParam(String name, String value) {
    nvps.add(new BasicNameValuePair(name, value));
    return this;
  }

  /** 设置请求参数 */
  public HttpUtils setParamMap(Map<String, String> paramMap) {
    for (Entry<String, String> param : paramMap.entrySet()) {
      nvps.add(new BasicNameValuePair(param.getKey(), param.getValue()));
    }
    return this;
  }

  /** 设置字符串参数 */
  public HttpUtils setStringParam(String requestParam) {
    this.requestParam = requestParam;
    return this;
  }

  /** 设置连接超时时间 */
  public HttpUtils setConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    this.requestConfigBuilder = requestConfigBuilder.setConnectTimeout(connectTimeout);
    requestConfig = requestConfigBuilder.build();
    return this;
  }

  /** http get 请求 */
  public Map<String, String> get(String url) {
    Map<String, String> resultMap = new HashMap<String, String>();
    // 获取请求URI
    URI uri = getUri(url);
    if (uri != null) {
      HttpGet httpGet = new HttpGet(uri);
      httpGet.setConfig(requestConfig);
      if (!CollectionUtils.isEmpty(headers)) {
        Header[] header = new Header[headers.size()];
        httpGet.setHeaders(headers.toArray(header));
      }

      // 执行get请求
      try {
        CloseableHttpResponse response = httpClient.execute(httpGet);
        return getHttpResult(response, url, httpGet, resultMap);
      } catch (Exception e) {
        httpGet.abort();
        resultMap.put("result", e.getMessage());
        logger.error("获取http GET请求返回值失败 url=" + url, e);
      }
    }
    return resultMap;
  }

  /** http post 请求 */
  public Map<String, String> post(String url) {
    HttpPost httpPost = new HttpPost(url);
    httpPost.setConfig(requestConfig);
    if (!CollectionUtils.isEmpty(headers)) {
      Header[] header = new Header[headers.size()];
      httpPost.setHeaders(headers.toArray(header));
    }
    if (!CollectionUtils.isEmpty(nvps)) {
      try {
        httpPost.setEntity(new UrlEncodedFormEntity(nvps, charset));
      } catch (Exception e) {
        logger.error("http post entity form error", e);
      }
    }
    if (!StringUtils.isEmpty(requestParam)) {
      try {
        httpPost.setEntity(new StringEntity(requestParam, charset));
      } catch (UnsupportedCharsetException e) {
        logger.error("http post entity form error", e);
      }
    }
    Map<String, String> resultMap = new HashMap<String, String>();
    // 执行post请求
    try {
      CloseableHttpResponse response = httpClient.execute(httpPost);
      return getHttpResult(response, url, httpPost, resultMap);
    } catch (Exception e) {
      httpPost.abort();
      resultMap.put("result", e.getMessage());
      logger.error("获取http POST请求返回值失败 url=" + url, e);
    }
    return resultMap;
  }

  /** 获取请求返回值 */
  private Map<String, String> getHttpResult(
      CloseableHttpResponse response,
      String url,
      HttpUriRequest request,
      Map<String, String> resultMap) {
    String result = "";
    int statusCode = response.getStatusLine().getStatusCode();
    resultMap.put("statusCode", statusCode + "");
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      try {
        result = EntityUtils.toString(entity, charset);
        EntityUtils.consume(entity); // 释放连接
      } catch (Exception e) {
        logger.error("获取http请求返回值解析失败", e);
        request.abort();
      }
    }
    if (statusCode != 200) {
      result = "HttpClient status code :" + statusCode + "  request url===" + url;
      logger.info("HttpClient status code :" + statusCode + "  request url===" + url);
      request.abort();
    }
    resultMap.put("result", result);
    return resultMap;
  }

  /** 获取重定向url返回的location */
  public String redirectLocation(String url) {
    String location = "";
    // 获取请求URI
    URI uri = getUri(url);
    if (uri != null) {
      HttpGet httpGet = new HttpGet(uri);
      requestConfig = requestConfigBuilder.setRedirectsEnabled(false).build(); // 设置自动重定向false
      httpGet.setConfig(requestConfig);
      if (!CollectionUtils.isEmpty(headers)) {
        Header[] header = new Header[headers.size()];
        httpGet.setHeaders(headers.toArray(header));
      }

      try {
        // 执行get请求
        CloseableHttpResponse response = httpClient.execute(httpGet);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY
            || statusCode == HttpStatus.SC_MOVED_TEMPORARILY) { // 301 302
          Header header = response.getFirstHeader("Location");
          if (header != null) {
            location = header.getValue();
          }
        }
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          EntityUtils.consume(entity);
        }
      } catch (Exception e) {
        logger.error("获取http GET请求获取 302 Location失败 url=" + url, e);
        httpGet.abort();
      }
    }
    return location;
  }

  private URI getUri(String url) {
    URI uri = null;
    try {
      URIBuilder uriBuilder = new URIBuilder(url);
      if (!CollectionUtils.isEmpty(nvps)) {
        uriBuilder.setParameters(nvps);
      }
      uri = uriBuilder.build();
    } catch (URISyntaxException e) {
      logger.error("url 地址异常", e);
    }
    return uri;
  }

  public static void main(String[] args) {
    //        HttpUtils HttpUtils = httpUtils.init();
    //        httpUtils.setHeader("Accept","*/*");
    //
    // httpUtils.setHeader("Authorization","69qMW7reOXhrAh29LjPWwwP+quFqLf++MbPbsB9/NcTCKGzZE2EU7tBUBU5gqG236VF5pMyVrsE5K7hBWiyuLuJRqmxKdPct4lbGrjZZqkvv7t5iAUVpAt0BwkhyliSjqqslZifFx3P4A//NYgGwk/LMyk2Syns0Xxy5KOx+CKnHG5gm8VNry6jVS5ybHcem8Kxkhir3YrPesxzWvkX2QQ==");
    //        String url
    // ="http://172.18.5.14:30000/flink/app/list?jobName=etl_event_tracking2hive_sql&teamId=1&pageNum=1&pageSize=1";
    //        Map<String, String> post = httpUtils.post(url);
    //        System.out.println(post.get("result"));
    //        ObjectMapper objectMapper = new ObjectMapper();
    //        try {
    //            JsonNode rootNode = objectMapper.readTree(post.get("result"));
    //            JsonNode dataNode = rootNode.get("data");
    //            if (dataNode != null) {
    //                JsonNode recordsNode = dataNode.get("records");
    //                if (recordsNode != null && recordsNode.isArray() && recordsNode.size() > 0) {
    //                    JsonNode firstRecordNode = recordsNode.get(0);
    //                    String id = firstRecordNode.get("id").asText();
    //                    System.out.println("ID: " + id);
    //                }
    //            }
    //        } catch (Exception e) {
    //            e.printStackTrace();
    //        }
  }
}
