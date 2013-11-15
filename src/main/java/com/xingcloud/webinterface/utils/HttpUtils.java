package com.xingcloud.webinterface.utils;

import com.xingcloud.webinterface.conf.WebInterfaceConfig;
import com.xingcloud.webinterface.monitor.WIEvent;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient;
import org.apache.http.nio.client.HttpAsyncClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.concurrent.Future;

public class HttpUtils {
  public static boolean ENABLE_SYSTEM_MONITOR;
  public static String SYSTEM_MONITOR_HOST;
  public static int SYSTEM_MONITOR_PORT;
  public static String SYSTEM_MONITOR_PATH;

  static {
    Configuration configuration = WebInterfaceConfig.getConfiguration();
    ENABLE_SYSTEM_MONITOR = configuration.getBoolean("system-monitor[@enabled]", false);
    SYSTEM_MONITOR_HOST = configuration.getString("system-monitor.host");
    SYSTEM_MONITOR_PORT = configuration.getInt("system-monitor.port");
    SYSTEM_MONITOR_PATH = configuration.getString("system-monitor.path");
  }

  public static StatusLine sendMonitorInfo(WIEvent... mis) throws Exception {
    if (ArrayUtils.isEmpty(mis)) {
      return null;
    }
    URIBuilder builder = new URIBuilder();
    builder.setScheme(WebInterfaceConstants.HTTP);
    builder.setHost(SYSTEM_MONITOR_HOST);
    builder.setPort(SYSTEM_MONITOR_PORT);
    builder.setPath(SYSTEM_MONITOR_PATH);
    for (int i = 0; i < mis.length; i++) {
      builder.addParameter("action" + i, mis[i].toString());
    }

    URI uri = builder.build();
    HttpAsyncClient httpclient = new DefaultHttpAsyncClient();
    httpclient.start();
    try {
      HttpGet request = new HttpGet(uri);
      Future<HttpResponse> future = httpclient.execute(request, null);
      HttpResponse response = future.get();
      return response.getStatusLine();
    } finally {
      httpclient.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    URIBuilder builder = new URIBuilder();
    builder.setScheme("http");
    builder.setHost("localhost");
    builder.setPort(8080);
    builder.setPath("/dd/t");
    builder.addParameter("params", "tomcat.visit.query.enter,0");
    URI uri = builder.build();

    HttpAsyncClient httpclient = new DefaultHttpAsyncClient();
    httpclient.start();
    try {
      HttpGet request = new HttpGet(uri);
      Future<HttpResponse> future = httpclient.execute(request, null);
      HttpResponse response = future.get();
      System.out.println("Response: " + response.getStatusLine());
      System.out.println("Shutting down");
    } finally {
      httpclient.shutdown();
    }
    System.out.println("Done");
  }

  public static void readResponse(InputStream in) throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String line = null;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }
  }
}
