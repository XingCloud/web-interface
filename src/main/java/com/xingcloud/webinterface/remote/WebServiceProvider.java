package com.xingcloud.webinterface.remote;

import static com.xingcloud.webinterface.utils.WebInterfaceConstants.HTTP;

import com.caucho.hessian.client.HessianProxyFactory;
import com.xingcloud.webinterface.conf.WebInterfaceConfig;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WebServiceProvider {

  private static final Logger LOGGER = Logger.getLogger(WebServiceProvider.class);
  private static final HessianProxyFactory FACTORY = new HessianProxyFactory();
  private static final Map<String, Object> SERVICE_MAP = new ConcurrentHashMap<String, Object>();

  public static void loadSingleServiceFromString(URIBuilder builder, String serviceString) throws Exception {
    URI uri;
    String id, host, portString, path;
    int a, b, c, port;
    Object service;
    a = serviceString.indexOf('@');
    b = serviceString.indexOf(':');
    c = serviceString.indexOf('/');
    id = serviceString.substring(0, a);
    host = serviceString.substring(a + 1, b);
    portString = serviceString.substring(b + 1, c);
    port = Integer.valueOf(portString);
    path = serviceString.substring(c);

    builder.setScheme(HTTP);
    builder.setHost(host);
    builder.setPort(port);
    builder.setPath(path);
    uri = builder.build();
    service = FACTORY.create(uri.toString());
    SERVICE_MAP.put(id, service);
    LOGGER.info("[WEB-SERVICE] - WebService loaded - " + uri);
  }

  static {
    Configuration configuration = WebInterfaceConfig.getConfiguration();
    Object serviceObject = configuration.getProperty("web-services.web-service");
    URIBuilder builder = new URIBuilder();
    try {
      if (serviceObject instanceof String) {
        loadSingleServiceFromString(builder, serviceObject.toString());
      } else if (serviceObject instanceof Collection) {
        Collection webServcies = (Collection) serviceObject;
        if (CollectionUtils.isNotEmpty(webServcies)) {
          for (Object webService : webServcies) {
            loadSingleServiceFromString(builder, webService.toString());
          }
        }
      } else {
        LOGGER.info("[WEB-SERVICE] - No web-service will be loaded.");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void init(){

  }

  public static Object provideService(String id) {
    return SERVICE_MAP.get(id);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {

  }

}
