package com.xingcloud.webinterface.web.servlet;

import static com.xingcloud.webinterface.enums.SyncType.LIST;
import static com.xingcloud.webinterface.enums.SyncType.REMOVE;
import static com.xingcloud.webinterface.enums.SyncType.SAVE;
import static com.xingcloud.webinterface.enums.SyncType.UPDATE;

import com.google.common.base.Strings;
import com.google.gson.reflect.TypeToken;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.enums.SyncType;
import com.xingcloud.webinterface.exception.ParseJsonException;
import com.xingcloud.webinterface.model.CustomUserProperty;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomUserPropertiesOperateServlet extends AbstractServlet {
  private static final Logger LOGGER = Logger.getLogger(CustomUserPropertiesOperateServlet.class);

  private static final long serialVersionUID = 621676853016773010L;

  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
    IOException {
    LOGGER.info("[SERVLET-ENTER]");
    long t1 = System.currentTimeMillis();
    long t2 = 0;
    long usingTime = 0;
    String projectId = request.getParameter("project_id");
    String operationTypeString = request.getParameter("type");
    String json = request.getParameter("params");
    Map<String, Object> result = new HashMap<String, Object>();
    Map<String, Object> operationMSG;
    try {
      operationMSG = doWork(projectId, json, operationTypeString);
      result.put("result", true);
      result.putAll(operationMSG);
    } catch (Exception e) {
      e.printStackTrace();
      result.put("result", false);
    } finally {
      t2 = System.currentTimeMillis();
      usingTime = t2 - t1;
      result.put("milli", usingTime);
    }

    Writer writer = new OutputStreamWriter(response.getOutputStream(), "utf-8");
    writer.write(GSON.toJson(result));
    writer.flush();
    LOGGER.info("[SERVLET-EXIT] in " + usingTime + " milliseconds");
  }

  private Map<String, Object> doWork(String projectId, String json, String operationTypeString) throws Exception {
    if (Strings.isNullOrEmpty(operationTypeString)) {
      throw new Exception("type is null.");
    }
    Map<String, Object> result = new HashMap<String, Object>();
    SyncType st = Enum.valueOf(SyncType.class, operationTypeString);

    int size = 0;
    LOGGER.info("[PARAM] - operation type - " + st);
    if (LIST.equals(st)) {
      if (Strings.isNullOrEmpty(projectId)) {
        throw new Exception("project id is null.");
      }

      LOGGER.info("[PARAM] - project id - " + projectId);

      List<CustomUserProperty> listResult = listTables(projectId);
      result.put("result", listResult);
    } else {
      if (Strings.isNullOrEmpty(json)) {
        throw new Exception("parameter json is null.");
      }
      json = URLDecoder.decode(json, "utf8");
      LOGGER.info("[PARAM] - json - " + json);

      Type type = new TypeToken<List<CustomUserProperty>>() {
      }.getType();
      List<CustomUserProperty> properties;

      try {
        properties = GSON.fromJson(json, type);
      } catch (Exception e) {
        throw new ParseJsonException("cannot parse json", e.getCause());
      }

      size = properties.size();
      if (SAVE.equals(st)) {
        createTables(properties);
      } else if (UPDATE.equals(st)) {
        updateTables(properties);
      } else if (REMOVE.equals(st)) {
        dropTables(properties);
      } else {
        throw new UnsupportedOperationException("cannot resolve operation - " + st);
      }
      result.put("cnt", size);
    }
    return result;
  }

  private void updateTables(List<CustomUserProperty> properties) throws Exception {
    MySql_16seqid manager = MySql_16seqid.getInstance();
    for (CustomUserProperty cup : properties) {
      if (!cup.validateAndTrim()) {
        throw new Exception("Custom User Property - " + cup + " is not valid");
      }
      LOGGER.info("received cup - " + cup);
      manager.updateTable(cup.getProjectId(), cup.getName(), cup.getNickname(), cup.getSlicePattern());
    }
  }

  private void createTables(List<CustomUserProperty> properties) throws Exception {
    MySql_16seqid manager = MySql_16seqid.getInstance();
    for (CustomUserProperty cup : properties) {
      if (!cup.validateAndTrim()) {
        throw new Exception("Custom User Property - " + cup + " is not valid");
      }
      LOGGER.info("received cup - " + cup);
      boolean success = false;
      for (int i = 0; i < 3; i++) {
        try {
          if (!manager.dbexists(cup.getProjectId())) {
            manager.createDBIfNotExist(cup.getProjectId());
          }
          manager.createTable(cup.getProjectId(), cup.getName(), cup.getType(), cup.getFunc());
          success = true;
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          if (success) {
            break;
          }
        }
      }
      if (!success) {
        throw new Exception();
      }
    }
  }

  private void dropTables(List<CustomUserProperty> properties) throws SQLException {
    MySql_16seqid manager = MySql_16seqid.getInstance();
    for (CustomUserProperty cup : properties) {
      manager.dropTable(cup.getProjectId(), cup.getName());
    }
  }

  private List<CustomUserProperty> listTables(String projectId) throws SQLException {
    MySql_16seqid manager = MySql_16seqid.getInstance();
    List<UserProp> result = manager.getUserProps(projectId);
    if (CollectionUtils.isEmpty(result)) {
      return null;
    }
    List<CustomUserProperty> properties = new ArrayList<CustomUserProperty>(result.size());
    CustomUserProperty cup;
    for (UserProp up : result) {
      cup = new CustomUserProperty(projectId, up.getPropName(), up.getPropAlias(), up.getPropType(), up.getPropSegm(),
                                   up.getPropFunc());
      cup.validateAndTrim();
      properties.add(cup);
    }
    return properties;
  }

}
