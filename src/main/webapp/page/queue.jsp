<?xml version="1.0" encoding="UTF-8" ?>
<%@page import="com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor" %>
<%@page import="com.xingcloud.cache.RedisPoolManager" %>
<%@page import="org.apache.commons.collections.CollectionUtils" %>
<%@page import="org.apache.commons.collections.MapUtils" %>
<%@page import="org.apache.commons.lang3.StringUtils" %>
<%@page import="redis.clients.jedis.Jedis" %>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ page language="java" import="java.util.ArrayList" %>
<%@ page language="java" import="java.util.Collections" %>
<%@ page language="java" import="java.util.HashMap" %>
<%@ page language="java" import="java.util.List" %>
<%@ page language="java" import="java.util.Map" %>
<%@ page import="java.util.Set" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>
<%@ page language="java" %>

<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%
  boolean queryProcessing = false;
  int t = -1;
  String type = request.getParameter("type");
  if (StringUtils.isNotBlank(type)) {
    t = Integer.valueOf(type);
  }

  String key = null;
  switch (t) {
    case 0:
      key = "SINGLE_QUEUE";
      break;
    case 1:
      key = "BATCH_QUEUE";
      break;
    case 2:
      queryProcessing = true;
      break;
    default:
      break;
  }
  List<String> content = null;
  Map<String, String> contentMap = null;
  if (queryProcessing) {
    RedisPoolManager manager = RedisPoolManager.getInstance();
    boolean successful = true;
    Jedis jedis = null;
    try {
      jedis = manager.borrowJedis();
      manager.select(jedis, 1);
      Set<String> keySet = jedis.keys("*");
      int counter = 0;
      if (CollectionUtils.isNotEmpty(keySet)) {
        contentMap = new HashMap<String, String>(keySet.size());
        String[] keysArray = new String[keySet.size()];
        for (String k : keySet) {
          keysArray[counter] = k;
          ++counter;
        }
        List<String> vl = jedis.mget(keysArray);
        String v;
        for (int i = 0; i < keySet.size(); i++) {
          contentMap.put(keysArray[i], vl.get(i));
        }
      }
    } catch (Exception e) {
      successful = false;
      throw e;
    } finally {
      if (successful) {
        manager.returnJedis(jedis);
      } else {
        manager.returnBrokenJedis(jedis);
      }
    }
  } else {
    if (key != null) {
      RedisPoolManager manager = RedisPoolManager.getInstance();
      boolean successful = true;
      Jedis jedis = null;
      try {
        jedis = manager.borrowJedis();
        content = jedis.lrange(key, 0l, Long.MAX_VALUE);
      } catch (Exception e) {
        successful = false;
        throw e;
      } finally {
        if (successful) {
          manager.returnJedis(jedis);
        } else {
          manager.returnBrokenJedis(jedis);
        }
      }
    }
  }

  if (CollectionUtils.isNotEmpty(content)) {
    List<String> l = new ArrayList<String>();
    if (t == 0) {
      for (String s : content) {
        FormulaQueryDescriptor fqd = FormulaQueryDescriptor.parseToDesc(s);
        l.add(fqd.getCacheKey());
      }
    } else {
      for (String s : content) {
        Set<FormulaQueryDescriptor> set = FormulaQueryDescriptor.parseToDescs(s);
        StringBuilder sb = new StringBuilder();
        for (FormulaQueryDescriptor fqd : set) {
          sb.append(fqd.getCacheKey());
          sb.append("<br/>");
        }
        l.add(sb.toString());
      }
    }
    Collections.reverse(l);
    request.setAttribute("content", l);
  }
  if (MapUtils.isNotEmpty(contentMap)) {
    request.setAttribute("contentMap", contentMap);
  }
  request.setAttribute("type", type);
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <title>Cache Viewer</title>
  <link href=" <c:url value='/' />styles/black-tie/jquery-ui-1.10.0.custom.css" rel="stylesheet"/>
  <script src="<c:url value='/' />scripts/jquery.min.js"></script>
  <script src="<c:url value='/' />scripts/jquery-ui-1.10.0.custom.min.js"></script>
  <style>
    .number {
      font-size: 1.5em;
    }
  </style>
</head>
<body style="font-family: monospace;">
<form id="form0" action="queue.jsp" method="post">
  <label>Choose task type:</label>
  <select id="select_type" name="type">
    <option value="0" <c:if test="${type == 0}">selected</c:if>>Single</option>
    <option value="1" <c:if test="${type == 1}">selected</c:if>>Batch</option>
    <option value="2" <c:if test="${type == 2}">selected</c:if>>Processing</option>
  </select>
  <input type="submit" value="List content"/>
</form>
<hr/>
<c:choose>
  <c:when test="${type == 2 }">
    <c:choose>
      <c:when test="${empty contentMap}">
        <div style="font-size: 3em;">NULL-CONTENT</div>
      </c:when>
      <c:otherwise>
        <c:forEach var="entry" items="${contentMap}" varStatus="varStat">
          <div id="cache_status_area_${varStat.count}">
            <span class="number">${varStat.count}</span>
            <span>${entry.key}(${entry.value})</span>
          </div>
        </c:forEach>
      </c:otherwise>
    </c:choose>
  </c:when>
  <c:otherwise>
    <c:choose>
      <c:when test="${empty content}">
        <div style="font-size: 3em;">NULL-CONTENT</div>
      </c:when>
      <c:otherwise>
        <c:forEach items="${content}" var="obj" varStatus="varStat">
          <div id="cache_status_area_${varStat.count}">
            <div class="number">${varStat.count}</div>
            <div>${obj}</div>
          </div>
          <hr/>
        </c:forEach>
      </c:otherwise>
    </c:choose>
  </c:otherwise>
</c:choose>
</body>
</html>