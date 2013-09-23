<?xml version="1.0" encoding="UTF-8" ?>
<%@page import="com.xingcloud.cache.XCache"%>
<%@page import="com.xingcloud.webinterface.model.Filter"%>
<%@page import="com.xingcloud.webinterface.enums.Interval"%>
<%@page import="com.xingcloud.cache.enumpack.CacheExpireType"%>
<%@page import="com.xingcloud.cache.XCacheInfo"%>
<%@page import="com.google.gson.Gson"%>
<%@page import="java.util.HashSet"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page language="java" import="com.xingcloud.cache.redis.NoSelectRedisXCacheOperator"%>
<%@ page language="java" import="com.xingcloud.webinterface.enums.Interval"%>
<%@ page language="java" import="com.xingcloud.webinterface.enums.GroupByType"%>
<%@ page language="java" import="com.google.common.base.Strings"%>
<%@ page language="java" import="com.xingcloud.webinterface.utils.WebInterfaceConstants"%>
<%@ page language="java" import="java.util.Set"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
    String type = request.getParameter("type");
    String dbNumberString = request.getParameter("number");
    String action = request.getParameter("action");

    XCacheInfo cacheInfo = null;
    int dbNumber = -1;
    if (!Strings.isNullOrEmpty(dbNumberString)) {
        dbNumber = Integer.valueOf(dbNumberString);
    }
    if (dbNumber == 10) {
        cacheInfo = XCacheInfo.INCREMENTAL_NORMAL_DEBUG;
    } else {
        cacheInfo = XCacheInfo.CACHE_INFO_11;
    }

    String cacheKey = null;
    if ("common".equals(type)) {
        String projectId = request.getParameter("c_project_id");
        String beginDate = request.getParameter("c_begin_date");
        String endDate = request.getParameter("c_end_date");
        String event = request.getParameter("c_event");
        String segment = request.getParameter("c_segment");
        String interval = request.getParameter("c_interval");

        projectId = Strings.isNullOrEmpty(projectId) ? "*" : projectId;
        beginDate = Strings.isNullOrEmpty(beginDate) ? "*" : beginDate;
        endDate = Strings.isNullOrEmpty(endDate) ? "*" : endDate;
        event = Strings.isNullOrEmpty(event) ? "*" : event;
        segment = Strings.isNullOrEmpty(segment) ? WebInterfaceConstants.TOTAL_USER_IDENTIFIER
                : segment;

        interval = Strings.isNullOrEmpty(interval) ? "*" : interval;

        char seperater = ',';
        StringBuilder sb = new StringBuilder();
        sb.append("COMMON");
        sb.append(seperater);

        sb.append(projectId);
        sb.append(seperater);

        sb.append(beginDate);
        sb.append(seperater);

        sb.append(endDate);
        sb.append(seperater);

        sb.append(event);
        sb.append(seperater);

        sb.append(segment);
        sb.append(seperater);

        sb.append(Filter.ALL.toString());
        sb.append(seperater);

        sb.append(interval);
        cacheKey = sb.toString();
        request.setAttribute("c_project_id", projectId);
        request.setAttribute("c_begin_date", beginDate);
        request.setAttribute("c_end_date", endDate);
        request.setAttribute("c_event", event);
        request.setAttribute("c_segment", segment);
        request.setAttribute("c_interval", interval);
    } else if ("group".equals(type)) {
        String projectId = request.getParameter("g_project_id");
        String beginDate = request.getParameter("g_begin_date");
        String endDate = request.getParameter("g_end_date");
        String event = request.getParameter("g_event");
        String segment = request.getParameter("g_segment");
        String groupByType = request.getParameter("g_groupByType");
        String groupBy = request.getParameter("g_groupby");

        projectId = Strings.isNullOrEmpty(projectId) ? "*" : projectId;
        beginDate = Strings.isNullOrEmpty(beginDate) ? "*" : beginDate;
        endDate = Strings.isNullOrEmpty(endDate) ? "*" : endDate;
        event = Strings.isNullOrEmpty(event) ? "*" : event;
        segment = Strings.isNullOrEmpty(segment) ? WebInterfaceConstants.TOTAL_USER_IDENTIFIER
                : segment;

        groupByType = Strings.isNullOrEmpty(groupByType) ? "*"
                : groupByType;
        groupBy = Strings.isNullOrEmpty(groupBy) ? "*" : groupBy;

        char seperater = ',';
        StringBuilder sb = new StringBuilder();
        sb.append("GROUP");
        sb.append(seperater);

        sb.append(projectId);
        sb.append(seperater);

        sb.append(beginDate);
        sb.append(seperater);

        sb.append(endDate);
        sb.append(seperater);

        sb.append(event);
        sb.append(seperater);

        sb.append(segment);
        sb.append(seperater);

        sb.append(Filter.ALL.toString());
        sb.append(seperater);

        sb.append(groupByType);
        sb.append(seperater);
        sb.append(groupBy);

        cacheKey = sb.toString();
        request.setAttribute("g_project_id", projectId);
        request.setAttribute("g_begin_date", beginDate);
        request.setAttribute("g_end_date", endDate);
        request.setAttribute("g_event", event);
        request.setAttribute("g_segment", segment);
        request.setAttribute("g_groupByType", groupByType);
        request.setAttribute("g_groupby", groupBy);
    } else {
        String pattern = request.getParameter("pattern");
        cacheKey = pattern;
        request.setAttribute("pattern", cacheKey);
    }

    Set<String> keys = null;
    int deleteNum = 0;
    String msg = null;
    String json = null;
    if (!Strings.isNullOrEmpty(cacheKey)) {
        if ("Delete".equals(action)) {
            deleteNum = NoSelectRedisXCacheOperator.getInstance()
                    .deleteKeys(cacheKey, cacheInfo);
            msg = deleteNum + " records has been deleted";
        } else if ("GetContent".equals(action)) {
            XCache xCache = NoSelectRedisXCacheOperator.getInstance()
                    .getCache(cacheKey, cacheInfo);
            msg = "Cache content(" + cacheKey + ")";
            if (xCache != null) {
                json = WebInterfaceConstants.DEFAULT_GSON_PLAIN
                        .toJson(xCache);
            }
        } else {
            keys = NoSelectRedisXCacheOperator.getInstance().listKeys(
                    cacheKey, cacheInfo);
            if (keys != null && keys.isEmpty()) {
                keys = null;
            }
        }
    }
    request.setAttribute("key", cacheKey);
    request.setAttribute("msg", msg);
    request.setAttribute("json", json);
    request.setAttribute("keys", keys);
    request.setAttribute("number", dbNumber);
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<title>Redis operate</title>
<style>
.left {
	width: 40px;
	font-family: sans-serif;
	font-size: 15px;
	font-weight: bold;
}

.key_style {
	color: blue;
	font-family: courier new;
	text-align: center;
	font-weight: bold;
	font-size: 1.7em;
	border: 2px gray solid;
	margin-right: 5px;
	margin-bottom: 10px;
	padding: 5px;
}
</style>
</head>
<body style="font-family: monospace;">
    <div class="key_style">${key}</div>
    <hr />
    <form id="form0" action="list.jsp" method="post">
        <input type="hidden" name="type" value="pattern" />
        <label>Pattern:</label>
        <input id="inp_txt_01" type="text" name="pattern" value='${pattern}' style="width: 700px" />
        <label>DB Number:</label>
        <select name="number">
            <option value="10" <c:if test="${number == 10}">selected</c:if>>Main-Cache</option>
            <option value="11" <c:if test="${number == 11}">selected</c:if>>Test-Cache</option>
        </select>
        <label>Action:</label>
        <select name="action">
            <option value="List">List</option>
            <option value="GetContent">Get-Content</option>
            <option value="Delete">Delete</option>
        </select>
        <input type="submit" value="Do it now" />
    </form>
    <hr />
    <form id="form1" action="list.jsp" method="post">
        <input type="hidden" name="type" value="common" />
        <label>Project Id:</label>
        <input id="inp_txt_c_project_id" type="text" name="c_project_id" value="${c_project_id}" />
        <label>Begin Date:</label>
        <input id="inp_txt_c_begin_date" type="text" name="c_begin_date" value="${c_begin_date}" />
        <label>End Date:</label>
        <input id="inp_txt_c_end_date" type="text" name="c_end_date" value="${c_end_date}" />
        <label>Event:</label>
        <input id="inp_txt_c_event" type="text" name="c_event" value="${c_event}" />
        <label>Segment:</label>
        <input id="inp_txt_c_segment" type="text" name="c_segment" value='${c_segment}' />
        <label>Interval:</label>
        <select id="inp_sel_c_interval" name="c_interval">
            <option <c:if test="${c_interval == null || c_interval == \"\" || c_interval == \"*\"}">selected</c:if> value=""></option>
            <option <c:if test="${c_interval == \"MIN5\"}">selected</c:if> value="MIN5">MIN5</option>
            <option <c:if test="${c_interval == \"HOUR\"}">selected</c:if> value="HOUR">HOUR</option>
            <option <c:if test="${c_interval == \"PERIOD\"}">selected</c:if> value="PERIOD">PERIOD</option>
        </select>
        <label>DB Number:</label>
        <select name="number">
            <option value="10" <c:if test="${number == 10}">selected</c:if>>Main-Cache</option>
            <option value="11" <c:if test="${number == 11}">selected</c:if>>Test-Cache</option>
        </select>
        <label>Action:</label>
        <select name="action">
            <option value="List">List</option>
            <option value="GetContent">Get-Content</option>
            <option value="Delete">Delete</option>
        </select>
        <input type="submit" value="Do it now" />
    </form>
    <hr />
    <form id="form2" action="list.jsp" method="post">
        <input type="hidden" name="type" value="group" />
        <label>Project Id:</label>
        <input id="inp_txt_g_project_id" type="text" name="g_project_id" value="${g_project_id}" />
        <label>Begin Date:</label>
        <input id="inp_txt_g_begin_date" type="text" name="g_begin_date" value="${g_begin_date}" />
        <label>End Date:</label>
        <input id="inp_txt_g_end_date" type="text" name="g_end_date" value="${g_end_date}" />
        <label>Event:</label>
        <input id="inp_txt_g_event" type="text" name="g_event" value="${g_event}" />
        <label>Segment:</label>
        <input id="inp_txt_g_segment" type="text" name="g_segment" value='<c:out value="${g_segment}" escapeXml="true" />' />
        <label>GroupByType:</label>
        <select id="inp_sel_g_interval" name="g_groupByType">
            <option <c:if test="${g_groupByType == \"USER_PROPERTIES\"}">selected</c:if> value="USER_PROPERTIES">USER_PROPERTIES</option>
            <option <c:if test="${g_groupByType == \"EVENT\"}">selected</c:if> value="EVENT">EVENT</option>
        </select>
        <label>Group By:</label>
        <input id="inp_txt_g_groupby" type="text" name="g_groupby" value="${g_groupby}" />
        <label>DB Number:</label>
        <select name="number">
            <option value="10" <c:if test="${number == 10}">selected</c:if>>Main-Cache</option>
            <option value="11" <c:if test="${number == 11}">selected</c:if>>Test-Cache</option>
        </select>
        <label>Action:</label>
        <select name="action">
            <option value="List">List</option>
            <option value="GetContent">Get-Content</option>
            <option value="Delete">Delete</option>
        </select>
        <input type="submit" value="Do it now" />
    </form>
    <hr />
    <div>
        <c:choose>
            <c:when test="${msg!=null}">
                <c:out value="${msg}" />
                <c:if test="${json!=null}">
                    <hr />
                    <c:out value="${json}" />
                </c:if>
            </c:when>
            <c:otherwise>
                <c:choose>
                    <c:when test="${keys!=null}">
                        <table>
                            <c:forEach var="obj" items="${keys}" varStatus="st">
                                <tr>
                                    <td class="left">${st.count}</td>
                                    <td>${obj}</td>
                                </tr>
                            </c:forEach>
                        </table>
                    </c:when>
                    <c:otherwise>
                        <c:out value="No matching keys." />
                    </c:otherwise>
                </c:choose>
            </c:otherwise>
        </c:choose>
    </div>
</body>
</html>