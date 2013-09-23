<?xml version="1.0" encoding="UTF-8" ?>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ page language="java" import="com.google.common.base.Strings" %>
<%@ page language="java" import="com.xingcloud.webinterface.enums.QueryType" %>
<%@ page language="java" import="com.xingcloud.webinterface.model.formula.FormulaParameterContainer" %>
<%@ page language="java" import="com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor" %>
<%@ page language="java" import="com.xingcloud.webinterface.model.intermediate.CommonIdResult" %>
<%@ page language="java" import="com.xingcloud.webinterface.model.intermediate.GroupByIdResult" %>
<%@ page language="java" import="com.xingcloud.webinterface.utils.IdResultBuilder" %>
<%@ page language="java" import="org.apache.commons.collections.CollectionUtils" %>
<%@ page language="java" import="org.apache.commons.collections.MapUtils" %>
<%@ page language="java" import="java.util.ArrayList" %>
<%@ page language="java" import="java.util.Collection" %>
<%@ page import="java.util.HashMap" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.Set" %>
<%@ page import="java.util.TreeSet" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>

<%
  String dbNumber = request.getParameter("number");
  String jsonContent = request.getParameter("inp_area");
  String operation = request.getParameter("operation");

  if ("del".equals(operation)) {
    String key = request.getParameter("key");
    System.out.println(key);
  } else if ("get".equals(operation)) {
    String key = request.getParameter("key");
    System.out.println(key);
  }
  if (!Strings.isNullOrEmpty(jsonContent)) {
    Collection<FormulaQueryDescriptor> distinctedDescriptors = null;
    Map<FormulaQueryDescriptor, FormulaQueryDescriptor> m = null;
    FormulaQueryDescriptor existsDescriptor = null;
    Set<String> distinctedDescriptorsKey = null;

    List<FormulaParameterContainer> fpcList = FormulaParameterContainer.json2Containers(jsonContent);

    QueryType queryType = null;
    for (FormulaParameterContainer fpc : fpcList) {
      fpc.validate();
      fpc.init();
      queryType = fpc.getQueryType();
    }
    request.setAttribute("queryType", queryType);

    if (QueryType.COMMON.equals(queryType)) {
      CommonIdResult idResult = null;
      List<CommonIdResult> commonIdResultList = new ArrayList<CommonIdResult>(fpcList.size());
      for (FormulaParameterContainer fpc : fpcList) {
        idResult = IdResultBuilder.buildCommonDescriptor(fpc);
        commonIdResultList.add(idResult);

        distinctedDescriptors = idResult.distinctDescriptor();
        if (CollectionUtils.isEmpty(distinctedDescriptors)) {
          continue;
        }
        if (m == null) {
          m = new HashMap<FormulaQueryDescriptor, FormulaQueryDescriptor>();
        }
        for (FormulaQueryDescriptor descriptor : distinctedDescriptors) {
          if (descriptor.isKilled()) {
            continue;
          }
          existsDescriptor = m.get(descriptor);
          if (existsDescriptor == null) {
            m.put(descriptor, descriptor);
          } else {
            existsDescriptor.addFunctions(descriptor.getFunctions());
          }
        }
      }
      request.setAttribute("idResult", commonIdResultList);
    } else {
      GroupByIdResult idResult = null;
      List<GroupByIdResult> groupByIdResultList = new ArrayList<GroupByIdResult>(fpcList.size());
      for (FormulaParameterContainer fpc : fpcList) {
        idResult = IdResultBuilder.buildGroupByDescriptor(fpc);
        groupByIdResultList.add(idResult);

        distinctedDescriptors = idResult.distinctDescriptor();

        if (CollectionUtils.isEmpty(distinctedDescriptors)) {
          continue;
        }
        if (m == null) {
          m = new HashMap<FormulaQueryDescriptor, FormulaQueryDescriptor>();
        }
        for (FormulaQueryDescriptor descriptor : distinctedDescriptors) {
          if (descriptor.isKilled()) {
            continue;
          }
          existsDescriptor = m.get(descriptor);
          if (existsDescriptor == null) {
            m.put(descriptor, descriptor);
          } else {
            existsDescriptor.addFunctions(descriptor.getFunctions());
          }
        }
      }
      request.setAttribute("idResult", groupByIdResultList);
    }
    distinctedDescriptors = MapUtils.isEmpty(m) ? null : m.values();
    distinctedDescriptorsKey = MapUtils.isEmpty(m) ? null : new TreeSet<String>();
    for (FormulaQueryDescriptor fqd : distinctedDescriptors) {
      distinctedDescriptorsKey.add(fqd.getKey());
    }

    request.setAttribute("distinctedDescriptorsKey", distinctedDescriptorsKey);

  }

  request.setAttribute("jsonContent", jsonContent);
  request.setAttribute("number", dbNumber);
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
    .table {
      padding: 5px;
      margin-bottom: 1.5em;
      font-family: sans-serif;
      font-size: 25px;
      font-weight: bold;
      font-family: sans-serif;
    }

    .titleStyle-none {
      font-family: sans-serif;
      font-size: 25px;
      font-weight: bold;
      padding: 3px;
    }

    .idStyle {
      font-family: sans-serif;
      font-size: 20px;
      font-weight: bold;
    }

    .itemStyle {
      font-family: sans-serif;
      font-size: 18px;
      font-weight: bold;
    }

    .qtStyle {
      font-family: sans-serif;
      font-size: 15px;
      font-weight: normal;
    }

    .fqdStyle-normal {
      font-family: monospace;
      font-size: 10px;
      font-weight: normal;
      color: #a78fc3;
    }

    .fqdStyle-null {
      font-family: monospace;
      font-size: 10px;
      font-weight: normal;
      color: gray;
    }

    .distinctedQD-none {
      font-family: courier new, monospace;
      font-size: 16px;
      font-weight: normal;
      color: #CECECE;
    }

    .distinctedQD-normal {
      font-family: courier new, monospace;
      font-size: 16px;
      font-weight: normal;
      color: #a78fc3;
    }

    .distinctedQD-abnormal {
      font-family: courier new, monospace;
      font-size: 16px;
      font-weight: normal;
      color: #f13f3d;
    }

    .legend-normal {
      font-family: sans-serif;
      font-size: 25px;
      font-weight: bold;
      padding: 8px;
      color: #a78fc3;
    }

    .legend-abnormal {
      font-family: sans-serif;
      font-size: 25px;
      font-weight: bold;
      padding: 8px;
      color: #f13f3d;
    }

    a:LINK {
      color: black;
    }
  </style>

  <script type="text/javascript">
    var ajaxUrl = "<c:url value='/cc'/>";
    $(document).ready(refreshAll);

    var keyArray = null;
    <c:if test="${!empty distinctedDescriptorsKey}">
    keyArray = new Array(${fn:length(distinctedDescriptorsKey)});
    <c:forEach items="${distinctedDescriptorsKey}" var="d" varStatus="varStat">
    keyArray[${varStat.index}] = '${d}';
    </c:forEach>
    </c:if>

    function deleteAll() {
      for (var i = 0; i < keyArray.length; i++) {
        doOperation(i, "del", keyArray[i]);
      }
    }

    function refreshAll() {
      for (var i = 0; i < keyArray.length; i++) {
        doOperation(i, "ref", keyArray[i]);
      }
    }

    function doOperation(id, operation_string, key_string) {
      var db = $("#select_area").val();
      var request = $.ajax({url: ajaxUrl, type: "post", dataType: "text", data: {operation: operation_string, key: key_string, dbNumber: db}});
      request.done(function (msg) {
        if ("true" == msg) {
          $("#cache_status_area_" + id).removeClass().addClass("distinctedQD-normal");
        } else if ("false" == msg) {
          $("#cache_status_area_" + id).removeClass().addClass("distinctedQD-abnormal");
        } else {
          $("#content_area").html("<p>" + msg + "</p>").dialog({
            modal: true,
            draggable: true,
            closeOnEscape: true,
            title: key_string,
            autoOpen: true,
            height: 400,
            width: 900
          });
        }
      });
      request.fail(function (jqXHR, textStatus) {
        alert(textStatus);
      });
    }
  </script>
</head>
<body style="font-family: monospace;">
<form id="form0" action="listall.jsp" method="post" class="table">
  <table border="1" cellpadding="0" cellspacing="0">
    <tr>
      <td>DB Number:</td>
      <td><select id="select_area" name="number">
        <option value="10" <c:if test="${number == 10}">selected</c:if>>Main-Cache</option>
        <option value="11" <c:if test="${number == 11}">selected</c:if>>Test-Cache</option>
      </select></td>
    </tr>
    <tr>
      <td>Json:</td>
      <td><textarea name="inp_area" rows="20" cols="100">${jsonContent}</textarea></td>
    </tr>
    <tr>
      <td align="center" colspan="2"><input type="submit" value="Do it now"/></td>
    </tr>
  </table>
</form>
<hr/>
<div class="titleStyle-none">Query-Type:&nbsp;${queryType}</div>
<hr/>
<div>
  <span class="titleStyle-none">Distincted formula query descriptors</span>
  <span class="legend-normal">CACHE-HIT</span>
  <span class="legend-abnormal">CACHE-MISS</span>
</div>
<div>
        <span class="titleStyle-none">
            <a href="javascript:void(0)" onclick="deleteAll()">Delete All</a>
        </span>
        <span class="titleStyle-none">
            <a href="javascript:void(0)" onclick="refreshAll()">Refresh All</a>
        </span>
</div>
<hr/>
<c:choose>
  <c:when test="${empty distinctedDescriptorsKey}">
    <div class="fqdStyle-null">&nbsp;&nbsp;NULL-CONTENT</div>
  </c:when>
  <c:otherwise>
    <c:forEach items="${distinctedDescriptorsKey}" var="d" varStatus="varStat">
      <div id="cache_status_area_${varStat.index}" class="distinctedQD-none">
        <span style="color: black;">${varStat.count}</span>
        <a href="javascript:void(0)" onclick="doOperation(${varStat.index}, 'del', '${fn:escapeXml(d)}')">Delete</a>
        <a href="javascript:void(0)" onclick="doOperation(${varStat.index}, 'get', '${fn:escapeXml(d)}')">Get</a>
        <a href="javascript:void(0)" onclick="doOperation(${varStat.index}, 'ref', '${fn:escapeXml(d)}')">Refresh</a>
        <span>${d}</span>
      </div>
    </c:forEach>
  </c:otherwise>
</c:choose>

<%--
<hr />
<div class="titleStyle-none">Detail</div>
<hr />
<div>
    <c:forEach items="${idResult}" var="idResult" varStatus="varStat">
        <div>
            <div class="idStyle">${varStat.count}.&nbsp;[${idResult.id}]</div>
            <c:forEach items="${idResult.itemResultMap}" var="itemResultEntry">
                <div class="itemStyle">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[${itemResultEntry.key}]</div>
                <c:choose>
                    <c:when test="${queryType==\"COMMON\"}">
                        <c:forEach items="${itemResultEntry.value.commonItemResults}" var="commonItemResult">
                            <c:forEach items="${commonItemResult.connectorMap}" var="connectorMapEntry">
                                <div class="qtStyle">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[${connectorMapEntry.key}]</div>
                                <c:choose>
                                    <c:when test="${empty connectorMapEntry.value}">
                                        <div class="fqdStyle-null">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;NULL-CONTENT</div>
                                    </c:when>
                                    <c:otherwise>
                                        <c:forEach items="${connectorMapEntry.value}" var="fqd">
                                            <div class="fqdStyle-normal">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;${fqd.key}</div>
                                        </c:forEach>
                                    </c:otherwise>
                                </c:choose>
                            </c:forEach>
                        </c:forEach>
                    </c:when>
                    <c:when test="${queryType==\"GROUP\"}">
                        <c:forEach items="${itemResultEntry.value.groupByItemResults}" var="groupByItemResult">
                            <c:forEach items="${groupByItemResult.connector}" var="fqd">
                                <div class="fqdStyle-normal">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;${fqd.key}</div>
                            </c:forEach>
                        </c:forEach>
                    </c:when>
                </c:choose>
            </c:forEach>
        </div>
    </c:forEach>
</div>
--%>
<!-- ui-dialog -->
<div id="content_area"/>
</body>
</html>