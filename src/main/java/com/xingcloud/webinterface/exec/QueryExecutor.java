package com.xingcloud.webinterface.exec;

import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.exception.RangingException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.UICheckException;
import com.xingcloud.webinterface.exception.UICheckTimeoutException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.exception.XQueryException;
import com.xingcloud.webinterface.model.result.QueryResult;

import java.text.ParseException;

public interface QueryExecutor {
  public QueryResult getResult() throws XQueryException, SegmentException, XParameterException, ParseException,
    UICheckException, UICheckTimeoutException, DataFillingException, RangingException, ParseIncrementalException,
    InterruptQueryException, NecessaryCollectionEmptyException;
}
