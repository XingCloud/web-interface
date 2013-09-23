package com.xingcloud.webinterface.cache;

import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.model.StatefulCache;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

public interface CacheChecker {

  public StatefulCache checkCache(FormulaQueryDescriptor descriptor) throws XCacheException, ParseIncrementalException,
    InterruptQueryException;

}
