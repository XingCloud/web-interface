package com.xingcloud.webinterface.calculate;

import static com.xingcloud.webinterface.cache.MemoryCachedObjects.MCO_FORMULA;
import static com.xingcloud.webinterface.calculate.Scale.DEFAULT_SCALE;
import static com.xingcloud.webinterface.calculate.Scale.buildLowerScale;
import static com.xingcloud.webinterface.calculate.Scale.buildRangeScale;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.FORMULA_PART_SPLITTOR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.V9_SPLITTOR;

import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.memcache.XMemCacheManager;
import com.xingcloud.webinterface.cache.MemoryCachedObjects;
import com.xingcloud.webinterface.exception.FormulaException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 13-12-16 Time: 下午2:49 Package: com.xingcloud.webinterface.calculate
 */
public class ScaleGroup {

  private List<Scale> scaleList;

  public static ScaleGroup buildScaleGroup(String rawScale) throws FormulaException, MemCacheException {
    if (StringUtils.isBlank(rawScale)) {
      throw new FormulaException("Raw scale is null.");
    }
    XMemCacheManager xMemCacheManager = MemoryCachedObjects.getInstance().getCacheManager();
    ScaleGroup scaleGroup = xMemCacheManager.getCacheElement(MCO_FORMULA, rawScale, ScaleGroup.class);
    if (scaleGroup != null) {
      return scaleGroup;
    }
    String[] rawScaleArr = StringUtils.split(rawScale, V9_SPLITTOR);
    if (ArrayUtils.isEmpty(rawScaleArr)) {
      throw new FormulaException("Empty scale");
    }
    scaleGroup = new ScaleGroup();

    List<Scale> scaleList1 = new ArrayList<Scale>(rawScaleArr.length);
    String scalePart, scalePartDateNode, scalePartDateNodeNext = null;
    int idx;

    for (int i = 0; i < rawScaleArr.length - 1; i++) {
      idx = rawScaleArr[i].indexOf(FORMULA_PART_SPLITTOR);
      scalePartDateNode = rawScaleArr[i].substring(0, idx);
      scalePart = rawScaleArr[i].substring(idx + 1);
      idx = rawScaleArr[i + 1].indexOf(FORMULA_PART_SPLITTOR);
      scalePartDateNodeNext = rawScaleArr[i + 1].substring(0, idx);
      scaleList1.add(buildRangeScale(scalePartDateNode, scalePartDateNodeNext, Double.valueOf(scalePart)));
    }
    idx = rawScaleArr[rawScaleArr.length - 1].indexOf(FORMULA_PART_SPLITTOR);
    scalePart = rawScaleArr[rawScaleArr.length - 1].substring(idx + 1);
    if (scalePartDateNodeNext == null) {
      scalePartDateNodeNext = rawScaleArr[rawScaleArr.length - 1].substring(0, idx);
    }
    scaleList1.add(buildLowerScale(scalePartDateNodeNext, Double.valueOf(scalePart)));
    scaleGroup.scaleList = scaleList1;
    xMemCacheManager.putCacheElement(MCO_FORMULA, rawScale, scaleGroup);
    return scaleGroup;
  }

  public double getScale(String date) throws FormulaException {
    if (CollectionUtils.isEmpty(this.scaleList)) {
      throw new FormulaException("Cannot parse any scale.");
    }
    Double scale;
    for (Scale s : scaleList) {
      scale = s.accept(date);
      if (scale != null) {
        return scale;
      }
    }
    return DEFAULT_SCALE;
  }
}
