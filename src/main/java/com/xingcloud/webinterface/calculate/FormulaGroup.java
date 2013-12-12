package com.xingcloud.webinterface.calculate;

import static com.xingcloud.webinterface.cache.MemoryCachedObjects.MCO_FORMULA;
import static com.xingcloud.webinterface.calculate.Formula.buildDefaultFormula;
import static com.xingcloud.webinterface.calculate.Formula.buildLowerFormula;
import static com.xingcloud.webinterface.calculate.Formula.buildRangeFormula;
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
 * User: Z J Wu Date: 13-12-11 Time: 下午4:53 Package: com.xingcloud.webinterface.calculate
 */
public class FormulaGroup {

  private Formula defaultFormula;

  private List<Formula> formulaList;

  public static FormulaGroup buildFormulaGroup(String rawFormula) throws FormulaException, MemCacheException {
    XMemCacheManager xMemCacheManager = MemoryCachedObjects.getInstance().getCacheManager();
    FormulaGroup formulaGroup = xMemCacheManager.getCacheElement(MCO_FORMULA, rawFormula, FormulaGroup.class);
    if (formulaGroup != null) {
      System.out.println("Cache loaded");
      return formulaGroup;
    }
    String[] rawFormulaArr = StringUtils.split(rawFormula, V9_SPLITTOR);
    if (ArrayUtils.isEmpty(rawFormulaArr)) {
      throw new FormulaException("Empty formula");
    }
    formulaGroup = new FormulaGroup();
    formulaGroup.defaultFormula = buildDefaultFormula(rawFormulaArr[0]);

    if (rawFormulaArr.length == 1) {
      xMemCacheManager.putCacheElement(MCO_FORMULA, rawFormula, formulaGroup);
      return formulaGroup;
    }

    List<Formula> formulaList = new ArrayList<Formula>(rawFormula.length() - 1);
    String formulaPart, formulaPartDateNode, formulaPartDateNodeNext = null;
    int idx;

    for (int i = 1; i < rawFormulaArr.length - 1; i++) {
      idx = rawFormulaArr[i].indexOf(FORMULA_PART_SPLITTOR);
      formulaPartDateNode = rawFormulaArr[i].substring(0, idx);
      formulaPart = rawFormulaArr[i].substring(idx + 1);
      idx = rawFormulaArr[i + 1].indexOf(FORMULA_PART_SPLITTOR);
      formulaPartDateNodeNext = rawFormulaArr[i + 1].substring(0, idx);
      formulaList.add(buildRangeFormula(formulaPartDateNode, formulaPartDateNodeNext, formulaPart));
    }
    idx = rawFormulaArr[rawFormulaArr.length - 1].indexOf(FORMULA_PART_SPLITTOR);
    formulaPart = rawFormulaArr[rawFormulaArr.length - 1].substring(idx + 1);
    if (formulaPartDateNodeNext == null) {
      formulaPartDateNodeNext = rawFormulaArr[rawFormulaArr.length - 1].substring(0, idx);
    }
    formulaList.add(buildLowerFormula(formulaPartDateNodeNext, formulaPart));
    formulaGroup.formulaList = formulaList;
    xMemCacheManager.putCacheElement(MCO_FORMULA, rawFormula, formulaGroup);
    System.out.println("Cache putted.");
    return formulaGroup;
  }

  public String getFormula(String date) throws FormulaException {
    if (CollectionUtils.isEmpty(this.formulaList)) {
      return defaultFormula.getFormulaString();
    }
    String formula;
    for (Formula f : formulaList) {
      formula = f.accept(date);
      if (formula != null) {
        return formula;
      }
    }
    return defaultFormula.getFormulaString();
  }

}
