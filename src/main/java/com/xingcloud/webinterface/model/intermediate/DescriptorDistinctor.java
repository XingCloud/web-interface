package com.xingcloud.webinterface.model.intermediate;

import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public abstract class DescriptorDistinctor {

  protected abstract Collection<FormulaQueryDescriptor> distinctDescriptor() throws NecessaryCollectionEmptyException;

  protected Collection<FormulaQueryDescriptor> distinctFromItemResultMap(
    Map<String, ? extends ItemResult> itemResultMap) throws NecessaryCollectionEmptyException {
    if (MapUtils.isEmpty(itemResultMap)) {
      return null;
    }
    ItemResult itemResult;
    Collection<FormulaQueryDescriptor> subDescriptors;
    Collection<Collection<FormulaQueryDescriptor>> descriptors = new ArrayList<Collection<FormulaQueryDescriptor>>(
      itemResultMap.size());

    for (Entry<String, ? extends ItemResult> entry : itemResultMap.entrySet()) {
      itemResult = entry.getValue();
      subDescriptors = itemResult.distinctDescriptor();
      descriptors.add(subDescriptors);
    }
    return distinctFromCollections(descriptors);
  }

  protected Collection<FormulaQueryDescriptor> distinctFromCollection(Collection<FormulaQueryDescriptor> descriptors) {
    if (CollectionUtils.isEmpty(descriptors)) {
      return null;
    }
    Map<FormulaQueryDescriptor, FormulaQueryDescriptor> map = null;
    FormulaQueryDescriptor existsDescriptor;
    for (FormulaQueryDescriptor descriptor : descriptors) {
      if (descriptor == null || descriptor.isKilled()) {
        continue;
      }
      if (map == null) {
        map = new HashMap<FormulaQueryDescriptor, FormulaQueryDescriptor>();
      }
      existsDescriptor = map.get(descriptor);
      if (existsDescriptor == null) {
        map.put(descriptor, descriptor);
      } else {
        existsDescriptor.addFunctions(descriptor.getFunctions());
      }
    }
    return map == null ? null : map.values();
  }

  protected Collection<FormulaQueryDescriptor> distinctFromCollections(
    Collection<Collection<FormulaQueryDescriptor>> descriptors) {
    if (CollectionUtils.isEmpty(descriptors)) {
      return null;
    }

    Map<FormulaQueryDescriptor, FormulaQueryDescriptor> map = null;
    FormulaQueryDescriptor existsDescriptor;
    for (Collection<FormulaQueryDescriptor> subDescriptors : descriptors) {
      if (CollectionUtils.isEmpty(subDescriptors)) {
        continue;
      }

      if (map == null) {
        map = new HashMap<FormulaQueryDescriptor, FormulaQueryDescriptor>(subDescriptors.size() * descriptors.size());
      }

      for (FormulaQueryDescriptor descriptor : subDescriptors) {
        if (descriptor == null || descriptor.isKilled()) {
          continue;
        }
        existsDescriptor = map.get(descriptor);
        if (existsDescriptor == null) {
          map.put(descriptor, descriptor);
        } else {
          existsDescriptor.addFunctions(descriptor.getFunctions());
        }
      }
    }
    return map == null ? null : map.values();
  }
}
