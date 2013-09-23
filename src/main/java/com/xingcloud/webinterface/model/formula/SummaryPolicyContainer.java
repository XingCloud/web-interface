package com.xingcloud.webinterface.model.formula;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.enums.AggregationPolicy;
import com.xingcloud.webinterface.enums.CommonQueryType;

public class SummaryPolicyContainer {

  @Expose
  @SerializedName("TSP")
  private AggregationPolicy totalSummaryPolicy;

  @Expose
  @SerializedName("NSP")
  private AggregationPolicy naturalSummaryPolicy;

  public SummaryPolicyContainer() {
    super();
  }

  public SummaryPolicyContainer(AggregationPolicy totalSummaryPolicy, AggregationPolicy naturalSummaryPolicy) {
    super();
    this.totalSummaryPolicy = totalSummaryPolicy;
    this.naturalSummaryPolicy = naturalSummaryPolicy;
  }

  public AggregationPolicy getTotalSummaryPolicy() {
    return totalSummaryPolicy;
  }

  public void setTotalSummaryPolicy(AggregationPolicy totalSummaryPolicy) {
    this.totalSummaryPolicy = totalSummaryPolicy;
  }

  public AggregationPolicy getNaturalSummaryPolicy() {
    return naturalSummaryPolicy;
  }

  public void setNaturalSummaryPolicy(AggregationPolicy naturalSummaryPolicy) {
    this.naturalSummaryPolicy = naturalSummaryPolicy;
  }

  @Override
  public String toString() {
    return "SPC.[T=" + totalSummaryPolicy + ",N=" + naturalSummaryPolicy + "]";
  }

  public AggregationPolicy getPolicyByCommonQueryType(CommonQueryType cqt) {
    switch (cqt) {
      case TOTAL:
        return getTotalSummaryPolicy();
      case NATURAL:
        return getNaturalSummaryPolicy();
      default:
        return null;
    }
  }

}
