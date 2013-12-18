package com.xingcloud.webinterface.utils;

import static com.xingcloud.basic.Constants.INTERNAL_NA;
import static com.xingcloud.webinterface.enums.AggregationPolicy.ACCUMULATION;
import static com.xingcloud.webinterface.enums.AggregationPolicy.ACCUMULATION_EXTEND;
import static com.xingcloud.webinterface.enums.AggregationPolicy.AVERAGE;
import static com.xingcloud.webinterface.enums.AggregationPolicy.AVERAGE_EXTEND;
import static com.xingcloud.webinterface.enums.AggregationPolicy.QUERY;
import static com.xingcloud.webinterface.enums.AggregationPolicy.SAME_AS_QUERY;
import static com.xingcloud.webinterface.enums.AggregationPolicy.SAME_AS_QUERY_EXTEND;
import static com.xingcloud.webinterface.enums.AggregationPolicy.SAME_AS_TOTAL;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.xingcloud.webinterface.enums.AggregationPolicy;

public class WebInterfaceConstants {
  public static final String FORMULA_ARITY_X = "x";
  public static final String FORMULA_ARITY_Y = "y";
  public static final char FORMULA_PART_SPLITTOR = ':';
  public static final char V9_SPLITTOR = '|';

  public static final String SEGMENT_IN_SEPARATOR = "[\\s]*\\|[\\s]*";
  public static final char SEGMENT_FUNCTION_BEGIN_CHAR = '$';
  public static final String SEGMENT_OP = "op";
  public static final String SEGMENT_TYPE = "type";
  public static final String SEGMENT_EXPR = "expr";
  public static final char SEGMENT_VARIABLE_BOUNDARY_CHAR = '`';

  /* 小时和5分钟
   * Index    Distance>1  HasSegment  HasNumberOfDay  ACCU     TOTAL                   NATURAL
   * 0000(0)  F           F           F               F        SAME_AS_QUERY_EXTEND    SAME_AS_TOTAL
   * 0001(1)  F           F           F               T        ACCUMULATION            SAME_AS_TOTAL
   * 0010(2)  F           F           T               F        N/A                     N/A
   * 0011(3)  F           F           T               T        N/A                     N/A
   *
   * 0100(4)  F           T           F               F        SAME_AS_QUERY_EXTEND    SAME_AS_QUERY_EXTEND
   * 0101(5)  F           T           F               T        SAME_AS_QUERY_EXTEND    ACCUMULATION
   * 0110(6)  F           T           T               F        N/A                     N/A
   * 0111(7)  F           T           T               T        N/A                     N/A
   *
   * 1000(8)  T           F           F               F        QUERY                   SAME_AS_TOTAL
   * 1001(9)  T           F           F               T        ACCUMULATION            SAME_AS_TOTAL
   * 1010(a)  T           F           T               F        N/A                     N/A
   * 1011(b)  T           F           T               T        N/A                     N/A
   *
   * 1100(c)  T           T           F               F        QUERY                   QUERY
   * 1101(d)  T           T           F               T        ACCUMULATION_EXTEND     ACCUMULATION
   * 1110(e)  T           T           T               F        N/A                     N/A
   * 1111(f)  T           T           T               T        N/A                     N/A
   */
  public static final AggregationPolicy[] MIN_HOUR_SUMMARY_POLICY_ARR_TOTAL = new AggregationPolicy[]{
    SAME_AS_QUERY_EXTEND, ACCUMULATION, null, null, SAME_AS_QUERY_EXTEND, SAME_AS_QUERY_EXTEND, null, null, QUERY,
    ACCUMULATION, null, null, QUERY, ACCUMULATION_EXTEND, null, null
  };

  public static final AggregationPolicy[] MIN_HOUR_SUMMARY_POLICY_ARR_NATURAL = new AggregationPolicy[]{SAME_AS_TOTAL,
                                                                                                        SAME_AS_TOTAL,
                                                                                                        null, null,
                                                                                                        SAME_AS_QUERY_EXTEND,
                                                                                                        ACCUMULATION,
                                                                                                        null, null,
                                                                                                        SAME_AS_TOTAL,
                                                                                                        SAME_AS_TOTAL,
                                                                                                        null, null,
                                                                                                        QUERY,
                                                                                                        ACCUMULATION,
                                                                                                        null, null
  };

  /* 天(Interval==DAY/WEEK/MONTH)
   * Index    Distance>1  HasSegment  HasNumberOfDay  ACCU     TOTAL                  NATURAL
   * 0000(0)  F           F           F               F        SAME_AS_QUERY          SAME_AS_TOTAL
   * 0001(1)  F           F           F               T        SAME_AS_QUERY          SAME_AS_TOTAL
   * 0010(2)  F           F           T               F        SAME_AS_QUERY          SAME_AS_TOTAL
   * 0011(3)  F           F           T               T        SAME_AS_QUERY          SAME_AS_TOTAL
   *
   * 0100(4)  F           T           F               F        SAME_AS_QUERY_EXTEND   SAME_AS_QUERY
   * 0101(5)  F           T           F               T        SAME_AS_QUERY_EXTEND   SAME_AS_QUERY
   * 0110(6)  F           T           T               F        SAME_AS_QUERY_EXTEND   SAME_AS_QUERY
   * 0111(7)  F           T           T               T        SAME_AS_QUERY_EXTEND   SAME_AS_QUERY
   *
   * 1000(8)  T           F           F               F        QUERY                  SAME_AS_TOTAL
   * 1001(9)  T           F           F               T        ACCUMULATION           SAME_AS_TOTAL
   * 1010(a)  T           F           T               F        AVERAGE                SAME_AS_TOTAL
   * 1011(b)  T           F           T               T        AVERAGE                SAME_AS_TOTAL
   *
   * 1100(c)  T           T           F               F        QUERY                  QUERY
   * 1101(d)  T           T           F               T        ACCUMULATION_EXTEND    ACCUMULATION
   * 1110(e)  T           T           T               F        AVERAGE_EXTEND         AVERAGE
   * 1111(f)  T           T           T               T        AVERAGE_EXTEND         AVERAGE
   */
  public static final AggregationPolicy[] PERIOD_SUMMARY_POLICY_ARR_TOTAL = new AggregationPolicy[]{SAME_AS_QUERY,
                                                                                                    SAME_AS_QUERY,
                                                                                                    SAME_AS_QUERY,
                                                                                                    SAME_AS_QUERY,
                                                                                                    SAME_AS_QUERY_EXTEND,
                                                                                                    SAME_AS_QUERY_EXTEND,
                                                                                                    SAME_AS_QUERY_EXTEND,
                                                                                                    SAME_AS_QUERY_EXTEND,
                                                                                                    QUERY, ACCUMULATION,
                                                                                                    AVERAGE, AVERAGE,
                                                                                                    QUERY,
                                                                                                    ACCUMULATION_EXTEND,
                                                                                                    AVERAGE_EXTEND,
                                                                                                    AVERAGE_EXTEND
  };

  public static final AggregationPolicy[] PERIOD_SUMMARY_POLICY_ARR_NATURAL = new AggregationPolicy[]{SAME_AS_TOTAL,
                                                                                                      SAME_AS_TOTAL,
                                                                                                      SAME_AS_TOTAL,
                                                                                                      SAME_AS_TOTAL,
                                                                                                      SAME_AS_QUERY,
                                                                                                      SAME_AS_QUERY,
                                                                                                      SAME_AS_QUERY,
                                                                                                      SAME_AS_QUERY,
                                                                                                      SAME_AS_TOTAL,
                                                                                                      SAME_AS_TOTAL,
                                                                                                      SAME_AS_TOTAL,
                                                                                                      SAME_AS_TOTAL,
                                                                                                      QUERY,
                                                                                                      ACCUMULATION,
                                                                                                      AVERAGE, AVERAGE
  };

  public static final double DEFAULT_SAMPLING_RATE = -1d;

  public static final String INCREMENTAL_STRING = "INCREMENTAL";
  public static final String VOLATILE_STRING = "VOLATILE";

  // Placeholders
  public static final String NOT_AVAILABLE_STRING = INTERNAL_NA;
  public static final String NOT_AVAILABLE_NUMBER = "NotAvailableNumber";
  public static final String NOT_AVAILABLE_KEY = "NotAvailablePlaceholder";
  public static final String PENDING_STRING = "pending";
  public static final String PENDING_NUMBER = "PendingNumber";
  public static final String PENDING_KEY = "PendingPlaceholder";

  public static final boolean CACHE_TRANSIENT = false;

  public static final boolean CACHE_MEMORY = false;

  public static final int CACHE_OBJ_NUM_5K = 5000;

  public static final int CACHE_DURATION_5MIN = 300;
  public static final int CACHE_DURATION_10MIN = 600;

  public static final String DEFAULT_PROJECT_ID_IDENTIFIER = "DEFAULT_PROJECT";
  public static final String TOTAL_USER = "TOTAL_USER";
  public static final String TOTAL_EVENT = "*";

  public static final int PAGE_SIZE = 10;
  public static final int PAGE_INDEX_DEFAULT = 0;
  public static final String TBL_USER_PROPERTIES_PROJECT_ID = "project_id";

  public static final Gson DEFAULT_GSON_PLAIN = new GsonBuilder().serializeNulls()
                                                                 .excludeFieldsWithoutExposeAnnotation().create();
  public static final Gson DEFAULT_GSON_PRETTY = new GsonBuilder().setPrettyPrinting().serializeNulls()
                                                                  .excludeFieldsWithoutExposeAnnotation().create();

  public static final String CACHE_PREFIX_UIC = "ui.check.";

  public static final String THREAD_NAME_PREFIX_XQUERY = "XqueryThread";

  public static final String HTTP = "HTTP";

  public static final String SQL_TABLE_NAME_SUFFIX_EVENT = "_deu";
  public static final String SQL_TABLE_NAME_PREFIX_USER = "fix_";

  public static final char GENERIC_SEPARATOR = '\001';

}
