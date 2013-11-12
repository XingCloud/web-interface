package com.xingcloud.webinterface.plan;

import com.caucho.hessian.client.HessianProxyFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.http.client.utils.URIBuilder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;

/**
 * User: Z J Wu Date: 13-8-9 Time: 下午4:12 Package: com.xingcloud.webinterface.plan
 */
public class TestLogicalPlanBase {
  protected static final String TEST_TABLE = "age";
  protected static final String TEST_EVENT = "pay.*";
  protected static final String TEST_EVENT_ALL = "*.*";
  protected static final String TEST_EVENT_VISIT = "visit.*";
  protected static final String TEST_EVENT2 = "Age.Menu.*.Success";
  protected static final String TEST_EVENT3 = "click.*.*.1";
  protected static final String TEST_REAL_BEGIN_DATE = "2013-11-01";
  protected static final String TEST_REAL_END_DATE = "2013-11-02";
  private static final HessianProxyFactory WS_FACTORY = new HessianProxyFactory();

  protected static Object SERVICE;

  static {
    URIBuilder builder = new URIBuilder();
    URI uri;

    builder.setScheme("http");
    builder.setHost("69.28.58.61");
    builder.setPort(8182);
    builder.setPath("/qm/q");

    try {
      uri = builder.build();
      SERVICE = WS_FACTORY.create(uri.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  protected LogicalPlan deserialize(String logicalPlanString) throws IOException {
    ObjectMapper mapper = Plans.DEFAULT_DRILL_CONFIG.getMapper();
    return mapper.readValue(logicalPlanString, LogicalPlan.class);
  }

  protected void write2File(String fileName, String plan) throws Exception {
    PrintWriter pw = null;
    String path = "src/test/resources/plans/" + fileName;
    File f = new File(path);
    System.out.println(f.getAbsolutePath());
    try {
      pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f)));
      pw.write(plan);
    } catch (Exception e) {
      throw e;
    } finally {
      if (pw != null) {
        pw.close();
      }
    }
  }

  public static void compress(InputStream is, OutputStream os) throws IOException {
    BZip2CompressorOutputStream gos = null;

    try {
      gos = new BZip2CompressorOutputStream(os);
      int count;
      byte data[] = new byte[32];
      while ((count = is.read(data, 0, 32)) != -1) {
        gos.write(data, 0, count);
      }
      gos.finish();
      gos.flush();
    } finally {
      IOUtils.closeQuietly(gos);
    }
  }

  public static String compress(String in) throws Exception {
    InputStream is = IOUtils.toInputStream(in);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String output = null;
    try {
      compress(is, baos);
      output = baos.toString();
      baos.flush();
    } finally {
      IOUtils.closeQuietly(is);
      IOUtils.closeQuietly(baos);
    }

    return output;
  }

  public static void main(String[] args) throws Exception {
    String s = compress("alandsvbjewkndvbgqowef0htg04[5yin[[40q68htoGIGHJPK3OUHRIGNJHTGOtpkrgouhnjrg3qoihfvbaenlk");
    System.out.println(s);
  }

}
