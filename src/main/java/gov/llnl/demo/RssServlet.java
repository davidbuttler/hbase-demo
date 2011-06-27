package gov.llnl.demo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


public class RssServlet
    extends HttpServlet {

private static final long serialVersionUID = 1L;
public static final String PLAIN = "text/plain";

public static final byte[] RSS_URLS = Bytes.toBytes("rss_urls");

public static final byte[] tags = Bytes.toBytes("tags");

/**
 * maintain the map of URL to user supplied tags in memory
 */
private static Map<String, Set<String>> urls = Maps.newHashMap();

private Configuration config;

private Splitter cSplitter = Splitter.on(',').omitEmptyStrings().trimResults();

private HTable rssTable;

@Override
public void init(ServletConfig servletConfig)
    throws ServletException
{
  urls = Maps.newHashMap();
  try {
    Gson g = new Gson();
    Type collectionType = new TypeToken<Set<String>>() {
    }.getType();

    config = getStaticConf();
    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(RSS_URLS)) {
      createTable(admin);
    }
    rssTable = new HTable(config, RSS_URLS);
    System.out.println("scanning full table:");
    Scan s = new Scan();
    ResultScanner scanner = rssTable.getScanner(s);
    for (Result row = scanner.next(); row != null; row = scanner.next()) {
      Set<String> tagSet = null;
      try {
        byte[] val = row.getValue(tags, tags);
        ByteArrayInputStream bis = new ByteArrayInputStream(val);
        InputStreamReader r = new InputStreamReader(bis);
        tagSet = g.fromJson(r, collectionType);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      if (tagSet == null) {
        tagSet = Sets.newHashSet();
      }
      String key = new String(row.getRow(), Charsets.UTF_8);
      urls.put(key, tagSet);

    }

  }
  catch (IOException e) {
    e.printStackTrace();
    throw new ServletException(e);
  }
  finally {
  }

}

private void createTable(HBaseAdmin admin)
    throws IOException
{
  HTableDescriptor desc = new HTableDescriptor(RSS_URLS);
  HColumnDescriptor family = new HColumnDescriptor(tags, 1, "GZ", false, false, HColumnDescriptor.DEFAULT_TTL,
      HColumnDescriptor.DEFAULT_BLOOMFILTER);
  desc.addFamily(family);
  admin.createTable(desc);
}

@Override
public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
{
  String action = request.getParameter("action");
  if (action != null && action.equals("add")) {
    String url = request.getParameter("url");
    url = URLDecoder.decode(url, "UTF-8");
    String tags = request.getParameter("tags");
    tags = URLDecoder.decode(tags, "UTF-8");
    Set<String> tagSet = Sets.newHashSet(cSplitter.split(tags));
    addURL(url, tagSet);
  }

  response.setCharacterEncoding(Charsets.UTF_8.name());
  response.setContentType(PLAIN);
  PrintWriter out = response.getWriter();
  String s = getURLs();
  out.println(s);
}

private String getURLs()
{
  Gson g = new Gson();
  String s = null;
  synchronized (urls) {
    s = g.toJson(urls);
  }
  return s;
}

private void addURL(String url, Set<String> tags)
{
  if (tags == null) {
    tags = Sets.newHashSet();
  }
  synchronized (urls) {
    Set<String> oldTags = urls.get(url);
    if (oldTags != null) {
      oldTags.addAll(tags);
      writeURLs(url, oldTags);
    }
    else {
      urls.put(url, tags);
      writeURLs(url, tags);
    }

  }
}

private void writeURLs(String url, Set<String> tagSet)
{
  try {
    Gson g = new Gson();
    String value = g.toJson(tagSet);
    Put p= new Put(url.getBytes());
    p.add(tags, tags, value.getBytes());
    rssTable.put(p);
  }
  catch (IOException e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }

}

@Override
protected void doPost(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse)
    throws ServletException, IOException
{
  doGet(httpServletRequest, httpServletResponse);
}


public static Configuration getStaticConf()
    throws IOException
{
  Configuration config = HBaseConfiguration.create();
  return config;
}
}
