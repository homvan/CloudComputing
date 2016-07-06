package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import cc.cmu.edu.minisite.ProfileServlet;

import org.json.JSONArray;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class FollowerServlet extends HttpServlet {

    public FollowerServlet() {

    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
    	
        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        
        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "172.31.4.82:2181");
        cfg.set("hbase.master", "172.31.4.82:60000");
        HTablePool tp = new HTablePool(cfg, 800);
    	JSONArray followers = new JSONArray();
    	
        System.out.println("Configurtoin success");
        TreeMap<Integer, String> treemap = new TreeMap<Integer, String>();
        try {  
        	HTableInterface htableInterface = tp.getTable("relation");
        	System.out.println("get relation table");
        	Get get = new Get(Bytes.toBytes(id));  
            Result rs = htableInterface.get(get);  
            if (rs.raw().length == 0) {  
                System.out.println( id + "doesn't exist");  
  
            } 
            else {  
            	System.out.println( id + "get!"); 
                for (KeyValue kv : rs.raw()) {  
                	String followerlist = new String(kv.getValue());
                    String[] followerArray = followerlist.split(",");
                    for (String followerid: followerArray){
                    	try{
                    		Class.forName("com.mysql.jdbc.Driver");
                            Connection conn;
                            conn = DriverManager.getConnection("jdbc:mysql://localhost/userdb?" +  "user=root&password=15319project");
                            Statement stmt = conn.createStatement(); 
                        	String followersql = "select * from userinfo where uid=" + followerid;
                        	ResultSet infos = stmt.executeQuery(followersql);
                        	//JSONObject follower = new JSONObject();
                        	if(infos.next()){
                        		treemap.put(infos.getString(2), infos.getString(3));
                        	}
                            //followers.put(follower);
                            conn.close();
                    	}
                    	catch (Exception exception){
                        	exception.printStackTrace();
                        }
                    	
                    }	
                }  
                
            }
            htableInterface.close();
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>(treemap.entrySet());
        
        Collections.sort(list, new Comparator<Map.Entry<String, String>>(){
            public int compare(Entry<String, String> o1, Entry<String, String> o2) {
                if (o1.getKey().compareTo(o2.getKey()) != 0) {
                    return o1.getKey().compareTo(o2.getKey());
                } else {
                    return o1.getValue().compareTo(o2.getValue());
                }
            }
        });
        
		for (Map.Entry<String, String> entry : list) {
			JSONObject follower = new JSONObject();
	        follower.put("name", entry.getKey());
	        follower.put("profile", entry.getValue());
	        followers.put(follower);
		}
        result.put("followers", followers);
        
        

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
        
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }

}
