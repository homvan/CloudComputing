package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;


import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import org.json.JSONArray;
import org.json.JSONObject;

public class TimelineServlet extends HttpServlet {
    
	static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());


    public TimelineServlet() throws Exception {
        
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
    	
    	
        JSONObject result = new JSONObject();
        String id = request.getParameter("id");
        String infosql = "select * from userinfo where uid=" + id;
        
        //connect to database
        try{
        	Class.forName("com.mysql.jdbc.Driver");
            Connection conn;
            conn = DriverManager.getConnection("jdbc:mysql://localhost/userdb?" +  "user=root&password=15319project");
            Statement stmt = conn.createStatement(); 
            String[] userinfo;
        	ResultSet infos = stmt.executeQuery(infosql);
        	
        	if(infos.next()){
        		result.put("name", infos.getString(2));
                result.put("profile", infos.getString(3));
        	}
            
            	
      
            conn.close();
        }
        catch (Exception exception){
        	exception.printStackTrace();
        }
        
        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "172.31.4.82:2181");
        cfg.set("hbase.master", "172.31.4.82:60000");
        HTablePool tp = new HTablePool(cfg, 800);
    	JSONArray followers = new JSONArray();
    	
        System.out.println("Configurtoin success");
        TreeMap<String, String> treemap = new TreeMap<String, String>();
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
	        
	        //get followee's ids
        String[] followeeArray = null;
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
                	String followeelist = new String(kv.getValue());
                	followeeArray = followeelist.split(",");
                }
            }
        }
        catch (Exception e) {
				// TODO: handle exception
        }
	        
	        //find posts from dynamoDB
			DynamoDBMapper mapper = new DynamoDBMapper(client);
			List<String> postlist = new ArrayList<String>();
			
			Post post = new Post();
			for (String followeeid:followeeArray) {
				int fid = Integer.parseInt(followeeid);
				post.setId(fid);

				DynamoDBQueryExpression<Post> queryExpression = new DynamoDBQueryExpression<Post>()
						.withHashKeyValues(post);

				List<Post> tmpReply = mapper.query(Post.class, queryExpression);

				for (Post reply : tmpReply) {
					postlist.add(reply.getPost());
				}
			}
			JSONArray postArray = new JSONArray();
			Collections.sort(postlist, new myComparator());
			
			int i;
			if(postlist.size() <= 30){
				i = 0;
			}
			else{
				for (i = postlist.size() - 30; i < postlist.size(); i++) {
					String e = postlist.get(i);
					JSONObject post_info = new JSONObject(e);
					postArray.put(post_info);
				}
			}
			
			result.put("posts", postArray);

        // implement the functionalities in doGet method.
        // you can add any helper methods or classes to accomplish this task

        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }
    
    class myComparator implements Comparator<String> {

    	public int compare(String o1, String o2) {
    		String time1 = null;
    		String time2 = null;
    		time1 = new JSONObject(o1).getString("timestamp");
    		time2 = new JSONObject(o2).getString("timestamp");
    		
    		if (time1.compareTo(time2) != 0) {
    			return time1.compareTo(time2);
    		} 
    		else {
    			int pid1 = Integer.parseInt(new JSONObject(o1).getString("pid"));
    			int pid2 = Integer.parseInt(new JSONObject(o2).getString("pid"));

    			return pid1 - pid2;
    		}
    	}
    }
    
    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse responce) throws ServletException, IOException {
        doGet(request, responce);
    }
    
	@DynamoDBTable(tableName="Posts")
	public static class Post {
		private int Uid;
		private String timestamp;
		private String post;

		@DynamoDBHashKey(attributeName = "UserID")
		public int getId() {
			return Uid;
		}

		public void setId(int id) {
			this.Uid = id;
		}

		@DynamoDBRangeKey(attributeName = "Timestamp")
		public String getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(String timestamp) {
			this.timestamp = timestamp;
		}

		@DynamoDBAttribute(attributeName = "Post")
		public String getPost() {
			return post;
		}

		public void setPost(String post) {
			this.post = post;
		}
	}
}