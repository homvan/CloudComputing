package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
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

import org.json.JSONArray;
import org.json.JSONObject;

public class HomepageServlet extends HttpServlet {
	static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    public HomepageServlet() {

    }
    
    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
    	String id = request.getParameter("id");
    	DynamoDBMapper mapper = new DynamoDBMapper(client);
    	
        JSONObject result = new JSONObject();
        JSONArray postsresult = new JSONArray();
        System.out.println("Find posts of " +id);
        
        int UserID = Integer.parseInt(id);
        Post requestKey = new Post();
        DynamoDBQueryExpression<Post> queryExpression = new DynamoDBQueryExpression<Post>()
				.withHashKeyValues(requestKey);

		List<Post> queryResults = mapper.query(Post.class, queryExpression);
        HashMap<String, String> postMap = new HashMap<String, String>();
       
        
        for (Post post : queryResults) {
        	postMap.put(post.getTimestamp(), post.getPost());
        }
        //sort by timestamp
        List<Map.Entry<String, String>>postlist = new ArrayList<Map.Entry<String, String>>(postMap.entrySet());
		
		Collections.sort(postlist, new Comparator<Map.Entry<String, String>>(){
			@Override
			public int compare(Entry<String, String> o1, Entry<String, String> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
		
		for (Map.Entry<String, String> entry: postlist){
			JSONObject postjson = new JSONObject(entry.getValue());
			postsresult.put(postjson);
		}
        result.put("posts", postsresult);
        PrintWriter writer = response.getWriter();           
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }
    
    @DynamoDBTable(tableName="Posts")
	public static class Post {
	
		private int uid;
		private String timestamp;
		private String post;

		@DynamoDBHashKey(attributeName = "UserID")
		public int getId() {
			return uid;
		}

		public void setId(int id) {
			this.uid = id;
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
    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}