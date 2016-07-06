package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import org.json.JSONArray;

import java.sql.*;
import java.util.List;

public class ProfileServlet extends HttpServlet {

    public ProfileServlet() {

    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");
        String name = "Unauthorized";
        String profile = "#";
        String loginsql = "select * from users where uid=" + id; 
        String infosql = "select * from userinfo where uid=" + id;
        
        //connect to database
        try{
        	Class.forName("com.mysql.jdbc.Driver");
            Connection conn;
            conn = DriverManager.getConnection("jdbc:mysql://localhost/userdb?" +  "user=root&password=15319project");
            Statement stmt = conn.createStatement(); 
            ResultSet loginrs = stmt.executeQuery(loginsql);
            if(loginrs.equals(null)){
            	//System.out.println("UserID does not exist!");
            }
            else{
            	//System.out.println("get name and profile");
            	String[] userinfo;
            	ResultSet infos = stmt.executeQuery(infosql);
            	
            	if(infos.next()){
            		result.put("name", infos.getString(2));
                    result.put("profile", infos.getString(3));
            	}
            	
            }
            conn.close();
        }
        catch (Exception exception){
        	exception.printStackTrace();
        }
       
        System.out.println(result);
  
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