import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {
	
	//Default mode: replication. Possible string values are "replication" and "sharding"
	private static String storageType = "replication";
	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-52-23-223-58.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-54-209-2-9.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-54-208-122-249.compute-1.amazonaws.com";
	
	/**
	 * Class OperationRequest store each request from client 
	 * The blockingqueue is formed by OperationRequests
	 * It contains: timestamp, key, and value(put)
	 */
	public class OperationRequest implements Comparable<OperationRequest>{

		private final String timestamp;
		private String value;
		private final String key;
		
		//Initiation
		//get request
		public OperationRequest(String timestamp, String key){
			this.timestamp = timestamp;
			this.key = key;
		}
		//put request
		public OperationRequest(String timestamp, String key, String value){
			this.timestamp = timestamp;
			this.key = key;
			this.value = value;
		}
		public int compareTo(OperationRequest or) {
                        return timestamp.compareTo(or.timestamp);
                }
		public String getRequest(){
			return "Request: " + timestamp + " " + key + " " + value;
		}
		
	}
	
	/**
	 * operationMap is a hash map
	 * in which key is the key of request
	 * value is the blockingqueue of each key in hashmap
	 * blockingqueue is a FIFO queue of request of each key
	 */
	private static HashMap<String, BlockingQueue<OperationRequest>> operationMap = new HashMap<String, BlockingQueue<OperationRequest>>();
	// Add request to queue
	public static synchronized void addRequest(String key, OperationRequest or){
		System.out.println("add " + or.getRequest() +" to queue");
		//if hashmap do not contain the key, add a new queue
		if(operationMap.containsKey(key) == false){
			operationMap.put(key, new PriorityBlockingQueue<OperationRequest>());
		}
		//put request into queue
		try{
			operationMap.get(key).put(or);
		}
		catch(InterruptedException e){}
		
	}
	
	/**
	 * For request of certain key
	 * keep waiting till the requests ahead in queue finished
	 */
	public static void requestWait(String key, OperationRequest or){
		System.out.println(or.getRequest() + " Waiting!");
		while (operationMap.get(key).peek() != or){
			try{
				synchronized(operationMap.get(key)){
					operationMap.get(key).wait();
				}
			}
			catch(InterruptedException e){}
		}
		System.out.println(or.getRequest() + " Operating!");
	}
	
	/**
	 * Remove request from queue after operating
	 */
	
	public static void removeRequest(String key, OperationRequest or){
		System.out.println("Remove "+or.getRequest()+" from queue ");
		try{
			synchronized(operationMap.get(key)){
				operationMap.get(key).take();
				operationMap.get(key).notifyAll();
			}
		}
		catch(InterruptedException e){}
	}
	
	/**
	 * Hash Function
	 * transfer key to number, and mod 3
	 * According to the remainder to choose datacenter
	 */
	
	public static String myHashFunc(String key){
				
		char[] cs = key.toCharArray();
		String dc_dns = null;
		int sum = 0;
		for(int i=0;i<cs.length;i++){
			sum += (int) cs[i];
		}
		int mod = (sum % 3) ;
		System.out.println(sum);
		System.out.println(mod);
		switch (mod){
			case 1:
				dc_dns = dataCenter1;
				break;
			case 2:
				dc_dns = dataCenter2;
				break;
			case 0:
				dc_dns = dataCenter3;
				break;
			default:
				break;
		}
		
		return dc_dns;
		
	}
	
	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis() 
                                                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
				
				//Add request to queue
				final OperationRequest operationRequest = new OperationRequest(timestamp, key, value);
				addRequest(key, operationRequest);
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for PUT operation here.
						//Each PUT operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.
						//wait former requests finished
						System.out.println("PUT!");
						requestWait(key, operationRequest);
						final String dc_dns = myHashFunc(key);
						if(storageType.equals("replication")){
							try{
								System.out.println("put " + key + " " + value + " to dataCenters");
							
								Thread dc_1 = new Thread(new Runnable(){
									public void run(){
										try{
											KeyValueLib.PUT(dataCenter1, key, value);
										}
										catch(Exception e){}
									}
								});
								dc_1.start();
								Thread dc_2 = new Thread(new Runnable(){
									public void run(){
										try{
											KeyValueLib.PUT(dataCenter2, key, value);
										}
										catch(IOException e){}
									}
								});
								dc_2.start();
								Thread dc_3 = new Thread(new Runnable(){
									public void run(){
										try{
											KeyValueLib.PUT(dataCenter3, key, value);
										}
										catch(IOException e){}
									}
								});
								dc_3.start();
							}
							catch(Exception e){}
						}
						
						if(storageType.equals("sharding")){
							
							try{
								
								System.out.println("put " + key + " " + value + " to dataCenter "+dc_dns);
							
								try{
									KeyValueLib.PUT(dc_dns, key, value);
								}
								catch(Exception e){}
								
							}
							catch(Exception e){}
						}
						
						
						//remove request from queue
						removeRequest(key, operationRequest);
					}
				});
				t.start();
				req.response().end(); //Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
			
				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis() 
								+ TimeZone.getTimeZone("EST").getRawOffset()).toString();
				
				//Add request to queue
				final OperationRequest operationRequest = new OperationRequest(timestamp, key);
				addRequest(key, operationRequest);
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for GET operation here.
                                                //Each GET operation is handled in a different thread.
                                                //Highly recommended that you make use of helper functions.
						
						//Wait former requests finished
						System.out.println("GET!");
						requestWait(key, operationRequest);
						String res = "0";
						final String dc_dns = myHashFunc(key);
						
						if(storageType.equals("replication")){
							String dc_dns_r;
							try{
								System.out.println("get " + key);
							
								if(loc.equals("1")){
									dc_dns_r = dataCenter1;
								}
								else if(loc.equals("2")){
									dc_dns_r = dataCenter2;
								}
								else{
									dc_dns_r = dataCenter3;
								}
								res = KeyValueLib.GET(dc_dns, key);
							}
							catch(Exception e){}
						}
						
						if(storageType.equals("sharding")){
							String dc_dns_s = null;
							if(loc.equals("1")){
								dc_dns_s = dataCenter1;
							}
							else if(loc.equals("2")){
								dc_dns_s = dataCenter2;
							}
							else if(loc.equals("3")){
								dc_dns_s = dataCenter3;
							}
							if(dc_dns_s.equals(dc_dns) || dc_dns_s==null){
								try{
									res = KeyValueLib.GET(dc_dns, key);
								}
								catch(Exception e){}
							}
							
							
						}
						
						
						//remove request from queue
						removeRequest(key, operationRequest);
				
						req.response().end(res); //Default response = 0
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
                        @Override
                        public void handle(final HttpServerRequest req) {
                                MultiMap map = req.params();
                                storageType = map.get("storage");
                                //This endpoint will be used by the auto-grader to set the 
				//consistency type that your key-value store has to support.
                                //You can initialize/re-initialize the required data structures here
                                req.response().end();
                        }
                });

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
}
