import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: causal, eventual, strong
	private static String consistencyType;

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String dataCenterUSE = "ec2-52-91-53-196.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-52-91-50-242.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-52-91-185-123.compute-1.amazonaws.com";

	private static final String coordinatorUSE = "ec2-54-85-201-23.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-52-91-38-70.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-52-23-193-101.compute-1.amazonaws.com";

	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);
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
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");
				///System.out.println("put operation"+"\t"+"key="+key+"\t"+"value="+value+"\t"
				///		+"timestamp="+timestamp+"\t"+"forwarded="+forwarded+"\t"+"forwardedregion="+forwardedRegion);
				Thread t = new Thread(new Runnable() {
					public void run() {
						try{	
						/* TODO: Add code for PUT request handling here
						 * Each operation is handled in a new thread.
						 * Use of helper functions is highly recommended */
							Long newtimestamp = 0L;
							int aimlocation = 0;
							//System.out.println("prepare to head");
                           // if (!consistencyType.equals("eventual")){
                           //	KeyValueLib.AHEAD(key, String.valueOf(timestamp));//tell dataceter there is a request
                           // }
							if(forwarded!=null){//do not need hash,give put requests to datacenter
	                            if (!consistencyType.equals("eventual")){
	                            	KeyValueLib.AHEAD(key, String.valueOf(timestamp));//tell dataceter there is a request
	                            }
								newtimestamp = Skews.handleSkew(timestamp, Integer.valueOf(forwardedRegion));
								KeyValueLib.PUT(dataCenterUSE, key, value, String.valueOf(newtimestamp), consistencyType);
								KeyValueLib.PUT(dataCenterUSW, key, value, String.valueOf(newtimestamp), consistencyType);
								KeyValueLib.PUT(dataCenterSING, key, value, String.valueOf(newtimestamp), consistencyType);
                                if (consistencyType.equals("strong")){
                                	KeyValueLib.COMPLETE(key, String.valueOf(newtimestamp));
                                }
							}
							else{//need hash
								//System.out.println("key="+key);
								aimlocation = findloc.hashfunction(key);
							///	System.out.println("aimlocation="+aimlocation);
								if(aimlocation==region){
		                            if (!consistencyType.equals("eventual")){
		                            	KeyValueLib.AHEAD(key, String.valueOf(timestamp));//tell dataceter there is a request
		                            }
									KeyValueLib.PUT(dataCenterUSE, key, value, String.valueOf(timestamp), consistencyType);
									KeyValueLib.PUT(dataCenterUSW, key, value, String.valueOf(timestamp), consistencyType);
									KeyValueLib.PUT(dataCenterSING, key, value, String.valueOf(timestamp), consistencyType);
								    if (consistencyType.equals("strong")){
								    	KeyValueLib.COMPLETE(key, String.valueOf(timestamp));
								    }
								}
								else{//forward request according aimlocation which is calculate by hashfunction
									if(aimlocation==1){
									///	System.out.println("forward to 1");
										KeyValueLib.FORWARD(coordinatorUSE, key, value, String.valueOf(timestamp));
									}
									else if(aimlocation==2){
									///	System.out.println("forward to 2");
										KeyValueLib.FORWARD(coordinatorUSW, key, value, String.valueOf(timestamp));
									}
									else{
									///	System.out.println("forward to 3");
										KeyValueLib.FORWARD(coordinatorSING, key, value, String.valueOf(timestamp));
									}
								}
							}
						}catch(Exception ex){
							
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				///System.out.println("get operation"+"\t"+"key="+key+"\t"+"timestamp="+timestamp);
				Thread t = new Thread(new Runnable() {
					public void run() {
						try{
							/* TODO: Add code for GET requests handling here
							 * Each operation is handled in a new thread.
							 * Use of helper functions is highly recommended */
								String response = "";
									if(region==1){
										response = KeyValueLib.GET(dataCenterUSE, key, String.valueOf(timestamp), consistencyType);
									}
									else if(region==2){
										response = KeyValueLib.GET(dataCenterUSW, key, String.valueOf(timestamp), consistencyType);
									}
									else{
										response = KeyValueLib.GET(dataCenterSING, key, String.valueOf(timestamp), consistencyType);
									}
								if(response.isEmpty()){
									response = "0";
								}
								req.response().end(response);
						}catch(Exception ex){
							ex.printStackTrace();
						}
					}
				});
				t.start();
			}
		});
		/* This endpoint is used by the grader to change the consistency level */
		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");
				req.response().end();
			}
		});
		/* BONUS HANDLERS BELOW */
		routeMatcher.get("/forwardcount", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().end(KeyValueLib.COUNT());
			}
		});

		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				KeyValueLib.RESET();
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

class findloc{
    public static int hashfunction(String key){
    	//System.out.println("hashfunction");
        int loc = 0;
        char[] temp = key.toCharArray();
        loc =3-(temp[0]+temp[temp.length-1])%3;
        return loc;
    }
}

