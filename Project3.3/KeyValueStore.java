import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
	private HashMap<String, ArrayList<StoreValue>> store = null;
	private static HashMap<String, KeyHandler> handlehp = new HashMap<String, KeyHandler>();
	int i = 0;

	public KeyValueStore() {
		store = new HashMap<String, ArrayList<StoreValue>>();
	}

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
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
				final String consistency = map.get("consistency");
				final Integer region = Integer.parseInt(map.get("region"));
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				final Long newtimestamp = Skews.handleSkew(timestamp, Integer.valueOf(region));
			///	System.out.println("put operation"+"\t"+"key="+key+"\t"+"value="+value+"\t"+"timestamp="+newtimestamp+"\t");
				if(consistency.equals("eventual")){
					if(keyValueStore.store.get(key)==null){	//when key no exits in hashmap
						keyValueStore.store.put(key, new ArrayList<StoreValue>());
                        keyValueStore.store.get(key).add(new StoreValue(newtimestamp, value));
					}
					else{//when key exits in hashmap
                        keyValueStore.store.get(key).add(new StoreValue(newtimestamp, value));
					}
				}
				else{
					Thread tp = new Thread(new Runnable(){//actually put action
						@Override
						public void run(){
				///			 System.out.println("put"+"\t"+"timestamp="+newtimestamp);
							//StoreValue sv = new StoreValue(newtimestamp, value);
							if(keyValueStore.store.get(key)==null){	//when key no exits in hashmap
								keyValueStore.store.put(key, new ArrayList<StoreValue>());
		                    //    keyValueStore.store.get(key).add(new StoreValue(newtimestamp, value));
							}
							//else{//when key exits in hashmap
		                        keyValueStore.store.get(key).add(new StoreValue(newtimestamp, value));
							//}
							 System.out.println("put completed"+"\t"+"timestamp="+newtimestamp);
						}
					});
					handlehp.get(key).keyqueue.add(new Request(newtimestamp, tp));
                    if (consistency.equals("causal")){
                    	handlehp.get(key).takelock = 1;//do not wait complete for unlock
                    }

				}
				//add thread to queue to order them
				String response = "stored";
				req.response().putHeader("Content-Type", "text/plain");
				req.response().putHeader("Content-Length", String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String consistency = map.get("consistency");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				/// System.out.println("get operation"+"\t"+"key="+key+"\t"+"timestamp="+timestamp);
				Thread tg = new Thread(new Runnable(){
					public void run(){
						 //System.out.println("get"+"\t"+"timestamp="+timestamp);
						ArrayList<StoreValue> values = keyValueStore.store.get(key);
						//System.out.println("get thread");
						String response = "";
						if (values != null) {
							for (StoreValue val : values) {
								response = response + val.getValue() + " ";
							}
						}
						/// System.out.println("get result"+"\t"+"key="+key+"\t"+"timestamp="+timestamp+"\n"+response);
						req.response().putHeader("Content-Type", "text/plain");
						if (response != null)
							req.response().putHeader("Content-Length", String.valueOf(response.length()));
						req.response().end(response);
						req.response().close();
					}
				});
                if (consistency.equals("strong")){
					handlehp.get(key).keyqueue.add(new Request(timestamp, tg));
                }
                //else if(consistency.equals("causal")){
				//	tg.start();
                //}
                else{
                	tg.run();
                }
			}
		});
		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				if(handlehp.get(key)==null){
					handlehp.put(key,new KeyHandler());
					Thread tr = new Thread(handlehp.get(key));
					tr.start();//open a thread to take keyqueue all the time
				}
				// lock key level queue
				handlehp.get(key).takelock++;
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));
				// unlock key level queue
				handlehp.get(key).takelock--;
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				 System.out.println(++i+"==============================================================");
				keyValueStore.store.clear();
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length", String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}

	public class Request {
		public Long timestamp = null;
		public Thread execute = null;

		public Request(Long timestamp, Thread execute) {
			this.timestamp = timestamp;
			this.execute = execute;
		}
	}

	public class KeyHandler implements Runnable {
		// Semaphore queuelock;
		PriorityBlockingQueue<Request> keyqueue;
		int takelock;

		public KeyHandler() {
			// this.queuelock = new Semaphore(1);
			this.keyqueue = new PriorityBlockingQueue<Request>(1024, new Cmp());
			this.takelock = 0;//0 is unlock, >0 is lock
		}

		@Override
		public void run() {
			try {
				while (true) {
					if (keyqueue.size() == 0 || takelock != 0) {
						// if(keyqueue.size()==0){
						// Thread.sleep(100);
						continue;
					} else if (keyqueue.size() != 0 && takelock == 0) {
						// else if(keyqueue.size()!=0){
						// System.out.println(keyqueue.size());
						// System.out.println("+++++++++"+keyqueue.take().timestamp+"++++++++++");
						//	Thread.sleep(600);
						keyqueue.take().execute.run();
					} else {
						// Thread.sleep(200);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public static class Cmp implements Comparator<Request> {
		public int compare(Request x, Request y) {
			int ret = x.timestamp.compareTo(y.timestamp);
			if (ret < 0) {
				return -1;
			}
			if (ret > 0) {
				return 1;
			}
			return 0;
		}
	}
}
