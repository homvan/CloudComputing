import java.io.*;
import java.net.*;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.lang.*;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collection;



public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 4;
	private final ServerSocket socket;
	private DataCenterInstance[] instances;
	private Float[] CPU_uti = new Float[3];
	private boolean[] healthstate;
	private int[] queue = new int[3];
	
	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances, boolean[] healthstate) {
		this.socket = socket;
		this.instances = instances;
		this.healthstate = healthstate;
	}

	// Complete this function
	public void start() throws Exception {
		
		/********************This part for Round Robin Test*************************/
		//RR();
		/******************Round Robin Test End*************************************/


		/******************This part for Custom Load Balancing Test*****************/
		//Custom();
		/*****************Custom Load Balancing Test End****************************/
		
		/*****************Health Check**********************************************/
		
		HC_test();
		
		/*************************Health Check End**********************************/

		/*************************Senior System Architect Test**********************/
		//SAT();
		/*************************Senior System Architect Test End******************/

	}
	//Round Robin
	private void RR() throws IOException{
		Runnable requestHandler;
		int i = 0;
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		while (true) {
			requestHandler = new RequestHandler(socket.accept(), instances[i]);
			executorService.execute(requestHandler);
			i += 1;
			if(i>2){
				i = 0;
			}
		}


	}
	
	//Health Check Test
	private void HC_test() throws Exception{
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		Runnable requestHandler;
		int i=0;
		while(true){
			if(healthstate[i]){
				requestHandler = new RequestHandler(socket.accept(), instances[i]);
				executorService.execute(requestHandler);
			}
			
			i += 1;
			if(i>2){
				i = 0;
			}
		
		}
		


	}
	//Custom Load Balancing
	private void Custom() throws Exception{
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		Runnable requestHandler;
		while (true) {
			int i = 10;
			if(i > 9){
				try{
					get_CPU();
				}
				catch(Exception e){}
				i = 0;
			}
			int least;
			least = get_least();
			
			//Change the priory queue of instances per 10 requests
			
			try{
				requestHandler = new RequestHandler(socket.accept(), instances[least]);
	                	executorService.execute(requestHandler);
        	        	requestHandler = new RequestHandler(socket.accept(), instances[0]);
                		executorService.execute(requestHandler);
	                	requestHandler = new RequestHandler(socket.accept(), instances[1]);
        	        	executorService.execute(requestHandler);
                		requestHandler = new RequestHandler(socket.accept(), instances[2]);
                        	executorService.execute(requestHandler);
			}
			catch(Exception e){}
			
			i++;
				
			
			
		
		}



	}
	// Senior System Architect Test
	private void SAT() throws Exception{
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		Runnable requestHandler;
		while (true) {
		
			int least;
			int i = 10;
			if(i>9){
				get_CPU();
				i = 0;
			}
			least = get_least();
			
			//Change the priory queue of instances per 10 requests
			
			requestHandler = new RequestHandler(socket.accept(), instances[least]);
	               	executorService.execute(requestHandler);
        	       	for(int j=0;j<3;j++){
				if(healthstate[j]){
					requestHandler = new RequestHandler(socket.accept(), instances[j]);
					executorService.execute(requestHandler);
					requestHandler = new RequestHandler(socket.accept(), instances[j]);
					executorService.execute(requestHandler);
				}
			}
			
			i++;
		
		}
	}
	//This function used in Custom Load Balancing Test to find the least loaded instance by considering CPU utilization
	private void get_CPU() throws Exception{
		String url_suffix = ":8080/info/cpu";
                String index;
                String tmp;
                String uti;
		// get the cpu utilization
			
		for (int i=0; i<3; i++){
			if (healthstate[i]){
				while (true){
					try{
	                        	        URL url = new URL(instances[i].getUrl() + url_suffix);
        	                	        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
                	        	        index = in.readLine();
                        	 	        tmp = index.split("body>")[1];
                                		uti = tmp.substring(0,tmp.length()-2);
						in.close();
						break;
                        		}
                        		catch(Exception e){continue;}
                    

				}
			
               			try{
                        		CPU_uti[i] = Float.parseFloat(uti);
					//System.out.println(CPU_uti[i]);
                        	}
                        	catch(Exception e){continue;}
			}
			else{
				CPU_uti[i] = Float.parseFloat("101");
			}
			
                }
	}
	//chose the least loaded instance
	private int get_least(){
		
		if (CPU_uti[0] <= CPU_uti[1]){
			if(CPU_uti[0]<=CPU_uti[2]){
				return 0;
			}
			else{
				return 2;


			}
		}
		else{
			if(CPU_uti[1] <= CPU_uti[2]){
				return 1;	
			}
			else{
				return 2;
			}
		}
		 
	}

	

	
	
	
	

}
