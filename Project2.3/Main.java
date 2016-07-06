import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.ConnectException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;


public class Main {
	private static final int PORT = 80;
	public static DataCenterInstance[] instances;
	private static ServerSocket serverSocket;
	public static boolean[] HealthState = new boolean[3];
	public static BasicAWSCredentials credential;
	//Update this list with the DNS of your data center instances
	static {
		instances = new DataCenterInstance[3];
		instances[0] = new DataCenterInstance("first_instance", "http://ec2-52-91-217-247.compute-1.amazonaws.com");
		instances[1] = new DataCenterInstance("second_instance", "http://ec2-52-91-217-180.compute-1.amazonaws.com");
		instances[2] = new DataCenterInstance("third_instance", "http://ec2-54-86-65-195.compute-1.amazonaws.com");
	}

	public static void main(String[] args) throws Exception {
		// Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(Main.class.getResourceAsStream("/AwsCredentials.properties"));

		credential = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		//Connect to EC2 Client
		AmazonEC2Client ec2 = new AmazonEC2Client(credential);

		initServerSocket();
		for (int i=0;i<3;i++){
			HealthState[i] = true;
		}
		/****************Start new Thread to check health*************/
		
		HealthCheck healthcheck = new HealthCheck();
		Thread healththread = new Thread(healthcheck);
		healththread.start();
		//new thread add instance
		Add_DC add_dc = new Add_DC();
		Thread launchthread = new Thread(add_dc);
		launchthread.start();
		
		/****************Health Check part end**************************/
		LoadBalancer loadBalancer = new LoadBalancer(serverSocket, instances, HealthState);
		loadBalancer.start();
		
		
		
	}

	/**
	 * Initialize the socket on which the Load Balancer will receive requests from the Load Generator
	 */
	private static void initServerSocket() {
		try {
			serverSocket = new ServerSocket(PORT);
		} catch (IOException e) {
			System.err.println("ERROR: Could not listen on port: " + PORT);
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
//for healthcheck
class HealthCheck implements Runnable{
	public void run(){
		System.out.println("start health checking");
		while(true){
			for(int i=0;i<3;i++){
				if (Main.HealthState[i]){
					try{
						
						URL healthURL = new URL(Main.instances[i].getUrl());
                           			HttpURLConnection Conn = (HttpURLConnection) healthURL.openConnection();
                            			Conn.setConnectTimeout(5*1000);
                            			Conn.setRequestMethod("GET");
						if(Conn.getResponseCode() != 200){
							System.out.println(i+" instance failed");
							Main.HealthState[i] = false;
				
						}
						//System.out.println(i+" instance good");
						Thread.sleep(1000);
						
					}
					catch(ConnectException ce){
						System.out.println(i+" instance failed");
						Main.HealthState[i] = false;
					}
					catch(Exception exception){
						System.out.println(i+" instance failed");
						Main.HealthState[i] = false;
					}
						
									
				}			
			
			}			
		}
	}

}

//Thread of adding new data center and keep checking its state for running
class Add_DC implements Runnable{
	
    	public String old_DNS;
	public int num = -1;// record the failed instance#
	AmazonEC2Client ec2 = new AmazonEC2Client(Main.credential);
	public String datacenter;
	private boolean flag = true;
	public void run() {
		
		while(true){
			while(flag){
				for (int i=0;i<3;i++){
					if(!Main.HealthState[i]){
						num = i;
						old_DNS = Main.instances[i].getUrl();
						flag = false;
					}
				}


			}
			
			
			//launch new instance
			System.out.println("launch new instance");
			try{
				datacenter = DC(ec2);
			}catch(Exception e){}
			
			System.out.println("the DNS of new instance is" + datacenter);

			//check new dc's health, if healthy, replace the failed instance
			checkDC(datacenter);
			//repalce old DC
			
			Main.HealthState[num] = true;
			Main.instances[num] = new DataCenterInstance("new_instance", "http://"+datacenter);
			
			
			flag = true;
		}
	}
	//this function check new data center's status
	public static void checkDC(String DC_DNS){
		//when instance running, it->true
		boolean checkflag = false;
		while(!checkflag){
			try{
				checkflag = true;
				Thread.sleep(10000);
				URL checkURL = new URL("http://"+DC_DNS+"/lookup/random");
                    
                    		System.out.println("This URL is http://"+DC_DNS+"/lookup/random");
                    
                    		HttpURLConnection con = (HttpURLConnection) checkURL.openConnection();
                    		con.setConnectTimeout(30000);
				con.setRequestMethod("GET");
                    		if(con.getResponseCode() == 200){
					System.out.println("new dc worked");
					
            
                 		}
                 		else{
                		      	System.out.println("new dc not work yet");
                  		      	checkflag = false;
                		}
				con.disconnect();
			}
			catch(Exception e){checkflag = false;}
		}
	}
	
	//launch a new data center instance
	public static String DC(AmazonEC2Client ec2) throws Exception{
		// Create Instance Request
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		// Configure Data Center Instance Request
		runInstancesRequest.withImageId("ami-ed80c388")
				.withInstanceType("m3.medium")
				.withMinCount(1)
				.withMaxCount(1)
				.withKeyName("15619hongf")
				.withSecurityGroups("P23");

		// Launch Instance
		RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
			
		//get instance
        	Instance instance = runInstancesResult.getReservation().getInstances().get(0);
        	String instanceid = instance.getInstanceId();

		//tag
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
       		createTagsRequest.withResources(instanceid)
        			.withTags(new Tag("Project","2.3"));
        	ec2.createTags(createTagsRequest);
		
		
		//dns
		DescribeInstanceStatusRequest describeInstanceRequest = new DescribeInstanceStatusRequest().withInstanceIds(instanceid);
        	DescribeInstanceStatusResult describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
        	List<InstanceStatus> state = describeInstanceResult.getInstanceStatuses();
        	try{
            		while (state.size() < 1) {
                	Thread.sleep(20000);
                
                	describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
                	state = describeInstanceResult.getInstanceStatuses();
            		}
            		String ins_state = state.get(0).getInstanceState().getName();
			while(!ins_state.equals("running")){}
        	}catch(Exception e){}
            	
		DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
        	List<Reservation> reservations = describeInstancesRequest.getReservations();
        	String DC_DNS = "";
        
       		while(DC_DNS.equals(""))
        	{
            		for(Reservation reservation : reservations){
                		for(Instance ins: reservation.getInstances()){
                    			if(ins.getInstanceId().equals(instanceid)){
                        			DC_DNS = ins.getPublicDnsName();
                    			}
                		}
            		}
        	}
        	return DC_DNS;
	}

}




