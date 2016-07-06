package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {
	private final static double DISTANCE = 999999999;
  /* Define per task state here. (kv stores etc) */
	//Store driver locations stream data
	private KeyValueStore<String, String> driverLocations;
	private KeyValueStore<String, String> driverList; 
  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
	  //Initialize stuff (maybe the kv stores?)
	  driverLocations = (KeyValueStore<String, String>) context.getStore("driver-loc");
	  driverList = (KeyValueStore<String, String>) context.getStore("driver-list");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
	// The main part of your code. Remember that all the messages for a particular partition
	// come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
	// at one task only, thereby enabling you to do stateful stream processing.
	  String incomingStream = envelope.getSystemStreamPartition().getStream();
	  if(incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())){
		  processDriverLocations((Map<String, Object>) envelope.getMessage());
	  }
	  else if(incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())){
		  processEventStream((Map<String, Object>) envelope.getMessage(), collector);
	  }
	  else{
		  throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
	  }
  }
  public void processDriverLocations(Map<String, Object> message){
	  if(!message.get("type").equals("DRIVER_LOCATION")){
		  throw new IllegalStateException("Unexpected type of driver locations stream ") ;
	  }
	  String driverId = String.valueOf((int) message.get("driverId"));
	  String blockId = String.valueOf((int) message.get("blockId"));
	  String longitude =String.valueOf((int)message.get("longitude"));
	  String latitude = String.valueOf((int)message.get("latitude"));
	  String location = latitude + ":" + longitude;
	  String key = blockId + ":" + driverId;
	  if(driverLocations.get(key) != null){
		  driverLocations.put(key, location);
	  }
  }
  public void processEventStream(Map<String, Object> message, MessageCollector messageCollector){
	  String type = String.valueOf(message.get("type"));
	  if(type.equals("RIDE_REQUEST")){
		  String riderId = String.valueOf((int) message.get("riderId"));
		  sendToClosestDriver(riderId, message, messageCollector);
	  }
	  else if(type.equals("RIDE_COMPLETE")){
		  String driverId = String.valueOf((int)message.get("driverId"));
		  String blockId = String.valueOf((int)message.get("blockId"));
		  String longitude =String.valueOf((int)message.get("longitude"));
		  String latitude = String.valueOf((int)message.get("latitude"));
		  String location = latitude + ":" + longitude;
	
		  String key = blockId + ":" + driverId;
		  driverLocations.put(key, location);
	  }
	  else if(type.equals("ENTERING_BLOCK")){
		  String driverId = String.valueOf((int)message.get("driverId"));
		  String blockId = String.valueOf((int)message.get("blockId"));
		  String key = blockId + ":" + driverId;
		  String status = (String) message.get("status");
		  if(status.equals("AVAILABLE")){
			  String longitude =String.valueOf((int)message.get("longitude"));
			  String latitude = String.valueOf((int)message.get("latitude"));
			  String location = latitude + ":" + longitude;
			  driverLocations.put(key, location);
		  }
		  else{
			  driverLocations.delete(key);
		  }
	  }
	  else {
		  String driverId = String.valueOf((int)message.get("driverId"));
		  String blockId = String.valueOf((int)message.get("blockId"));
		  String key = blockId + ":" + driverId;
		  driverLocations.delete(key);
	  }
	  
  }
  public void sendToClosestDriver(String riderId, Map<String, Object> message, MessageCollector messageCollector){
	  String blockId = String.valueOf(message.get("blockId"));
	  KeyValueIterator<String, String> alternativeDrivers = driverLocations.range(blockId + ":", blockId + ";");
	  int latitude = (int)message.get("latitude");
	  int longitude = (int)message.get("longitude");
	  Double distance = DISTANCE;
	  String chosenDriver = "";
	  double priceFactor = 0.0;
	  int driverRatio = 0;
	  try{
		  while(alternativeDrivers.hasNext()){
			  driverRatio += 1;
			  String currentDriverKey = alternativeDrivers.next().getKey();
			  String currentDriverLocation = driverLocations.get(currentDriverKey);
			  int currentDriverLatitude = Integer.parseInt(currentDriverLocation.split(":")[0]);
			  int currentDriverLongitude = Integer.parseInt(currentDriverLocation.split(":")[1]);
			  double latitudeDistance = (double) (currentDriverLatitude - latitude);
			  double longitudeDistance = (double) (currentDriverLongitude - longitude);
			  double currentDistance = Math.sqrt(latitudeDistance * latitudeDistance + longitudeDistance * longitudeDistance);
			  if(currentDistance < distance){
				  chosenDriver = currentDriverKey;
				  distance = currentDistance;
			  }
		  }
		  //initialize driver-list
		  if(driverList.get(blockId) == null){
			  //first 5, each is large enough to make spf > 1.8, guarantee spf = 1
			  driverList.put(blockId, "50:50:50:50:50");
		  }
		  String pastFiveRatioString = driverList.get(blockId);
		  String[] pastFiveRatio = pastFiveRatioString.split(":");
		  //average ratio of past four and current
		  double averageRatio = ((double)(Integer.parseInt(pastFiveRatio[1]) + Integer.parseInt(pastFiveRatio[2]) + Integer.parseInt(pastFiveRatio[3]) + Integer.parseInt(pastFiveRatio[4]) +driverRatio))/5.0;
		  if(averageRatio >= 1.8){
			  priceFactor = 1.0;
		  }
		  else{
			  priceFactor =  (8.0*(1.8-averageRatio)/0.8)+1.0;
		  }
		  //update the driver-list
		  String newFiveRatioString = pastFiveRatio[1] + ":" + pastFiveRatio[2] + ":" + pastFiveRatio[3] + ":" + pastFiveRatio[4] + ":" + String.valueOf(driverRatio);
		  driverList.put(blockId, newFiveRatioString);
		  driverLocations.delete(chosenDriver);
		  Map<String, String> output = new HashMap<String, String>();
		  output.put("riderId", riderId);
		  output.put("driverId", chosenDriver.split(":")[1]);
		  output.put("priceFactor", String.valueOf(priceFactor));
		  messageCollector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output));
	  }
	  finally{
		  alternativeDrivers.close();
	  }
  }
  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
	//this function is called at regular intervals, not required for this project
  }
}
