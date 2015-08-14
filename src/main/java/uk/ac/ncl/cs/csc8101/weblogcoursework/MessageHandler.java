/*
Copyright 2015 Red Hat, Inc. and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package uk.ac.ncl.cs.csc8101.weblogcoursework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;



import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.datastax.driver.core.*;


/**
 * Integration point for application specific processing logic.
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2015-01
 */
public class MessageHandler {

    private final static Cluster cluster;
    private final static Session session;
    
 
    static {
    	
    	

        cluster = new Cluster.Builder()
                .addContactPoint("127.0.0.1")
                .build();

        final Session bootstrapSession = cluster.connect();
        bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS csc8101 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        bootstrapSession.close();

        session = cluster.connect("csc8101");
        
        
        /**
         * I create the following tables:
         * 
         * test_counter_url which is counter table, to store the accesses for each url
         * test_session_counter to store all the sessions for each client        
         * test_total_counter to store the total number of unique urls accessed by each user
         * 
         */
               
        session.execute("CREATE TABLE IF NOT EXISTS test_counter_url (counter_url counter,url text, hour int, PRIMARY KEY (url, hour) )");
        session.execute("CREATE TABLE IF NOT EXISTS test_session_counter (clientId text, start timestamp, end timestamp, access bigint, number bigint,PRIMARY KEY (clientId, start) )");
        session.execute("CREATE TABLE IF NOT EXISTS test_total_counter (clientId text, number bigint, hyperLog blob, PRIMARY KEY (clientId) )");
    }
    
   
    
    
    //Define a static TheardLocal object dateFormat to ensure the right format of time in each message
    private static final ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>()
    		{
    	@Override protected SimpleDateFormat initialValue()
    	{
    		return new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");
    	}
    	
    		};
    		
    		
    //Define all the relevant database operations for 3 table.
    private final static PreparedStatement insertPC = session.prepare("INSERT INTO test_session_counter (clientId, start, end, access, number) VALUES ( ? , ?, ?, ?, ? )");
    private final static PreparedStatement insertPT = session.prepare("INSERT INTO test_total_counter (clientId, number, hyperLog) VALUES (? ,?, ?)");
    private final static PreparedStatement selectPT = session.prepare("SELECT hyperLog FROM test_total_counter WHERE clientId=?");
    private final static PreparedStatement updatePT = session.prepare("UPDATE test_total_counter SET number=?, hyperLog=? WHERE clientId=?");
    private final static PreparedStatement updatePS = session.prepare("UPDATE test_counter_url SET counter_url= counter_url+1 WHERE url=? AND hour=?");
    
    
    
    
    
    //Define a HashMap to store the status of each client. 
    //If a new record is put inside the map, it will checked whether the eldest entry is expired. If so, test_total_couner table will be updated. 
    private final HashMap<String,SiteSession> record = new LinkedHashMap<String,SiteSession>() {
        protected boolean removeEldestEntry(Map.Entry eldest) {
        	SiteSession site= (SiteSession)eldest.getValue();
        	boolean flag = site.isExpired();
        	if(flag)
        	{
        		try {sessionCounter(site);
				}catch (IOException | CardinalityMergeException e) {		
					e.printStackTrace();
				} 
        	}
            return flag;
        }
    };

    
    
    public static void close() {
        session.close();
        cluster.close();
    }

    
    //When the data stream ends or a ConsumerTimeoutException is thrown, 
    //flush method will be used to update test_total_counter table according to the records.
    public void flush() 
    {
    
    for(String client: record.keySet())
    {
    try {
		sessionCounter(record.get(client));
	} catch (IOException | CardinalityMergeException e) {
		
		e.printStackTrace();
	}
   
    }
   
   
   
    
   
    }

    public void handle(String message) 
    {
    //part1
    String[] str=message.split(" ");
    String dateString=str[1]+" "+str[2];
    int hour = Integer.parseInt(str[1].substring(13, 15));
    
    //Update the counter table test_counter_url according the time 
    //and url received from the message
    session.execute(new BoundStatement(updatePS).bind(str[4],hour));
       	
    	
    	
    //part2
   
    try{
    	
    //transfer the date string to a Date object and then to a long value.	
    Date date = dateFormat.get().parse(dateString);
    long time=date.getTime();
   
    //Check whether this client is included in the HashMap.
    if(record.containsKey(str[0]))
    {
    	
    //If client already exists, read the firsthit and lasthit message from the SiteSession object stored.    	
    SiteSession site=record.get(str[0]);
    long edge = site.getFirstHitMillis()+SiteSession.MAX_IDLE_MS;
    long lastHit = site.getLastHitMillis();
    
    //If the new message is between the lasthit and the edge of expiration, update the existing site session
    if(time<=edge && time>=lastHit)
    {
    site.update(time, str[4]);
    }    
   
    //If the time in the message come out of range, the SiteSession in the map will be used to update
    //test_session_counter and test_total_couner. Then the new message will be written into the map as
    //a SiteSession object which covers the old one.
    else if(time>edge)
    {
    sessionCounter(site);
    
      
    record.put(str[0],new SiteSession(str[0],time,str[4]));
    }
   
    else
    {
    //If the given is not in time ordered, it will be ignored and an exception be thrown.
    throw new IllegalArgumentException("given time out of order");
    }
    }
   
    else
    {
    record.put(str[0], new SiteSession(str[0],time,str[4]));
    }
   
      
  
    
    }catch (ParseException | IOException | IllegalArgumentException | CardinalityMergeException e) {		
		e.printStackTrace();
	}  
        
   
    }
    
    //HyperLogLog function is used to transfer a HyperLogLog object to a ByteBuffer object,
    //when we need to serialize a HyperLogLog object which contains the collection of all the accessed urls
    private ByteBuffer hyperLog(HyperLogLog log) throws IOException
    {
    byte[] inputBytes = log.getBytes();
         ByteBuffer byteBuffer = ByteBuffer.allocate(4+inputBytes.length);
         byteBuffer.putInt(inputBytes.length);
         byteBuffer.put(inputBytes);
         byteBuffer.flip();
         return byteBuffer;
    }
    
   
    
    //This method is used to process a Sitesession object stored in the HashMap.
        
    private void sessionCounter(SiteSession site) throws IOException, CardinalityMergeException
    {
    	    //Insert a new session to test_session_counter table.
    	    final Statement statement = new BoundStatement(insertPC).bind(site.getId(),new Date(site.getFirstHitMillis()), new Date(site.getLastHitMillis()),site.getHitCount(),site.getHyperLogLog().cardinality());
    	    session.execute(statement);

    	    final Statement query= new BoundStatement(selectPT).bind(site.getId());
    	   
    	    ResultSet resultSet = session.execute(query);
    	   
    	    //Query test_total_counter table to check whether this client already exists in the table.
    	    if(resultSet.isExhausted())
    	    {
    	    	
    	    //If the client does not exist, insert the his/her session into the table	
    	    final Statement insert = new BoundStatement(insertPT).bind(site.getId(),site.getHyperLogLog().cardinality(),hyperLog(site.getHyperLogLog()));
    	    session.execute(insert);
    	    
    	    }
    	   
    	    else
    	    {
    	     //If the client already exists in the table, deserialize the HyperLogLog data stored as blob in the table.
    	     //Merge the obtained HyperLogLog object with the other one in the expired session.
    	     //Rewrite the results back to the table.
    	     ByteBuffer flow=resultSet.one().getBytes(0);
    	     byte[] log = new byte[flow.getInt()];
    	     flow.get(log);
    	     HyperLogLog urlLog= HyperLogLog.Builder.build(log);
    	     urlLog.addAll(site.getHyperLogLog());
    	     final Statement update = new BoundStatement(updatePT).bind(urlLog.cardinality(),hyperLog(urlLog),site.getId());
    	     session.execute(update);
    	    }
    }
    
    
    
   
}


