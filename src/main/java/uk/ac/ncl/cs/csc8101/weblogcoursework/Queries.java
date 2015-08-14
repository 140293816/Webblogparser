/*
Copyright 2014 Red Hat, Inc. and/or its affiliates.

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




import com.datastax.driver.core.*;

/**
 * Simple integration tests for cassandra server v2 / CQL3 via datastax java-driver
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2014-01
 */
public class Queries {

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
	        
	      
	    }	    
	    
	    //Define SELECT operations for these 3 tables.
	    private final static PreparedStatement selectPS = session.prepare("SELECT counter_url FROM test_counter_url WHERE url=? AND hour>=? AND hour<?");
	    private final static PreparedStatement selectCA = session.prepare("SELECT * FROM test_session_counter WHERE clientId=?");
	    private final static PreparedStatement selectCT = session.prepare("SELECT number FROM test_total_counter WHERE clientId=?");
	    
	    public static void close() {
	        session.close();
	        cluster.close();
	    }
	    
	    public static void main(String[] args)
	    {
	    	
	    	String[] urls={"/images/32p49823.jpg","/english/playing/download/images/big.bird.gif","/cgi-bin/trivia/Trivia.pl"};
	    	int start = 0;
	    	int end =3;
	    	String id="285";
	    	
	    	
	    	//For a given a set of urls, start hour, end hour, query test_counter_url table and calculate the total accesses.
	    	System.out.println("Part1:\n");
	    	for(String url:urls)
	    	{
	    		Long sum=0L;
	    		ResultSet resultSet=session.execute( new BoundStatement(selectPS).bind(url,start,end) );
	    		for(Row number:resultSet.all())
	    		{
	    			sum+=number.getLong(0);
	    		}
	    		System.out.println("Total number of accesses for "+url+" between "+start+" and "+end+" o'clock is "+sum+"\n");
	    	}
	    	
	    	System.out.println("Part2:\n");
	    	System.out.println("All sessions of client "+id+" in the format of [client id, start time, number of accesses, end time, number of distinct urls]:\n");
	    	
	    	
	    	//For a given client, retrieve all his/her sessions in the test_session_counter table and the 
	    	//total number of distinct urls accessed in the test_total_counter table 
	    	ResultSet result0=session.execute(new BoundStatement(selectCA).bind(id));
	    	for(Row result: result0.all())
	    	{
	    		
	    		System.out.println(result.toString()+"\n");
	    	}
	    	ResultSet result1=session.execute(new BoundStatement(selectCT).bind(id));
	    	System.out.println("Number of distinct URLs over all the sessions for client "+id+": "+result1.one().getLong(0)+"\n\n\n");
	    	 	
	    	          
	    	close();
	    }
}
