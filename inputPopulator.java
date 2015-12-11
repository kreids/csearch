package indexer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class inputPopulator {

	//gets text file with hashes & urls from
	
    static DynamoDB dynamoDB;
    static String tableName;
    static AmazonDynamoDBClient client;
    
    public static void main(String[] args) throws Exception {
    	tableName = "urlhashreal";
    	
    	client = new AmazonDynamoDBClient(
    		    new ProfileCredentialsProvider());
    	client.setRegion(Region.getRegion(Regions.US_WEST_2));
    	
    	
    	dynamoDB = new DynamoDB(client);
    	
    	getTableInformation();
    	findTable();
    }
    
    
    	
   


    private static void findTable() throws FileNotFoundException, UnsupportedEncodingException {
    	
    	ScanRequest scanRequest = new ScanRequest()
        .withTableName(tableName);
    	
    	
        //Table table = dynamoDB.getTable(tableName);
    	PrintWriter writer = new PrintWriter("in/in1", "UTF-8");
    	
    	long startTime = System.currentTimeMillis();
    	
        long count=0;
        
        //scan table
        ScanResult result = client.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.getItems()){
        	if(count%10==0){
        		long time = System.currentTimeMillis()-startTime;
        		System.out.println(""+count + " : "+ time);
        	}
        	
        	//parse out hash
        	count++;
        	AttributeValue v = item.get("hash");
        	String hash =item.get("hash").toString();
        	hash =hash.toString().substring(4,hash.length()-2);

        	// parse out url
        	String url = item.get("url").toString();
        	url =url.toString().substring(4, url.length()-2);
        	writer.println(hash +" "+  url);
        	
        	
         //   System.out.println(":"+hash + ":" + url+":");
        }
        writer.close();
        System.out.println(count);
           
    }
    
    public static void getTableInformation() {

    	//Get table info
        System.out.println("Table " + tableName);
        
        TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
        System.out.format("Name: %s:\n" + "Status: %s \n"
                + "Provisioned Throughput (read capacity units/sec): %d \n"
                + "Provisioned Throughput (write capacity units/sec): %d \n",
        tableDescription.getTableName(), 
        tableDescription.getTableStatus(), 
        tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
        tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
    }
}