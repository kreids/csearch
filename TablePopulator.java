package indexer;

import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.lang.instrument.Instrumentation;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

public class TablePopulator {
	//populate dynamo table
	
	static DynamoDB dynamoDB;
    static String tableName;
    static AmazonDynamoDBClient client;
	
    //hard coded document count
	static int count = 10782;
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		client = new AmazonDynamoDBClient(
    		    new ProfileCredentialsProvider());
    	client.setRegion(Region.getRegion(Regions.US_WEST_2));
    	
    	//hard coded table name
    	tableName="index1";
    	
    	
    	
    	dynamoDB = new DynamoDB(client);
    	
//    	count=Integer.parseInt(args[1]);
    	
    	//calculate things
		calcTfIdf("out/out1/segaa");
	}
	
	public static float calcTfIdf(String fileName) throws FileNotFoundException, IOException{
		
		File file = new File(fileName);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		Table table = dynamoDB.getTable(tableName);
		
		//line number
		int ln = 1;
		
		while ((line = br.readLine()) != null) {
			ln++;
		//get key data
			String[] val = line.split("\t");
			System.out.println("VAL: "+line +":"+val[0]+" : " + val[1]);
			if(!val[1].matches("^[0-9]+$"))
				continue;
			int docsAppeared = Integer.parseInt(val[1]);
			System.out.println(docsAppeared);
			String[] outs = val[2].split(" _ ");
			
			List<HashMap<String,String>> mapList = new ArrayList<HashMap<String, String>>();
			
			int itemSize= 0;
			
			List<Double> idfList = new ArrayList<Double>();
			List<String> urlList = new ArrayList<String>();
			for(int i =0; i<outs.length; i++){
			//push value data to page
				HashMap<String,String> out = new HashMap<String,String>();
				System.out.println(val[0]+outs[i]);
				String[] nums = outs[i].split(" - ");
				//urlList.add(outs[0]);
				System.out.println(nums[0]);
				String[] numNums = nums[1].split(" ");
				double freq=(double)-1;
				double idf= (double)-1;
				
				// initialize word count and specific word count to -1
				double  wc = (double)-1;//Integer.parseInt(numNums[1]);
				double specwc = (double)-1;//Integer.parseInt(numNums[0]);
				
				
				itemSize = itemSize +104 + nums[0].length()*4;
				//System.out.println(idf);
				
				// make sure file doesn't excede dynamo limits
				if(itemSize>409600){
					break;
				}
				
				//calculate idf
				if(!numNums[0].equals("") && !numNums[1].equals("")){
					wc = Integer.parseInt(numNums[1]);
					specwc=Integer.parseInt(numNums[0]);
					freq = (specwc/wc);
					idf = (Math.log((double)count/docsAppeared));
					out.put("tf-idf",""+(freq/idf));
					out.put("url", nums[0]);
					//add attribute map to output list
					mapList.add(out);
				}
			}
		 
		 if(itemSize > 409600)
		 {
			 
		 }
		 //put item in table
		 else{
			System.out.println(""+ln+" : "+ itemSize);
			Item item = new Item().withPrimaryKey("word",val[0])
					.withList("mapList", mapList);
			table.putItem(item);
		 }
		// writeToDynamo(val[0],val[1],freq);
		 }
		
		
		return (float) 0;
	}
	
	public static void writeToDynamo(String word, String url, double tfidf ){
		System.out.println(word+" "+url+" "+tfidf);
		Table table = dynamoDB.getTable("index1");
		Item item = new Item()
	    .withPrimaryKey("word", word)
	    .withString("url", url)
	    .withDouble("tfidf", tfidf);
		
		table.putItem(item);
		
		
		
	}

}