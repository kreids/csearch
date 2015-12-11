package awsWrapper;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class awsWrapper {
	
	private AmazonS3 s3client;
	private AmazonDynamoDB dbClient;
	private DynamoDB dynamoDB;
	//List of all S3 folders
	private List<String> folders = new ArrayList<String>();
	//The current working database
	private String oper;
	//Bucket name
	private String bucketName;
	
	/**
	 * Constructor for database wrapper object
	 * @param BDBstore - path to where database store is (or will be)
	 */
	public awsWrapper()
	{
		ClientConfiguration cc = new ClientConfiguration()
	    .withMaxErrorRetry (10)
	    .withConnectionTimeout (10_000)
	    .withSocketTimeout (10_000)
	    .withTcpKeepAlive (true);
		s3client = new AmazonS3Client(new ProfileCredentialsProvider(), cc);
		dbClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dbClient.setRegion(Region.getRegion(Regions.US_WEST_2));
		dynamoDB = new DynamoDB(dbClient);

		folders.add("date_db");
		folders.add("doc_db");
		bucketName = "cis455crawlerreal";
	}
	
	/**
	 * Sets operating database.
	 * Operating database is one in which all inserts, deletes, and gets apply to
	 * @param db_name - Name of database to set to operating one
	 */
	public void setOper(String db_name)
	{
		oper = db_name;
//		if (folders.contains(db_name))
//		{
//			oper = db_name;
//		}
//		else
//		{
//			System.err.println("Invalid S3 folder name specified");
//		}
	}
	
	/**
	 * Checks if filename exits in operating database
	 * @param key - The file to check 
	 * @return True if it exists in operating db, false otherwise
	 */
	public synchronized boolean keyExists(String key)
	{
		key = Integer.toString(key.hashCode());
		try 
		{
		    S3Object object = s3client.getObject(bucketName, oper + "/" + key);
		    object.close();
		} 
		catch (AmazonServiceException e) 
		{
		    String errorCode = e.getErrorCode();
		    if (!errorCode.equals("NoSuchKey")) 
		    {
		        throw e;
		    }
		    return false;
		}
		catch (IOException e) 
		{
			e.printStackTrace();
			return false;
		}
		return true;
	    
	}
	
	/**
	 * Inserts key, value pair into database.
	 * Does nothing if it fails
	 * @param key - Key to insert
	 * @param value - Value to insert
	 */
	public synchronized void insert(String key, String value)
	{
		try 
		{
			key = Integer.toString(key.hashCode());
			File file = new File(key + ".html");
			file.createNewFile();
			PrintWriter writer = new PrintWriter(key + ".html", "UTF-8");
			writer.println(value);
			writer.close();
			String key2 = oper + "/" + key;
			s3client.putObject(new PutObjectRequest(bucketName, key2, file));
			file.delete();

		} 
		catch (AmazonServiceException ase) 
		{
			System.out.println("Caught an AmazonServiceException, which "
					+ "means your request made it "
					+ "to Amazon S3, but was rejected with an error response"
					+ " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} 
		catch (AmazonClientException ace) 
		{
			System.out.println("Caught an AmazonClientException, which "
					+ "means the client encountered "
					+ "an internal error while trying to "
					+ "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} 
		catch (FileNotFoundException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (UnsupportedEncodingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Retrieves value based on key from database.
	 * @param key - The key to get the value
	 * @return Relevant value associated with key, null if key doesn't exist or failure
	 */
	public synchronized String get(String key)
	{
		key = Integer.toString(key.hashCode());
		try
		{
			S3Object object = s3client.getObject(new GetObjectRequest(bucketName, oper + "/" + key));
			InputStream objectData = object.getObjectContent();
			//Process the objectData stream.
			try 
			{
				Scanner s = new Scanner(objectData).useDelimiter("\\A");
				String val = "";
				while (s.hasNext())
				{
					val = val + s.next();
				}
				objectData.close();
				s.close();
				return val;
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			catch (Exception e)
			{
				return null;
			}
		}
		catch (Exception e)
		{
			return null;
		}
		
	}
	
//	public void batchWrite()
//	{
//		File date_folder = new File("/dates");
//		File[] date_files = date_folder.listFiles();
//		File docs_folder = new File("/docs");
//		File[] docs_files = docs_folder.listFiles();
//		for (File f: date_files)
//		{
//			String key2 = "date_db/" + f.getName();
//			s3client.putObject(new PutObjectRequest(bucketName, key2, f));
//			f.delete();
//		}
//		
//		for (File f: docs_files)
//		{
//			String key2 = "doc_db/" + f.getName();
//			s3client.putObject(new PutObjectRequest(bucketName, key2, f));
//			f.delete();
//		}
//		
//	}
	
	public void batchWrite()
	{
		// Create a list of UploadPartResponse objects. You get one of these for
		// each part upload.
		List<PartETag> partETags = new ArrayList<PartETag>();



		
		
		File date_folder = new File("/dates");
		File[] date_files = date_folder.listFiles();
		File docs_folder = new File("/docs");
		File[] docs_files = docs_folder.listFiles();
		

		
		for (File f: date_files)
		{
			// Step 1: Initialize.
			String key2 = "date_db/" + f.getName();
			InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
					bucketName, key2);
			InitiateMultipartUploadResult initResponse = s3client
					.initiateMultipartUpload(initRequest);
			long contentLength = f.length();
			long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

			try 
			{
				// Step 2: Upload parts.
				long filePosition = 0;
				for (int i = 1; filePosition < contentLength; i++) {
					// Last part can be less than 5 MB. Adjust part size.
					partSize = Math.min(partSize,
							(contentLength - filePosition));

					// Create request to upload a part.
					UploadPartRequest uploadRequest = new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key2)
							.withUploadId(initResponse.getUploadId())
							.withPartNumber(i).withFileOffset(filePosition)
							.withFile(f).withPartSize(partSize);

					// Upload part and add response to our list.
					partETags.add(s3client.uploadPart(uploadRequest)
							.getPartETag());

					filePosition += partSize;
				}

				// Step 3: Complete.
				CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
						bucketName, key2,
						initResponse.getUploadId(), partETags);

				s3client.completeMultipartUpload(compRequest);
			} 
			catch (Exception e) 
			{
				s3client.abortMultipartUpload(new AbortMultipartUploadRequest(
						bucketName, key2, initResponse.getUploadId()));
			}

			f.delete();
		}

		for (File f : docs_files) {
			String key2 = "doc_db/" + f.getName();
			InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
					bucketName, key2);
			InitiateMultipartUploadResult initResponse = s3client
					.initiateMultipartUpload(initRequest);
			long contentLength = f.length();
			long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

			try 
			{
				// Step 2: Upload parts.
				long filePosition = 0;
				for (int i = 1; filePosition < contentLength; i++) {
					// Last part can be less than 5 MB. Adjust part size.
					partSize = Math.min(partSize,
							(contentLength - filePosition));

					// Create request to upload a part.
					UploadPartRequest uploadRequest = new UploadPartRequest()
							.withBucketName(bucketName)
							.withKey(key2)
							.withUploadId(initResponse.getUploadId())
							.withPartNumber(i).withFileOffset(filePosition)
							.withFile(f).withPartSize(partSize);

					// Upload part and add response to our list.
					partETags.add(s3client.uploadPart(uploadRequest)
							.getPartETag());

					filePosition += partSize;
				}

				// Step 3: Complete.
				CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
						bucketName, key2,
						initResponse.getUploadId(), partETags);

				s3client.completeMultipartUpload(compRequest);
			} 
			catch (Exception e) 
			{
				s3client.abortMultipartUpload(new AbortMultipartUploadRequest(
						bucketName, key2, initResponse.getUploadId()));
			}
			f.delete();
		}
		
	}
	
	/**
	 * Deletes entry from database based on key
	 * Does nothing if failure (e.g. key doesn't exit
	 * @param key - the key to delete
	 */
	public void delete(String key)
	{
		key = Integer.toString(key.hashCode());
		try 
		{
            s3client.deleteObject(new DeleteObjectRequest(bucketName, oper + "/" + key));
        } 
		catch (AmazonServiceException ase) 
		{
            System.out.println("Caught an AmazonServiceException.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
		catch (AmazonClientException ace) 
		{
            System.out.println("Caught an AmazonClientException.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	}
	
	public void putURLKey(String url)
	{
		Table table = dynamoDB.getTable("urlhashreal");
		String hash = Integer.toString(url.hashCode());
		Item item = new Item()
	    	.withPrimaryKey("hash", hash)
	    	.withString("url", url);
		Map<String, String> expressionAttributeNames = new HashMap<String, String>();
		expressionAttributeNames.put("#h", hash);
		PutItemOutcome outcome = table.putItem(
			    item, 
			    "attribute_not_exists(#h)", // ConditionExpression parameter
			    expressionAttributeNames,   // ExpressionAttributeNames parameter
			    null);
	}
	
	public void putTitle(String url, String title)
	{
		if (url=="" || title =="")
		{
			return;
		}
		Table table = dynamoDB.getTable("titles");
		Item item = new Item()
    		.withPrimaryKey("url", url)
    		.withString("title", title);
		PutItemOutcome outcome = table.putItem(item);
	}
	
	public void writeLinks(String url, Set<String> links)
	{
		Table table = dynamoDB.getTable("doc_links");
		Item item = new Item()
    		.withPrimaryKey("url", url)
    		.withStringSet("links", links);
		PutItemOutcome outcome = table.putItem(item); 
		
	}
	
	public void transferHash() throws FileNotFoundException, UnsupportedEncodingException
	{
		ScanRequest scanRequest = new ScanRequest().withTableName("urlhashreal");
		ScanResult result = dbClient.scan(scanRequest);
		PrintWriter writer = new PrintWriter("0.csv", "UTF-8");
		for (Map<String, AttributeValue> item : result.getItems())
		{
		   String hash = item.get("hash").getS();
		   String url = item.get("url").getS();
		   writer.println(hash + "," + url);
		}
		writer.close();
	}
	
	public void transferLinks() throws FileNotFoundException, UnsupportedEncodingException
	{
		ScanRequest scanRequest = new ScanRequest().withTableName("doc_links");
		ScanResult result = dbClient.scan(scanRequest);
		PrintWriter writer = new PrintWriter("link0.csv", "UTF-8");
		for (Map<String, AttributeValue> item : result.getItems())
		{
		   String url = item.get("url").getS();
		   List<String> links = item.get("links").getSS();
		   for (String link: links)
		   {
			   writer.println(url+ "," + link);
		   }
		}
		writer.close();
	}
	
	
//	public static void main(String[] args)
//	{
//		awsWrapper wrapper = new awsWrapper();
//		try {
//			wrapper.transferLinks();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	
	/**
	 * Gets a Map<String,String> object representing operating database
	 * @return a Map<String,String> object representing operating database, null if failure
	 * @throws UnsupportedEncodingException
	 */
//	public synchronized Map<String,String> getMap() throws UnsupportedEncodingException
//	{
//		Cursor cursor = null;
//		Map<String,String> map = new HashMap<String,String>();
//		 
//		try 
//		{
//		    if (oper!=null)
//		    {
//		    	cursor = oper.openCursor(null, null);
//		    }
//		 
//		    DatabaseEntry test_key = new DatabaseEntry();
//		    DatabaseEntry test_data = new DatabaseEntry();
//		 
//		    if (cursor!=null)
//		    {
//			    while (cursor.getNext(test_key, test_data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
//			    {
//			        String key = new String(test_key.getData(), "UTF-8");
//			        String value = new String(test_data.getData(), "UTF-8");
//			        map.put(key,value);
//			    }
//		    }
//		} 
//		catch (DatabaseException de) 
//		{
//			//Do nothing
//		} 
//		finally 
//		{
//		    if (cursor!=null)
//		    {
//		    	cursor.close();
//		    }
//		}
//		return map;
//	}
	
	/**
	 * Closes all databases 
	 */
//	public void close()
//	{
//		for (String d: db_map.keySet())
//		{
//			db_map.get(d).close();
//		}
//	}

}
