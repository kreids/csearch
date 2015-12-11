package indexer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Downloader {
	
	public static void main(String[] args) throws IOException{
		downloadFile("ei.html");
	}
	
	int numDocs;
	
	
	//downloads a file from s3
	public static String[] downloadFile(String fileName){
		AWSCredentials credentials = new ProfileCredentialsProvider("default").getCredentials();
		AmazonS3 s3;
		try {
			credentials = new ProfileCredentialsProvider("default")
			.getCredentials();
			s3 = new AmazonS3Client(credentials);

			//s3 data
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
			.withBucketName("cis455crawlerreal")
			.withPrefix("m");
			ObjectListing objectListing;

			do {
				objectListing = s3.listObjects(listObjectsRequest);
				for (S3ObjectSummary objectSummary : 
					objectListing.getObjectSummaries()) {
					System.out.println( " - " + objectSummary.getKey() + "  " +
			                "(size = " + objectSummary.getSize() + 
							")");
				}
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
			
			} catch (Exception e) {
				//System.exit(-1);
			throw new AmazonClientException(
			"Cannot load the credentials from the credential profiles file. "
			+ "Please make sure that your credentials file is at the correct "
			+ "location (/home/cis455/.aws/credentials), and is in valid format.",
			e);
		//	System.exit(-1);
			}
		//System.out.println(fileName);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usEast1);
		AmazonS3Client s3Client = new AmazonS3Client(credentials);  
		System.out.println(fileName);
		System.out.println(fileName);
		S3Object object = s3Client.getObject(new GetObjectRequest("cis455crawlerreal/doc_db", fileName));
		InputStream reader = new BufferedInputStream(
				   object.getObjectContent());
		 //displayTextInputStream(reader);
		String[] words = null;
		try{
			//parse html doc
		Document doc;
		doc = Jsoup.parse(reader, "UTF-8", "test.html");
		// deal with empty docs
		if(!doc.equals(null)){
			if(!(doc.body()==null))
			if(doc.body().hasText()){
				//handle text fixxing
				String text = doc.body().text();
				text = text.toLowerCase();
				text = text.replaceAll("[^a-z0-9\\s]", "");
				text=text.replaceAll("[0-9]", "");
				System.out.println(text);		
				words = text.split(" ");
			}
		}}
		
		catch(IOException e){
			
		}
		return words;
	}
	

}
