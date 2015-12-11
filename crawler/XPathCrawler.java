package edu.upenn.cis455.crawler;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import awsWrapper.awsWrapper;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.servlet.HttpClient;




public class XPathCrawler {

	//Stores the max file size to retrieve (in mb)
	double max_size = Double.MAX_VALUE;
	//Max number of files to retrieve (negative value means no limit)
	int max_files = -1;
	//Counter for how many files we've retrieved 
	int count = 0;
	//Map for crawler threads
	Map<Integer, CrawlerThread> crawl_map = new HashMap<Integer,CrawlerThread>();
	//Constant for number of threads for crawler
	final static int numThreads = 20;
	//Wrapper for database
	awsWrapper dbwrapper;
	//File for links
	File link_file;
	//File for url hashes 
	File hash_file;
	//PrintWriters for files
	PrintWriter link_writer;
	PrintWriter hash_writer;
	boolean writing = false;
	
	public XPathCrawler()
	{
		link_file = new File("link6.csv");
		hash_file = new File("6.csv");
		try 
		{
			link_file.createNewFile();
			hash_file.createNewFile();
			link_writer = new PrintWriter(new FileWriter(link_file, true)); 
			hash_writer = new PrintWriter(new FileWriter(hash_file, true)); 
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < numThreads; i++) {
			CrawlerThread ct = new CrawlerThread(i);
			crawl_map.put(i, ct);
		}
	}    
	
	/**
	 * Crawls the web for html/xml files starting with the first url in the Queue.
	 * Keeps going until limit is reached (or until no more pages are left in case there's no limit)
	 * Doesn't retrieve files above max file size
	 */
	public void crawl()
	{
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		for (int i: crawl_map.keySet())
		{
			executor.execute(crawl_map.get(i));
        } 
//		link_writer.close();
//		hash_writer.close();
	}

	public boolean allEmpty()
	{
		boolean return_bool = true;
		for (int i: crawl_map.keySet())
		{
			return_bool = return_bool && crawl_map.get(i).getQueue().isEmpty();
		}
		return return_bool;
	}
	
	public void place(String url)
	{
		try 
		{
			URI uri = new URI(url);
		    String whole_host = uri.getHost();
		    String url_host;
		    if (whole_host == null)
		    {
		    	return;
		    }
		    if (whole_host.startsWith("www."))
		    {
		    	url_host = whole_host.substring(4);
		    }
		    else
		    {
		    	url_host = whole_host;
		    }
		    crawl_map.get(Math.abs(url_host.hashCode()) % numThreads).getQueue().add(url);
		}
		catch (URISyntaxException e) 
		{
			return;
		}
	    catch (NullPointerException e)
		{
	    	return;
		}
	}
	
	public boolean parseContent(String content, String url, boolean cache)
	{
	    if (content==null)
	    {
	    	return false;
	    }
		//List of all links from this page
		Set<String> the_links = new HashSet<String>();
		
		org.jsoup.nodes.Document doc = Jsoup.parse(content);
	    
		//Searching for <a href=..> elements
	    Elements links = doc.select("a[href]");
	    for (Element link: links)
	    {
	    	String val = link.attr("href");
			if (val.toLowerCase().startsWith("http")) 
			{
				if (!cache)
				{
					the_links.add(val);
				}
				place(val);
			}
			else if (val.toLowerCase().startsWith("www"))
			{
				if (!cache)
				{
					the_links.add("http://" + val);
				}
				place ("http://" + val);
			}
			else if (!val.contains("#")) 
			{
				if (val.startsWith("//"))
				{
					String new_url;
					if (url.startsWith("https://"))
					{
						new_url = "https:" + val;
					}
					else
					{
						new_url = "http:" + val;
					}
					if (!cache)
					{
						the_links.add(new_url);
					}
					place(new_url);
				}
				else
				{
					int first = url.indexOf("/");
					if (first!=-1)
					{
						int second = url.indexOf("/", first + 1);
						if (second!=-1)
						{
							int third = url.indexOf("/", second + 1);
							if (third!=-1)
							{
								String new_url = url.substring(0, third)
									+ val;
								if (!cache)
								{
									the_links.add(new_url);
								}
								place(new_url);
							}
						}
					}
				}
			}
	    }
	    if (!cache)
	    {
	    	String title = doc.title();
	    	if (title!="")
	    	{
	    		dbwrapper.putTitle(url,title);
	    	}
	    }
	    if (!cache && !the_links.isEmpty())
	    {
//	    	dbwrapper.writeLinks(url, the_links);
	    	for (String link: the_links)
	    	{
	    		link_writer.println(url + "," + link);
	    	}
	    }
		return true;
	}
	
	
	/**
	 * Parses XML content and matches XPaths
	 * @param content - String containing XML content
	 * @param url - URL of file
	 * @return true on successful parse, false otherwise
	 */
//	public boolean parseXMLContent(String content, String url)
//	{
//		try 
//		{
//			//Building XML document
//			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//			DocumentBuilder builder = factory.newDocumentBuilder();
//			Document doc = builder.parse(new ByteArrayInputStream(content.getBytes("utf-8")));
//			doc.getDocumentElement().normalize();
//			
//			//Getting XPath parser
//			XPathEngineFactory xf = new XPathEngineFactory(); 
//			XPathEngine parser = xf.getXPathEngine();
//			
//			//Iterating over all channels in database to match XPath expressions
//			dbwrapper.setOper("channel_db");
//			Map<String,String> channels = dbwrapper.getMap();
//			for (String channel: channels.keySet())
//			{
//				String xlist = dbwrapper.get(channel);
//				String[] xpaths = xlist.split(";");
//				parser.setXPaths(xpaths);
//				//Checking if at least one XPaths matched
//				boolean[] results = parser.evaluate(doc);
//				boolean one_match = false;
//				for (boolean b: results)
//				{
//					one_match = one_match || b;
//				}
//				//If even one matches
//				if (one_match)
//				{
//					//Get array of URL matches and add to it and put it back into database
//					dbwrapper.setOper("match_db");
//					if (dbwrapper.keyExists(channel))
//					{
//						String[] old = dbwrapper.getArray(channel);
//						//Creating a copy of the old array with an additional entry at the end
//					    String[] n = Arrays.copyOf(old, old.length + 1);
//					    //Adding new entry to array;
//					    n[old.length - 1] = url;
//					    //Deleting old db entry
//					    dbwrapper.delete(channel);
//					    //Inserting new db entry
//					    dbwrapper.insertArray(channel, n);
//					}
//					else
//					{
//						//Inserting a new array if there was nothing in the db already
//						dbwrapper.insertArray(channel, new String[]{url});
//					}
//				}
//			}
//			return true;
//		} 
//		catch (ParserConfigurationException | SAXException | IOException e) 
//		{
//			System.err.println("Error parsing document, here's the problem url: " + url);
//			return false;
//		}
//	}
	
	//Basic getters and setters
		
	public void setMaxSize(double max_size2)
	{
		max_size = max_size2;
	}
	
	public void setMaxFiles(int mf)
	{
		max_files = mf;
	}
	
	public void setDB(awsWrapper dbwrapper2)
	{
		dbwrapper = dbwrapper2;
	}
	
	
	class CrawlerThread extends Thread
	{
		//Integer so the thread knows what # thread it is
		int num;
		BlockingQueue<String> q = new LinkedBlockingQueue<String>();
		//List of URLS visited during this crawl
		Set<String> visited = new HashSet<String>();
		//Maps host names to the RobotsTxtInfo files
		Map<String,RobotsTxtInfo> robot_store = new HashMap<String,RobotsTxtInfo>();
		
		public CrawlerThread(int n)
		{
			num = n;
		}
		
		public void run()
		{
			//While max file limit hasn't been reached 
			while (count<=max_files)
			{
//				System.out.println(num + " is running");
				//If there are more URLs to process
				if (!allEmpty())
				{
					//Getting next url 
					String url;
					try 
					{
						url = q.take();
					} 
					catch (InterruptedException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
						continue;
					}
					HttpClient h = new HttpClient();	
					
					//Skip this url if we've visited it during this crawl
					if (visited.contains(url))
					{
						continue;
					}
					visited.add(url);
					
//					dbwrapper.putURLKey(url);
					hash_writer.println(url.hashCode() + "," + url);
					
					//Dealing with robots.txt
					h.robots(url, robot_store);
					
					
					//Setting if-modified-since header if needed
					dbwrapper.setOper("date_db");
					if (dbwrapper.keyExists(url))
					{
						String date = dbwrapper.get(url);
						if (date != null)
						{
							date = date.replaceAll("\n", "");
							h.getHeaders().put("If-Modified-Since", date);
						}
					}
					
					//Sending the first head request 
					String status = h.sendHead(url);
					if (status == null)
					{
						continue;
					}
					//If the url is valid
					if (!status.equals("invalid url"))
					{
						//Skiping the URL if directed to do so by robots.txt
						if (status.equals("skipped"))
						{
//							System.out.println("Skipping " + url + " due to robots.txt");
							continue;
						}
						//Getting response number
						String response_num = h.getResponse();
						//Redirecting if needed
						if (response_num.contains("301") || response_num.contains("302") || response_num.contains("303"))
						{
							place(h.getResponseHeaders().get("Location"));
						}
						//If successful request
						else if (response_num.contains("200"))
						{
							
							//Making sure file size of document is less than limit
							if (h.getResponseHeaders().containsKey("Content-Length"))
							{
								long byte_size = Long.parseLong(h.getResponseHeaders().get("Content-Length"));
								if (max_size < byte_size/1000000.0)
								{
									continue;
								}
							}
							
							//Getting content type
							String content_type = h.getResponseHeaders().get("Content-Type");
							if (content_type == null)
							{
								continue;
							}
							//If html
							if (content_type.contains("text/html"))
							{
								//Sending get request
								String content = h.sendGet(url);

								if (!content.equals("invalid url")) 
								{
									System.out.println("Thread" + num + ": " + url + ": Downloading...");
									if (count%100 == 0)
									{
										System.out.println(count);
//										if (!writing && count!=0)
//										{
//											writing = true;
//											dbwrapper.batchWrite();
//											writing = false;
//										}
									}
									count++;
									//If we could parse the page successfully
									if (parseContent(content, url, false)) 
									{
										//Saving date of visit into database
										dbwrapper.setOper("date_db");
										if (h.getResponseHeaders().get("Date") != null)
										{
//											dbwrapper.insert(url, h
//													.getResponseHeaders().get("Date"));
											//batch uploads
											String name = Integer.toString(url.hashCode());
											File date_file = new File("dates/" + name + ".html");
											try 
											{
												date_file.createNewFile();
												PrintWriter date_writer = new PrintWriter(new FileWriter(date_file, true));
//												PrintWriter date_writer = new PrintWriter(new FileWriter(new File(".").getAbsolutePath()+"/dates/"+ url.hashCode() + ".html",true));
												date_writer.println(h.getResponseHeaders().get("Date"));
												date_writer.close();
											} 
											catch (IOException e) 
											{
												// TODO Auto-generated catch block
												e.printStackTrace();
											}
											
											
										}
										//Saving content of page into database
										dbwrapper.setOper("doc_db");
//										dbwrapper.insert(url, content);
										//batch uploads
										File doc_file = new File("docs/" + url.hashCode() + ".html");
										try 
										{
											doc_file.createNewFile();
											PrintWriter doc_writer = new PrintWriter(new FileWriter(doc_file, true));
//											PrintWriter doc_writer = new PrintWriter(new FileWriter(new File(".").getAbsolutePath()+"/docs/"+ url.hashCode() + ".html",true));
											doc_writer.println(content);
											doc_writer.close();
										} 
										catch (Exception e)
										{
											e.printStackTrace();
										}
									}
								}
							}
//							//If XML
//							else if (content_type.contains("text/xml") || content_type.contains("application/xml") || content_type.contains("xml+"))
//							{
//								//Sending get request
//								String content = h.sendGet(url);
//								if (!content.equals("invalid url")) 
//								{
//									System.out.println(url + ": Downloading...");
//									count++;
//									//If the parse was successful
//									if (parseXMLContent(content, url)) 
//									{
//										//Saving date of visit into database
//										dbwrapper.setOper("date_db");
//										dbwrapper.insert(url, h
//												.getResponseHeaders().get("Date"));
//										//Saving content of page into database
//										dbwrapper.setOper("doc_db");
//										dbwrapper.insert(url, content);
//										//Saving type of page (xml) into database
//										dbwrapper.setOper("type_db");
//										dbwrapper.insert(url,"xml");
//									}
//								}
//							}
							
						}
						//If it wasn't modified, use the cached version of the page to parse
						else if (response_num.contains("304"))
						{
//							System.out.println(url + ": Not modified");
							dbwrapper.setOper("doc_db");
							String content = dbwrapper.get(url);
							parseContent(content,url, true);
							if (count%100 == 0)
							{
								System.out.println(count);
//								if (!writing && count!=0)
//								{
//									writing = true;
//									dbwrapper.batchWrite();
//									writing = false;
//								}
							}
							count++;
						}
					}
					else
					{
						System.out.println("invalid url " + url);
					}
				}
				else
				{
					break;
				}
			}
		}
		
		public BlockingQueue<String> getQueue()
		{
			return q;
		}
	}
	
	/**
	 * Main method to run from command line
	 * @param args - starting URL, path to database, max file size,
	 * 				 (optional) max number of files to retrieve
	 */
	public static void main(String[] args)
	{
		String URL;
		String BDBstore;
		double max_size;
		int max_files = -1; 
				
		//Parsing args array
		if (args.length == 3 || args.length == 4)
		{
			URL = args[0];
			BDBstore = args[1];
			max_size = Double.parseDouble(args[2]);
			if (args.length == 4)
			{
				max_files = Integer.parseInt(args[3]);
			}
			//Creating database wrapper
			awsWrapper dbwrapper = new awsWrapper();
			//Getting crawler object
			XPathCrawlerFactory xf = new XPathCrawlerFactory();
			XPathCrawler x = xf.getCrawler();
			//Setting relevant instance variables of crawler
			x.setMaxFiles(max_files);
			x.setMaxSize(max_size);
			x.setDB(dbwrapper);
			//Seed URLS
//			x.place(URL);
//			x.place("https://en.wikipedia.org/wiki/Main_Page");
			try (BufferedReader br = new BufferedReader(new FileReader(new File("frontier.txt")))) 
			{
			    String line;
			    while ((line = br.readLine()) != null) 
			    {
			       x.place(line);
			    }
			} 
			catch (FileNotFoundException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			x.crawl();
			//Closing database
//			dbwrapper.close();
						
		}
		else
		{
			System.out.println("Usage: URL, BDBStore path, max file size, (optional) max no. files to retrieve");
			return;
		}
	}
	
	
}
