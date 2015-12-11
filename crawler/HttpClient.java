package edu.upenn.cis455.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;

/**
 * Implements a basic HTTP Client
 * @author Rohan Bopardikar
**/
public class HttpClient {
	
	//Initializing relevant variables, such as host, port, and headers
	private int port = 8080;
	private Socket socket;
	private String host = "localhost";
	private Map<String, String> headers;
	private PrintWriter out;
	private ResponseParser p;
	//RobotsTxtInfo object for this URL
	private RobotsTxtInfo robot;
	//Most specific user-agent match from robos.txt
	private String best_match = null;
	
	/**
	 * Basic constructor
	**/
	public HttpClient()
	{
		//Putting baseline headers into the Map
		headers = new HashMap<String,String>();
		headers.put("From", "cis455crawler");
		headers.put("Host", host + ":" + port);
		headers.put("User-Agent", "cis455crawler");
		headers.put("Accept",
				"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		headers.put("Accept-Language", "en-US,en;q=0.5");
		headers.put("Connection", "close");

	}
	
	/**
	 * Sends basic GET Request for requested URL
	 * @param url The URL to send request for
	 * @return Data of the webpage
	 */
	public String sendGet(String url)
	{
		//Getting most relevant user-agent from robots.txt
		best_match = findBestMatch();
		//Deicing whether or not to skip
		if (shouldSkip(url))
		{
			return "skipped";
		}
		if (url.startsWith(("https")))
		{
			return sendGetSecure(url);
		}
		try 
		{
			//Creating URL objects
			URL http_url = new URL(url);
			//Delaying visit if directed to by robots.txt
			crawlDelay();
			HttpURLConnection secure_connection = (HttpURLConnection)http_url.openConnection();
			//Setting request method
			secure_connection.setRequestMethod("GET");
			//Sending all headers except for host (since URLConnection will take care of this)
			for (String h: headers.keySet())
			{
				if (!h.equals("Host"))
				{
					secure_connection.setRequestProperty(h, headers.get(h));
				}
			}
			
			//Getting ResponseParser object to read in page content
			p = new ResponseParser(new BufferedReader(new InputStreamReader(secure_connection.getInputStream())));
			
			//Parsing all response headers from returned object
			Map<String,String> p_headers = p.getHeaders();
			Map<String, List<String>> response_headers = secure_connection.getHeaderFields();
			for (Map.Entry<String, List<String>> entry : response_headers.entrySet()) 
			{
				if (entry.getKey() == null)
				{
					p.setResponse(entry.getValue().get(0));
				}
				else
				{
					p_headers.put(entry.getKey(),entry.getValue().get(0));
				}
			}
			
			//If we didn't get any headers in the response, the URL was invalid
			if (p.getHeaders().size() == 0)
			{
				return ("invalid url");
			}
			
			//Otherwise return the contents of the returned document
			return p.getData();
			
		} 
		catch (IOException | IllegalArgumentException e) 
		{
			return "invalid url";
		}		
//		try
//		{
//			//Deals with secure request
//			if (url.startsWith(("https")))
//			{
//				return sendGetSecure(url);
//			}
//			//Parsing host and port from URL
//			URLInfo url_info = new URLInfo(url);
//			host = url_info.getHostName();
//			port = url_info.getPortNo();
//			
//			//Changing Host header if necessary
//			headers.put("Host", host + ":" + port);
//			
//			//Getting file path of URL
//			String file_path = url_info.getFilePath();
//			
//			//If we weren't able to find a host, URL is invalid
//			if (host == null)
//			{
//				return "invalid url";
//			}
//			
//			//Delaying visits based on robots.txt crawl delay
//			crawlDelay();
//			
//			//Otherwise, opening up socket and sending request with all headers
//			socket = new Socket(host, port);
//			out = new PrintWriter(socket.getOutputStream(), true);
//			String message = "GET " + file_path + " HTTP/1.1\r\n";
//			for (String header: headers.keySet())
//			{
//				String head = header + ": " + headers.get(header) + "\r\n";
//				message = message + head;
//			}
//			out.println(message);
//
//			//Creating ResponseParser object to parse the Response
//			p = new ResponseParser(socket);
//			
//			//If we didn't get any headers in the response, the URL was invalid
//			if (p.getHeaders().size() == 0)
//			{
//				return ("invalid url");
//			}
//			//Otherwise return the contents of the returned document
//			return p.getData();
//		} 
//		catch (UnknownHostException e) 
//		{
//			System.err.println("Unknown Host Exception");
//			System.err.println("Problem url is: http://www.youtube.com/motherboardtv?feature=watch&trk_source=motherboard");
//		} 
//		catch (IOException | IllegalArgumentException e) 
//		{
//			System.err.println("IO Exception");;
//		}
//		//Return null for all caught exceptions (Servlet will deal with this)
//		return null;
	}
	
	/**
	 * Sends HEAD request to specified URL
	 * @param url - URL to request
	 * @return String indicating success status of request
	 */
	public String sendHead(String url)
	{
		//Getting most relevant user-agent from robots.txt
		if (robot!=null)
		{
			best_match = findBestMatch();
		}
		//Deciding if robots.txt says we should skip url
		if (shouldSkip(url))
		{
			return "skipped";
		}
		try
		{ 
			//Dealing with secure requests
			if (url.startsWith(("https")))
			{
				return sendHeadSecure(url);
			}
			
			//Creating URL and connection objects
			URL http_url = new URL(url);
			HttpURLConnection secure_connection = (HttpURLConnection) http_url.openConnection();
			//Setting request method to HEAD
			secure_connection.setRequestMethod("HEAD");
			//Sending all headers except for host (since URLConnection will take care of this)
			for (String h: headers.keySet())
			{
				if (!h.equals("Host"))
				{
					secure_connection.setRequestProperty(h, headers.get(h));
				}
			}
			//Setting all Response Headers 
			p = new ResponseParser();
			Map<String,String> p_headers = p.getHeaders();
			Map<String, List<String>> response_headers = secure_connection.getHeaderFields();
			for (Map.Entry<String, List<String>> entry : response_headers.entrySet()) 
			{
				if (entry.getKey() == null)
				{
					p.setResponse(entry.getValue().get(0));
				}
				else
				{
					p_headers.put(entry.getKey(),entry.getValue().get(0));
				}
			}
			//If we didn't get any headers in the response, the URL was invalid
			if (p.getHeaders().size() == 0)
			{
				return ("invalid url");
			}
			
			//Otherwise return the contents of the returned document
			return ("good head");
			
		} 
		catch (IOException | IllegalArgumentException e) 
		{
			return "invalid url";
		}
			
			
			
//			//Parsing host and port from URL
//			URLInfo url_info = new URLInfo(url);
//			host = url_info.getHostName();
//			port = url_info.getPortNo();
//			
//			//Changing Host header if necessary
//			headers.put("Host", host + ":" + port);
//			
//			//Getting file path of URL
//			String file_path = url_info.getFilePath();
//			
//			//If we weren't able to find a host, URL is invalid
//			if (host == null)
//			{
//				return "invalid url";
//			}
//			
//			//Otherwise, opening up socket and sending request with all headers
//			socket = new Socket(host, port);
//			out = new PrintWriter(socket.getOutputStream(), true);
//			String message = "HEAD " + file_path + " HTTP/1.1\r\n";
//			for (String header: headers.keySet())
//			{
//				String head = header + ": " + headers.get(header) + "\r\n";
//				message = message + head;
//			}
//			out.println(message);
//
//			//Creating ResponseParser object to parse the Response
//			p = new ResponseParser(socket);
//					
//			//If we didn't get any headers in the response, the URL was invalid
//			if (p.getHeaders().size() == 0)
//			{
//				return ("invalid url");
//			}
//			
//			//Otherwise return the contents of the returned document
//			return p.getData();
//		} 
//		catch (UnknownHostException e) 
//		{
//			System.err.println("Unknown Host Exception");
//			System.err.println("Problem url is: " + url);
//		} 
//		catch (IOException | IllegalArgumentException e) 
//		{
//			System.err.println("IO Exception");;
//		}
//		//Return null for all caught exceptions (Servlet will deal with this)
//		return null;
	}
	
	/**
	 * Sends HTTPS HEAD request
	 * @param url - the URL to request
	 * @return String with success status of request
	 */
	public String sendHeadSecure(String url)
	{
		try 
		{
			//Creating URL and connection objects
			URL https_url = new URL(url);
			HttpsURLConnection secure_connection = (HttpsURLConnection)https_url.openConnection();
			//Setting request method to HEAD
			secure_connection.setRequestMethod("HEAD");
			//Sending all headers except for host (since URLConnection will take care of this)
			for (String h: headers.keySet())
			{
				if (!h.equals("Host"))
				{
					secure_connection.setRequestProperty(h, headers.get(h));
				}
			}
			//Setting all Reposnse Headers 
			p = new ResponseParser();
			Map<String,String> p_headers = p.getHeaders();
			Map<String, List<String>> response_headers = secure_connection.getHeaderFields();
			for (Map.Entry<String, List<String>> entry : response_headers.entrySet()) 
			{
				if (entry.getKey() == null)
				{
					p.setResponse(entry.getValue().get(0));
				}
				else
				{
					p_headers.put(entry.getKey(),entry.getValue().get(0));
				}
			}
			//If we didn't get any headers in the response, the URL was invalid
			if (p.getHeaders().size() == 0)
			{
				return ("invalid url");
			}
			
			//Otherwise return the contents of the returned document
			return ("good head");
			
		} 
		catch (IOException | IllegalArgumentException e) 
		{
			e.printStackTrace();
			return "invalid url";
		}
	}
	
	/**
	 * Sends HTTPS GET Request
	 * @param url - the URL to request
	 * @return String with URL content
	 */
	public String sendGetSecure(String url)
	{
		try 
		{
			//Creating URL objects
			URL https_url = new URL(url);
			//Delaying visit if directed to by robots.txt
			crawlDelay();
			HttpsURLConnection secure_connection = (HttpsURLConnection)https_url.openConnection();
			//Setting request method
			secure_connection.setRequestMethod("GET");
			//Sending all headers except for host (since URLConnection will take care of this)
			for (String h: headers.keySet())
			{
				if (!h.equals("Host"))
				{
					secure_connection.setRequestProperty(h, headers.get(h));
				}
			}
			
			//Getting ResponseParser object to read in page content
			p = new ResponseParser(new BufferedReader(new InputStreamReader(secure_connection.getInputStream())));
			
			//Parsing all response headers from returned object
			Map<String,String> p_headers = p.getHeaders();
			Map<String, List<String>> response_headers = secure_connection.getHeaderFields();
			for (Map.Entry<String, List<String>> entry : response_headers.entrySet()) 
			{
				if (entry.getKey() == null)
				{
					p.setResponse(entry.getValue().get(0));
				}
				else
				{
					p_headers.put(entry.getKey(),entry.getValue().get(0));
				}
			}
			
			//If we didn't get any headers in the response, the URL was invalid
			if (p.getHeaders().size() == 0)
			{
				return ("invalid url");
			}
			
			//Otherwise return the contents of the returned document
			return p.getData();
			
		} 
		catch (IOException | IllegalArgumentException e) 
		{
			return "invalid url";
		}		
	}
	
	/**
	 * Finds and parsers robots.txt file
	 * @param url - URL to find appropriate robots.txt
	 * @param robo_map Cached robots.txt object from current crawl, if any
	 */
	public void robots(String url, Map<String,RobotsTxtInfo> robo_map)
	{
//		if (url.equals("https://www.reddit.com//r/goldbenefits"))
//		{
//			System.out.println("here");
//		}
		
		//If we have a secure request
		if (url.startsWith("https"))
		{
			try 
			{
				//Open up connection and try to see if a robots.txt file exists on the server
				URL url_info = new URL(url);
				host = url_info.getHost();
						
				if (robo_map.containsKey(host))
				{
					robot = robo_map.get(host);
					return;
				}
				else
				{
					String new_url = "https://" + host + "/robots.txt";
					
					URL https_url = new URL(new_url);
					HttpsURLConnection secure_connection = (HttpsURLConnection)https_url.openConnection();
					secure_connection.setRequestMethod("GET");
					for (String h: headers.keySet())
					{
						if (!h.equals("Host"))
						{
							secure_connection.setRequestProperty(h, headers.get(h));
						}
						
					}
					//Putting the robots.txt object into the map, if any exists
					try
					{
						robo_map.put(host,parseRobot(new BufferedReader(new InputStreamReader(secure_connection.getInputStream()))));
					}
					catch (IllegalArgumentException e)
					{
						robot = new RobotsTxtInfo(); 
						return;
					}
				}
			} 
			catch (IOException e) 
			{
				robot = new RobotsTxtInfo(); 
				return;
			}
		}
		//If not secure URL
		else
		{
			//Open up connection and try to see if a robots.txt file exists on the server
			URLInfo url_info = new URLInfo(url);
			host = url_info.getHostName();
			if (robo_map.containsKey(host))
			{
				robot = robo_map.get(host);
				return;
			}
			else
			{
				port = url_info.getPortNo();
				headers.put("Host", host + ":" + port);
				String file_path = "/robots.txt";
				
				//If we weren't able to find a host, URL is invalid
				if (host == null)
				{
					robot = new RobotsTxtInfo(); 
					return;
				}
				
				//Otherwise, opening up socket and sending request with all headers
				try 
				{
					socket = new Socket(host, port);
					out = new PrintWriter(socket.getOutputStream(), true);
					String message = "GET " + file_path + " HTTP/1.1\r\n";
					for (String header: headers.keySet())
					{
						String head = header + ": " + headers.get(header) + "\r\n";
						message = message + head;
					}
					out.println(message);
					BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					//Putting robots.txt object into map, if any
					robo_map.put(host, parseRobot(socketReader));
				}
				catch (IOException e)
				{
					robot = new RobotsTxtInfo(); 
					return;
				}
				
			}
		}
	}
	
	/**
	 * Parses the robots.txt file
	 * @param socketReader - BufferedReader from connected socket
	 * @return RobotsTxtInfo object with all its instance variables set
	 */
	public RobotsTxtInfo parseRobot(BufferedReader socketReader)
	{
		try 
		{
			//Initializing variables
			String input;
			String user_agent = null;
			robot = new RobotsTxtInfo();
			
			//Reading in file
			while ((input = socketReader.readLine()) != null) 
			{
				//Ignoring comments
				if (input.startsWith("#"))
				{
					continue;
				}
				String[] input_split = input.split(":");
				if (input_split.length > 1)
				{
					//Setting user agent if found
					if (input.startsWith("User-agent"))
					{
						user_agent = input_split[1].substring(1);
						robot.addUserAgent(user_agent);
					}
					//Setting disallowed URLs for User agent
					else if (input.startsWith("Disallow"))
					{
						robot.addDisallowedLink(user_agent, input_split[1].substring(1));
					}
					//Setting allowed URLs for User agent
					else if (input.startsWith("Allow"))
					{
						robot.addAllowedLink(user_agent, input_split[1].substring(1));
					}
					//Setting crawl-delay for User agent
					else if (input.startsWith("Crawl-delay"))
					{
						try
						{
							robot.addCrawlDelay(user_agent, Integer.parseInt(input_split[1].substring(1)));
						}
						catch (Exception e)
						{
						}
					}
				}
			}
			return robot;
		} 
		catch (IOException e) 
		{
			return null;
		}
	}
	
	/**
	 * Finds most relevant user-agent in robots.txt file
	 * Example: if there are user-agents * and cis455crawler, gives cis455crawler
	 * @return String with most relevant user agent
	 */
	public String findBestMatch()
	{		
		if (robot.containsUserAgent("cis455crawler"))
		{
			best_match = "cis455crawler";
		}
		else if (robot.containsUserAgent("*"))
		{
			best_match = "*";
		}
		return best_match;
	}
	
	/**
	 * Determines if a URL should be skipped based on robots.txt
	 * If the disallowed and allowed options both apply, picks whichever has a longer match
	 * @param url - The URL we needed to determine if we should skip or not
	 * @return - true if we should skip the URL, false otherwise
	 */
	public boolean shouldSkip(String url)
	{
		boolean temp_bool = false;	
		
		//Trying URLs with and without / at end 
		String new_url;
		if (url.substring(url.length()-1).equals("/"))
		{
			new_url = url.substring(0,url.length()-1);
		}
		else
		{
			new_url = url + "/";
		}
		
		//Searching disallowed links
		if (best_match!=null && robot.getDisallowedLinks(best_match) !=null)
		{
			for (String s: robot.getDisallowedLinks(best_match))
			{
				if (url.contains(s) || new_url.contains(s))
				{
					if (robot.getAllowedLinks(best_match)!=null)
					{
						//Searching allowed links
						for (String s2: robot.getAllowedLinks(best_match))
						{
							//Returning longer match
							if ((url.contains(s2) || new_url.contains(s2)) && s2.length() > s.length())
							{
								temp_bool = false;
								continue;
							}
						}
					}
					temp_bool = true;
				}
			}
		}
		return temp_bool;
	}
	
	/**
	 * Users java.util's TimeUnit to delay page visitation
	 * as specified in crawl-delay in robots.txt.
	 * 
	 * Treats value in robots.txt as seconds to wait before
	 * subsequent visits
	 */
	public void crawlDelay()
	{
//		int delay = robot.getCrawlDelay(best_match);
//		if (delay!=-1)
//		{
//			try 
//			{
//				TimeUnit.SECONDS.sleep(robot.getCrawlDelay(best_match));
//			} 
//			catch (InterruptedException e) 
//			{
//				return;
//			}
//		}
	}
	
	//Basic Getters and Setters
	
	public int getPort()
	{
		return port;
	}
	
	public String getHost()
	{
		return host;
	}
	
	public Map<String,String> getHeaders()
	{
		return headers;
	}
	
	public void setPort(int newPort)
	{
		port = newPort;
	}
	
	public void setHost(String newHost)
	{
		host = newHost;
	}
	
	public ResponseParser getParser()
	{
		return p;
	}
	
	public Map<String,String> getResponseHeaders()
	{
		return p.getHeaders();
	}
	
	public String getResponse()
	{
		return p.getResponse();
	}
	
	
}
