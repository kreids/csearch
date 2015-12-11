package indexer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class idfMapReduce {
	//map class
	public static class Map
	extends Mapper<Object, Text,Text, Text>{
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			//parses url out of key and puts it in value
			String sVal = value.toString();
			int space = sVal.indexOf(" ");
			
			
		
			Text keyText = new Text(sVal.substring(0, space ));
			Text valText = new Text(sVal.substring(space+1));
			
			
			
			context.write(keyText, valText);
		
		}
	}
	public static class Reduce
	extends Reducer<Text, Text, Text, Text>{
		//maps every word to a value which contains all urls containing it and necessary data to calculate tf-idf
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			if(key.toString().equals("this")){
				//System.out.println();
			}
			StringBuffer sb = new StringBuffer();
			
			if(key.equals("")){
				return;
			}
			//System.out.println(key);
			int i =0;
			
			Text val = new Text(":");
			
			
			Iterator<Text> itterAbl = values.iterator();
			
			//System.out.println(key.toString()+"\t"+val.toString());
			
			Stack<String> oldVals= new Stack<String>();
			
			while((val= itterAbl.next())!=null){
				i++;
				//System.out.println("Steve "+key.toString()+" : " +val.toString());
				//System.out.println(key.toString()+"\t"+ val.toString());
				//System.out.println("\t:::"+val);
				//System.out.println("valIt " + specWC);
				/*
				 * value[0] = specific wordCount
				 * value[1] = total wordCount
				 * value[2] = index
				 * value[3] = corpus size
				 */
				//System.out.println(key+" : "+val);
				
				// push urls and word counts to a stack
				oldVals.push(val.toString());
				
				
				//System.out.println(key+" : " + val);
				//String value[] = val.toString().split(" ");
				
				//indexSum +=
				
				//System.out.println(key+" : "+"SpecWC: " + wc);
				//long freq = specWC/wc;
				//System.out.print(freq); System.out.println(": wc");//long freq = specWC/wc;
				//System.out.println(freq);
				if(itterAbl.hasNext()==false){
					break;
				}
				
			}
			
			
			//
			//Text oVal = new Text(""+ i);
			//System.out.println(key.toString()+i);
			Text ogVal;// =oldVals.pop();
			//System.out.println(key+" : " + ogVal + " " + i);
			
			
			
			while(!oldVals.isEmpty()){
				//pop urls from stack and add them to value
				ogVal =new Text(oldVals.pop());
				//System.out.println(key+" : " + ogVal + " " + i);
				String ogg = ogVal.toString();
				String toAppend = ogg.toString();
				toAppend = toAppend.replace("\t", " - ");
				System.out.println(toAppend);
				
				//append values to write val
				sb.append(toAppend + " _ ");
				//ogVal =oldVals.pop();
				//context.write(key, new Text(ogVal + " " +i) );
			}
		///	System.out.println(key+" : " + ogVal + " " + i);
			context.write(key, new Text(""+i+"\t"+ sb.toString()));
			//System.out.println(key+" : " + oVal);
			
		}
	}
	
	
}
