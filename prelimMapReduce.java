package indexer;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

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

import com.amazonaws.AmazonServiceException;

public class prelimMapReduce {
	static int count = 0;
	
	/*lic static void main(String[]args) throws IOException, ClassNotFoundException, InterruptedException{
		
		String line; 
        String arguements[];
        Configuration conf = new Configuration();

        // compute the total number of attributes in the file
        FileReader infile = new FileReader("input/in1");
        BufferedReader bufread = new BufferedReader(infile);
        line = bufread.readLine();
        System.out.println(line);
        arguements = line.split(","); // split the fields separated by comma
        conf.setInt("argno", arguements.length); // saving that attribute value 
        Job job = new Job(conf, "nb");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class); 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
	}*/
 
		
		
		
		/*Map test= new Map();
		test.map("","", "ie.html");
		test.map("","", "spacejam.html");
		test.map("","", "rivs.html");
		ArrayList<String> tarr = test.getArrList();
		Reduce testR = new Reduce();
		String lastKey = "";
		ArrayList<String> sameVals = new ArrayList<String>();
		for(String line: tarr){
			//System.out.println("main: "+line);
			String[] bb = line.split("\t");
			
			//System.out.println("ln "+ bb[0] + " " + bb[1]);
			//System.out.println("asdf   :" + lastKey+"_:_"+ bb[0]);
			if(lastKey.equals(bb[0])){
				//System.out.println(bb[0]+"_equals_"+ lastKey);
				sameVals.add(bb[1]);
				//System.out.println(sameVals.size());
				//System.out.println(sameVals.size());
			}
			else{
				//System.out.println(bb[0]+"_NOT_equals_"+ lastKey);
				//if(!lastKey.equals(null)){
				testR.reduce(lastKey, sameVals.iterator());
				sameVals= new ArrayList<String>();
				sameVals.add(bb[1]);
				
			}
			//System.out.println(line);
			lastKey = bb[0];
			//testR.reduce(bb[0], );
		}
		String lastKey2 = "";
		ArrayList<String> sameVals2 = new ArrayList<String>();
		idfMapReduce.Reduce idgafos = new idfMapReduce.Reduce();
		for(String s: testR.rArr){
			String key = s.substring(0, s.indexOf(" "));
			String val = s.substring(s.indexOf(" "));
			//System.out.println(key+"\t"+val);
			if(lastKey2.equals(key)){
				sameVals2.add(val);
			}
			else{
				idgafos.reduce(lastKey2, sameVals2.iterator());
				sameVals2= new ArrayList<String>();
				sameVals2.add(val);
			}
			lastKey2 = key;
			
		}
		
	}*/
	
	//map
	public static class Map 
		extends Mapper<Object, Text,Text, Text>{
			
		
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			//ArrayList<String> mapList;
			
			//MiniDoc mini = new MiniDoc("www.test.com", Corpus.downloadFile("ie.html"));
			//String[] words = Corpus.downloadFile(fileLoc);
			//mapList = new ArrayList<String>();
			
			String inLine= value.toString();
			String[] inLines = inLine.split(" ");
			count++;
			System.out.println(count);
			String[] words =null;
			try{
			//download words from a page	
			words=Downloader.downloadFile(inLines[0]);
			
			if(words==null){ }
			//only if words work
			else{
			int wordCount = words.length;
			
			for(int i = 0; i<words.length; i++){
				//System.out.println(word);
				
				// clean up words
				//words[i] = words[i].toLowerCase();
				//words[i] = words[i].replaceAll("[^a-z0-9]", ""); // get rid of strange chars
				//brake for empty words
				
				// stopwords
				if(words[i].equals("")||words[i].equals(null)||words[i].equals("a")||words[i].equals("the")||words[i].equals("this")||words[i].equals("able")
						||words[i].equals("about")||words[i].equals("across")||words[i].equals("after")||words[i].equals("all")||words[i].equals("almost")
						||words[i].equals("also")||words[i].equals("am")||words[i].equals("among")||words[i].equals("an")||words[i].equals("and")||words[i].equals("any")
						||words[i].equals("are")||words[i].equals("as")||words[i].equals("at")||words[i].equals("be")||words[i].equals("because")||words[i].equals("been")||
						words[i].equals("cannont")||words[i].equals("could")||words[i].equals("could")||words[i].equals("dear")||words[i].equals("did")||words[i].equals("do")
						||words[i].equals("does")||words[i].equals("either")||words[i].equals("else")||words[i].equals("ever")||words[i].equals("every")||words[i].equals("for")
						||words[i].equals("from")||words[i].equals("get")||words[i].equals("got")||words[i].equals("had")||words[i].equals("has")||words[i].equals("have")
						||words[i].equals("he")||words[i].equals("her")||words[i].equals("hers")||words[i].equals("him")||words[i].equals("his")||words[i].equals("how")||
						words[i].equals("however")||words[i].equals("i")||words[i].equals("if")||words[i].equals("in")||words[i].equals("into")||words[i].equals("is")
						||words[i].equals("it")||words[i].equals("its")||words[i].equals("just")||words[i].equals("least")||words[i].equals("let")||words[i].equals("like")
						||words[i].equals("likely")||words[i].equals("may")||words[i].equals("me")||words[i].equals("might")||words[i].equals("most")||words[i].equals("must")
						||words[i].equals("my")||words[i].equals("neither")||words[i].equals("nor")||words[i].equals("not")||words[i].equals("of")||words[i].equals("off")
						||words[i].equals("often")||words[i].equals("on")||words[i].equals("only")||words[i].equals("or")||words[i].equals("other")||words[i].equals("our")
						||words[i].equals("rather")||words[i].equals("said")||words[i].equals("say")||words[i].equals("she")||words[i].equals("should")||words[i].equals("since")
						||words[i].equals("so")||words[i].equals("some")||words[i].equals("than")||words[i].equals("that")||words[i].equals("that")||words[i].equals("their")
						||words[i].equals("them")||words[i].equals("then")||words[i].equals("there")||words[i].equals("these")||words[i].equals("they")||words[i].equals("tis")
						||words[i].equals("to")||words[i].equals("too")||words[i].equals("twas")||words[i].equals("us")||words[i].equals("wants")||words[i].equals("was")
						||words[i].equals("we")||words[i].equals("were")||words[i].equals("what")||words[i].equals("when")||words[i].equals("where")||words[i].equals("which")||
						words[i].equals("when")||words[i].equals("where")||words[i].equals("which")||words[i].equals("while")||words[i].equals("who")||words[i].equals("whom")
						||words[i].equals("why")||words[i].equals("will")||words[i].equals("with")||words[i].equals("would")||words[i].equals("yet")||words[i].equals("you")
						||words[i].equals("your")){
					//System.out.println("empt");
					continue;
				}
				else{
					//write url / count of 1
					Text keyText = new Text(words[i] + " " + inLines[1]);
					String outVal = new String("1 " + wordCount + " " +i);
					Text oText= new Text(outVal);
					context.write(keyText, oText);
					//mapList.add(words[i] + " " + fileLoc +"\t"+"1 " + wordCount +" "+ i +" 1");
				}
			}
				//System.out.println(words[i] + " " + mini.url +"\t"+"1 " + wordCount +" "+ i +" 1
			}
			}
			catch(AmazonServiceException ase){
				System.out.println("err");
				return;
			}	
			
			
		}
		
		
	}
	
	//map
		public static class Reduce extends Reducer<Text, Text, Text, Text>{
			
		
		//int count2=0;	
			@Override
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				
				if(key.equals("")){
					return;
				}
				int specWC = 0;
				int wc = -1;
				Text val = new Text(":");
				
				Iterator<Text> itterAbl = values.iterator();
				while((val= itterAbl.next())!=null){
					
					//System.out.println("valIt " + specWC);
					/*
					 * value[0] = specific wordCount
					 * value[1] = total wordCount
					 * 
					 * 
					 */
					
					
					String v1 = val.toString();
					String value[] = v1.split(" ");
					// get wc
					specWC += Integer.parseInt(value[0]);
					wc = Integer.parseInt(value[1]);
					//indexSum += 1 + Integer.parseInt(value[2]);
					
					//System.out.println(key+" : "+"SpecWC: " + wc);
					//long freq = specWC/wc;
					//System.out.print(freq); System.out.println(": wc");//long freq = specWC/wc;
					//System.out.println(freq);
					
					
					if(itterAbl.hasNext()==false){
						break;
					}
					
					
				}
				//write output
				Text oText = new Text(
						new String(""+specWC+" "+wc));
				context.write(key, oText);
				
				
				
			}


			
		}
}
