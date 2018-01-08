import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AvgGrowth
{
	public static class AvgGrowthMap extends Mapper<LongWritable,Text,Text,Text>
	{
				
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
		String str[]=values.toString().split("\t");
		String job_title=str[4];
		String year=str[7];
		context.write(new Text(job_title),new Text (year));	
		
	
	}
	} 
	
	

	
	public static class AvgGrowthReduce extends Reducer<Text,Text,Text,Text>
	{
		Map<Double,Text> tree=new TreeMap<Double,Text>(Collections.reverseOrder());
		int a,b,c,d,e,f;
		double avggrowth;
		      

	public void reduce(Text k,Iterable<Text> val,Context context) throws IOException,InterruptedException
	{   
		for(Text t:val)
		{
		if(t.equals("2011"))
		{
			a++;
		}
		else if(t.equals("2012"))
		{
			b++;
		}
		else if(t.equals("2013"))
		{
			c++;
		}
		else if(t.equals("2014"))
		{
			d++;
		}
		else if(t.equals("2015"))
		{
			e++;
		}
		else 
		{
			f++;
		}
		}
		avggrowth=((b-a)+(c-b)+(d-c)+(e-d)+(f-e))/5;
		
		tree.put(avggrowth,k);
		
	}
	@SuppressWarnings("rawtypes")
	protected void cleanup(Context context) throws IOException,InterruptedException
	{
		Set set3=tree.entrySet();
		Iterator i1=set3.iterator();
		int j=0;
		
		while(i1.hasNext() && j<15)
		{
			
			Map.Entry me3=(Map.Entry)i1.next();
		    context.write(new Text(me3.getValue().toString()), new Text(me3.getKey().toString()));
			j++;
			
			
		}
			
	}
			
	}		
			
	
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Avg_Growth");
	
	job.setJarByClass(AvgGrowth.class);
	job.setMapperClass(AvgGrowthMap.class);
	job.setReducerClass(AvgGrowthReduce.class);
	
	job.setNumReduceTasks(1);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job,new Path (args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	}
  
