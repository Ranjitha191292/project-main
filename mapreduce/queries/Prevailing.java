import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Prevailing
{
	public static class PrevailingMap extends Mapper<LongWritable,Text,Text,Text>
	{
				
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
		String str[]=values.toString().split("\t");
		String wage=str[6];
		String year=str[7];
		String title=str[4];
		String position=str[5];
		String status=str[1];
		String tot=wage+" "+year+" "+position+" "+status;
	    context.write(new Text(title),new Text(tot));
		
		
	}
	} 
	
	public static class PrevailPart extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key,Text value, int numReduceTasks)
	      {
	    	String mon[]=value.toString().split(" ");
	    	String year_part=mon[1];
	    	int yr=Integer.parseInt(year_part);
	        
	        
	        if(yr==2011)
	        {
	        	return 0 % numReduceTasks;
	        }
	        else if(yr==2012)
	        {
	        	return 1 % numReduceTasks;
	        }
	        else if(yr==2013)
	        {
	        	return 2 % numReduceTasks;
	        }
	        else if(yr==2014)
	        {
	        	return 3 % numReduceTasks;
	        }
	        else if(yr==2015)
	        {
	        	return 4 % numReduceTasks;
	        }
	        else
	        {
	        	return 5 % numReduceTasks;
	        }
	       
	      }
	   }

	
	public static class PrevailingReduce extends Reducer<Text,Text,Text,Text>
	{
		Map<Double,String> treemap1=new TreeMap<Double,String>(Collections.reverseOrder());
		Map<Double,String> treemap2=new TreeMap<Double,String>(Collections.reverseOrder());
         
		      

	public void reduce(Text k,Iterable<Text> val,Context context) throws IOException,InterruptedException
	{   
		long sum1=0;
		long sum2=0;
		double avg1=0;
		double avg2=0;
		int m=0;
		int n=0;
		
		String k1=k.toString();
		
		for(Text t:val)
		{			
		String all[]=t.toString().split(" ");
		String wage=all[0];
		//year=all[1];
		String position=all[2];
		String status=all[3];
		
		long wag=Long.parseLong(wage);
		
		
		if(status.equals("CERTIFIED") || status.equals("CERTIFIED WITHDRAWN"))
		{
		if(position.equals("Y"))
		{
			sum1=sum1+wag;
			m++;
		}
		else
		{
			sum2=sum2+wag;
			n++;
		}
			
		}
		
		}
		if(m!=0)
		{	
		avg1=sum1/m;
		}
		if(n!=0)
		{
		avg2=sum2/n;
		}
		if(avg1!=0)
		{
			treemap1.put(avg1,k1);
		}
		if(avg2!=0)
		{
			treemap2.put(avg2,k1);
		}
		//String two=year+" "+avg1+" "+avg2;
	  //context.write(k,new Text(two));
	}
	@SuppressWarnings("rawtypes")
	protected void cleanup(Context context) throws IOException,InterruptedException
	{
		Set set1=treemap1.entrySet();
		Set set2=treemap2.entrySet();
		
		Iterator i=set1.iterator();
		Iterator j=set2.iterator();
		
		String l1=null;
		String l2=null;
		String l3=null;
		
		
		
		while(i.hasNext()||j.hasNext())
		{
			if(i.hasNext())
			{
			   Map.Entry me1=(Map.Entry)i.next();
			   l1=me1.getValue().toString()+","+me1.getKey().toString();
			   
			}
			
			if(j.hasNext())
			{
				Map.Entry me2=(Map.Entry)j.next();
				l2=" "+me2.getValue().toString()+","+me2.getKey().toString();
					
			}
			l3=l1+l2;
			
			context.write(new Text(" "), new Text(l3));
			
		}
				
		}
	}
	
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Avg_Prevailng");
	
	job.setJarByClass(Prevailing.class);
	job.setMapperClass(PrevailingMap.class);
	job.setPartitionerClass(PrevailPart.class);
	job.setReducerClass(PrevailingReduce.class);
	
	job.setNumReduceTasks(6);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job,new Path (args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	}
  
