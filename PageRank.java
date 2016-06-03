
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class PageRank {

    public static void main(String[] args) throws IOException {

    	preProcess(args);
    	pageRankHandler(true, args);
    	pageRankHandler(false, args);
     
    }
    /**
     * preprocess MR
     * filter the edges and make the data to be nodeid, pr, outgoings
     * @throws IOException
     */
    private static void preProcess(String[] args) throws IOException {
    	
    	Job job = getFirstJob();
    	String inputPath = args[0];
        String outputPath = args[1] + "/Data";

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        try { 
            job.waitForCompletion(true);
        } catch(Exception e) {
            System.err.println("ERROR IN JOB: " + e);
            return;
        }
        
        Counters counters = job.getCounters();
        Counter edges = counters.findCounter(PageRankCounter.counter.EDGE);
        System.out.println(edges.getValue());
          
        
    }
    
    /**
     * key part of the project
     * to execute simple or block version of PR
     * terminate condition : overall residual is below 0.001
     * and write residual to a file
     * @param isSimple
     * @throws IOException
     */
    private static void pageRankHandler(boolean isSimple, String[] args) throws IOException {
    	
    	int count = 1;
		String inputPath = null;
	    String outputPath = null;
	    double threshold = 0.0009;
		Job job = null;
	    String path = isSimple ? "/SimpleStage" : "/BlockStage";
    	while(true) {
    		if (count == 1) {
    			job = isSimple ? getSimpleJob() : getBlockJob();
    			inputPath = args[1] + "/Data";
    	        outputPath = args[1] + path + count;
    		}
    		else {
    			job = isSimple ? getSimpleJob() : getBlockJob();
    			inputPath = args[1] + path + (count - 1);
    			outputPath = args[1] + path + count;
    		}
    		
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            try { 
                job.waitForCompletion(true);
            } catch(Exception e) {
                System.err.println("ERROR IN JOB: " + e);
                return;
            }
            
            Counters counters = job.getCounters();
            Counter residual = counters.findCounter(PageRankCounter.counter.RESIDUAL);
            double currSum = 1.0 * residual.getValue() / Integer.MAX_VALUE / 685230;
            Counter reduceMR = counters.findCounter(PageRankCounter.counter.MR);

            
        	String message = "Iteration " + count + " avg error " + currSum;
        	System.out.println(message);
        	if(!isSimple){
        		System.out.println("Total number of iterations in reducers " + String.valueOf(1.0 * reduceMR.getValue() / 68));
        	}
            
            if(currSum < threshold) break; 
            
            residual.setValue(0);
            reduceMR.setValue(0);
            count++;
            
    	}
    	
    	
    	if(!isSimple) {
    		dataProcess(count, args);
    	}
            
    }
    
    /**
     * generate first 2 nodes of the block for block PR
     * @param count
     * @throws IOException
     */
    private static void dataProcess(int count, String[] args) throws IOException {
    	
    	Job job = getDataProcessingJob();
    	String inputPath = args[1] + "/BlockStage" + count;
    	String outputPath = args[1] + "/Result";
    	
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    	
        try { 
            job.waitForCompletion(true);
        } catch(Exception e) {
            System.err.println("ERROR IN JOB: " + e);
            return;
        }
    }
    /**
     * generate preprocessing job
     * @return
     * @throws IOException
     */
    private static Job getFirstJob() throws IOException {
    	Configuration conf = new Configuration();
        Job job = new Job(conf, "FirstJob");

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(PageRank.class);

        return job;
    }
    
    /**
     * generate simple PR job
     * @return
     * @throws IOException
     */
    private static Job getSimpleJob() throws IOException {
    	Configuration conf = new Configuration();
        Job job = new Job(conf, "SimpleJob");

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(PageRank.class);

        return job;
    }
    
    /**
     * generate block PR job
     * @return
     * @throws IOException
     */
    private static Job getBlockJob() throws IOException {
    	Configuration conf = new Configuration();
        Job job = new Job(conf, "BlockJob");

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(BlockMapper.class);
        job.setReducerClass(BlockReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(PageRank.class);

        return job;
    }
    
    /**
     * generate dataprocessing of block PR (to get first 2 nodes of each block) job
     * @return
     * @throws IOException
     */
    private static Job getDataProcessingJob() throws IOException {
    	Configuration conf = new Configuration();
        Job job = new Job(conf, "DataProcessingJob");

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(DataProcessingMapper.class);
        job.setReducerClass(DataProcessingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(PageRank.class);

        return job;
    }

    
}
