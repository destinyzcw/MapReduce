import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * this mapper is used to filter the edges with netid to remove about 10%
 * and make the data in the format of nodeid, pr, outgoing nodes.
 * @author zhuchongwei
 *
 */
public class FirstMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String[] nodes = value.toString().trim().split("\\s+");

    	
    	if(nodes.length != 3) {
    		return;
    	}
    	
    	int fromNode = Integer.parseInt(nodes[0]);
    	int toNode = Integer.parseInt(nodes[1]);
    	double d = Double.parseDouble(nodes[2]);
    	
    	String node1 = null;
    	
    	Counter edges = context.getCounter(PageRankCounter.counter.EDGE);
   
    	if(!check(d)) {
    		node1 = fromNode + "";

    	}
    	else {
    		edges.setValue(edges.getValue() + 1);
    		node1 = fromNode + " " + toNode;
    	}
    	
		context.write(new LongWritable(fromNode), new Text(node1));
		String node2 = toNode + "";
		context.write(new LongWritable(toNode), new Text(node2));
    	
    	  	
    }
    /**
     * check if the node should be used by checking the value in the third column
     * reference : project instructions of cs5300p2
     * @param x
     * @return
     */
    private boolean check(double x) {
    	
        double fromNetID = 0.443;
        double rejectMin = 0.9 * fromNetID;
        double rejectLimit = rejectMin + 0.01;
        
        return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
    }
}
