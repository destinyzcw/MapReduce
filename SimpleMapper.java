import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * simple version of MR
 * emit (nodeid, node)
 * @author zhuchongwei
 *
 */
public class SimpleMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String[] nodes = value.toString().trim().split("\\s+");
    	
    	context.write(new LongWritable(Long.parseLong(nodes[0])), value);
    	
    	if(nodes.length == 3) {
    	
    	String[] outNodes = nodes[2].split("\\,");
    	
	    	for(String outNode : outNodes) {
	    		if(outNode == null) continue;
	    		try{
	    			long l = Long.parseLong(outNode);
	    			double pr = Double.parseDouble(nodes[1]) / outNodes.length;
	        		context.write(new LongWritable(l), new Text(String.valueOf(pr)));
	    		}
	    		catch (NumberFormatException e) {
	    			System.out.println("This String has error " + value.toString());
	    		}
	    	}
    	}
    	
    }
}
