import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * simple version of MR
 * if the value is a node then record its outgoings
 * if it is a pr value cumulate it and make it the new pr of the node
 * input (nodeid, node or pr)
 * emit (nodeid, <pr, outgoings>)
 * @author zhuchongwei
 *
 */
public class SimpleReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    	
    	double d = 0.85;
    	int N = 685230;
    	String outNodes = "";
    	double sum = 0.0;
    	double prev = 0.0;
    	for(Text value : values) {
    		String[] nodes = value.toString().trim().split("\\s+");
    		if(nodes.length == 3) {
    			outNodes = nodes[2];
    			prev = Double.parseDouble(nodes[1]);
    		}
    		else if (nodes.length == 2) {
    			prev = Double.parseDouble(nodes[1]);
    		}
    		else {
    			sum += Double.parseDouble(value.toString());
    		}
    	}
        sum = (1.0 - d) / N + d * sum;
        Counter residual = context.getCounter(PageRankCounter.counter.RESIDUAL);
        double currResidual = Math.abs(prev - sum) / sum;
        residual.setValue((long) (residual.getValue() + Math.floor(currResidual * Integer.MAX_VALUE)));
    	String output = sum + " " + outNodes;
    	context.write(key, new Text(output));
    	
    }
}

