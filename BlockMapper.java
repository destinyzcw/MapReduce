import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
/**
 * emit (blockid, node)
 * @author zhuchongwei
 *
 */
public class BlockMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	/**
	 * map nodes into blocks and determine if it is BC or BE
	 */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String[] nodes = value.toString().trim().split("\\s+");
    	int nodeId = Integer.parseInt(nodes[0]);
    	int blockId = Block.getBlockId(nodeId);
    	String out = nodes.length == 3 ? nodes[2] : "";
    	String node = "BE " + nodes[0] + " " + nodes[1] + " " + out; 
    	// the node belong to the block is BE emit(blockId, node)
    	context.write(new LongWritable(blockId), new Text(node));
    	
    	if(nodes.length == 3) {
    		String[] outNodes = nodes[2].split("\\,");
    		//find the outgoing nodes of the "node", if it belong to a different block
    		//the "node" is BC of that blcok
    		for(String outNode : outNodes) {
    			int outNodeId = Integer.parseInt(outNode);
    			int outBlockId = Block.getBlockId(outNodeId);
    			if(outBlockId != blockId) {
    				String BCnode = "BC " + nodes[0] + " " + nodes[1] + " " + nodes[2]; 
    				context.write(new LongWritable(outBlockId), new Text(BCnode));
    			}
    		}
    	}
    	
    }
}
