import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * this class is used to generate the final first two nodes of each block
 * emit (nodeid, node)
 * @author zhuchongwei
 *
 */
public class DataProcessingMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 String[] nodes = value.toString().trim().split("\\s+");
	     int nodeId = Integer.parseInt(nodes[0]);
	     if(Block.isResult(nodeId)) {
	    	 context.write(new LongWritable(nodeId), new Text(nodes[1]));
	     }
	 }
}
