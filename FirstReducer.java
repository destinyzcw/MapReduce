import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
/**
 * input (nodeid, node(with 0 or 1 outgoing)
 * for each node with the same nodeid collect the outgoings with a list
 * emit (nodeid, <pr, outgoings>)
 * @author zhuchongwei
 *
 */
public class FirstReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    	double defaultPR = 1.0 / 685230;
    	List<String> list = new ArrayList<>();
		for(Text t : values) {
			String[] node = t.toString().split("\\s+");
			if(node.length == 2) {
				list.add(node[1]);
			}
		}
		StringBuilder sb = new StringBuilder();
		sb.append(defaultPR);
		sb.append(" ");
		for(String toNode : list) {
			sb.append(toNode);
			sb.append(",");
		}
		if(list.size() != 0) sb.deleteCharAt(sb.length() - 1);
		context.write(key, new Text(sb.toString()));
    }
}
