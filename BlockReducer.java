import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * input (blockid, node)
 * emit (nodeid, <pr, outgoings>)
 * @author zhuchongwei
 *
 */
public class BlockReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	private HashMap<Integer, Double> PR;
	private HashMap<Integer, ArrayList<Node>> BE;
	private HashMap<Integer, ArrayList<Node>> BC;
	private HashMap<Integer, Double> NPR;
	private static final double d = 0.85;
	
	/**
	 * for each node sent to this block first tell if it is a BC node, then construct BE node 
	 * and iterate to calculate the PR of each node in this block
	 * terminate condition : residual in block is less than 0.001 or iterate 10 times
	 */
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
    	PR = new HashMap<>();
    	BE = new HashMap<>();
    	BC = new HashMap<>();
    	NPR = new HashMap<>();
    	HashMap<Integer, Node> vertices = new HashMap<>();
    	
    	HashMap<Integer, Node> boundNodes = new HashMap<>();
    	double residual = 0;
    	for(Text t : values) {
    		String[] nodes = t.toString().trim().split("\\s+");
    		String out = nodes.length == 4 ? nodes[3] : "";
    		Node node = new Node(nodes[1], nodes[2], out);
    		
    		if(nodes[0].equals("BE")) {
    			vertices.put(node.nodeid, node);
    		}
    		else {
    			boundNodes.put(node.nodeid, node);
    		}
    	}
    	
    	for(Node n : boundNodes.values()) {
    		for(Integer blockNode : n) {
        		if(vertices.containsKey(blockNode)){
        			if(!BC.containsKey(blockNode)) {
        				ArrayList<Node> bounds = new ArrayList<>();
        				bounds.add(n);
        				BC.put(blockNode, bounds);
        			}
        			else {
        				BC.get(blockNode).add(n);
        				
        			}
        		}
    		}
    	}
    	for(Node n : vertices.values()) {
    		PR.put(n.nodeid, n.pageRank);
    		for(Integer blockNode : n) {
        		if(vertices.containsKey(blockNode)){
        			if(!BE.containsKey(blockNode)) {
        				ArrayList<Node> bounds = new ArrayList<>();
        				bounds.add(n);
        				BE.put(blockNode, bounds);
        			}
        			else {
        				BE.get(blockNode).add(n);
        			}
        		}
    		}
    	}
    	
    	int count = 0;
    	do {
    		residual = iterateGaussian(vertices);
    		count++;
    	} while(residual > 0.001);
    	
    	Counter reduceMR = context.getCounter(PageRankCounter.counter.MR);
    	reduceMR.setValue(reduceMR.getValue() + count);
        Counter globalResidual = context.getCounter(PageRankCounter.counter.RESIDUAL);
        double currResidual = 0.0;
        for(Node n : vertices.values()) {
        	currResidual += Math.abs(NPR.get(n.nodeid) - n.pageRank) / NPR.get(n.nodeid);
        }
        long curr = (long) Math.floor(currResidual * Integer.MAX_VALUE);
        globalResidual.setValue(globalResidual.getValue() + curr);
    	for(Node n : vertices.values()) {
    		n.pageRank = NPR.get(n.nodeid);
    		String out = n.getoutNodes();
    		String output = "" + n.pageRank + " " + out;
    		context.write(new LongWritable(n.nodeid), new Text(output));
    	}
    	System.out.println(vertices.size());
    }
    
    /**
     * using Jocobi method to iterate PR in blocks
     * reference : project instructions of cs5300
     * @param vertices
     * @return
     */
    private double iterateBlockOnce(HashMap<Integer, Node> vertices) {
		double residual = 0.0;
    	for(Node n : vertices.values()) {
    		NPR.put(n.nodeid, 0.0);
    	}
    	for(Node n : vertices.values()) {
    		if(BE.containsKey(n.nodeid)) {
        		for(Node blockNode : BE.get(n.nodeid)) {
        			NPR.put(n.nodeid, NPR.get(n.nodeid) + PR.get(blockNode.nodeid) / blockNode.out.length);
        		}
    		}
    		if(BC.containsKey(n.nodeid)) {
        		for(Node boundNode : BC.get(n.nodeid)) {
        			NPR.put(n.nodeid, NPR.get(n.nodeid) + boundNode.pageRank / boundNode.out.length);
        		}
    		}
    		NPR.put(n.nodeid, d * NPR.get(n.nodeid) + (1 - d) / 685230);
    	}
    	for(Node n : vertices.values()) {
    		residual += Math.abs(NPR.get(n.nodeid) - PR.get(n.nodeid)) / NPR.get(n.nodeid);
    		PR.put(n.nodeid, NPR.get(n.nodeid));
    	}
		residual /= vertices.size();
    	return residual;
    }
    /**
     * using Gaussian method to iterate PR in blocks
     * @param vertices
     * @return
     */
    private double iterateGaussian(HashMap<Integer, Node> vertices) {
    	double residual = 0.0;
    	for(Node n : vertices.values()) {
    		NPR.put(n.nodeid, PR.get(n.nodeid));
    	}
    	for(Node n : vertices.values()) {
    		NPR.put(n.nodeid, 0.0);
    		if(BE.containsKey(n.nodeid)) {
        		for(Node blockNode : BE.get(n.nodeid)) {
        			NPR.put(n.nodeid, NPR.get(n.nodeid) + NPR.get(blockNode.nodeid) / blockNode.out.length);
        		}
    		}
    		if(BC.containsKey(n.nodeid)) {
        		for(Node boundNode : BC.get(n.nodeid)) {
        			NPR.put(n.nodeid, NPR.get(n.nodeid) + boundNode.pageRank / boundNode.out.length);
        		}
    		}
    		NPR.put(n.nodeid, d * NPR.get(n.nodeid) + (1 - d) / 685230);
    	}
    	for(Node n : vertices.values()) {
    		residual += Math.abs(NPR.get(n.nodeid) - PR.get(n.nodeid)) / NPR.get(n.nodeid);
    		PR.put(n.nodeid, NPR.get(n.nodeid));
    	}
		residual /= vertices.size();
    	return residual;
    }
}
