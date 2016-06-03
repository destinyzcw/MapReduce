import java.util.*;
/**
 * this class is used to keep the data of nodes to avoid frequent parse of strings
 * @author zhuchongwei
 *
 */
public class Node implements Iterable<Integer> {
    protected int nodeid;
    protected double pageRank = 1.0 / 685230;
    protected int[] out;
    
    public Node(String nodeid, String pageRank, String out) {
    	this.nodeid = Integer.parseInt(nodeid);
    	this.pageRank = Double.parseDouble(pageRank);
    	if(out.equals("")) this.out = new int[0];
    	else {
    		String[] outNodes = out.split("\\,");
    		this.out = new int[outNodes.length];
    		int index = 0;
    		for(String outNode : outNodes) {
    			this.out[index++] = Integer.parseInt(outNode);
    		}
    	}
    }
    
    public String getoutNodes() {
    	if(out.length == 0) return "";
    	StringBuilder sb = new StringBuilder();
    	for(int outNode : out) {
    		sb.append(outNode);
    		sb.append(",");
    	}
    	sb.deleteCharAt(sb.length() - 1);
    	return sb.toString();
    }
	
    @Override
	public Iterator<Integer> iterator() {
        ArrayList<Integer> l = new ArrayList<Integer>();
        for(int i : out) {
            l.add(i);
        }
        return l.iterator();
	}
    
    
}
