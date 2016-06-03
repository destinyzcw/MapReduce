/**
 * get the block id of each node and determine if it is the first two nodes of the block
 * @author zhuchongwei
 *
 */
public class Block {
	// cumulate the number of blocks to make it easy to get the blockId in O(1)
	private static int[] blockId = {10328,20373,30629,40645,50462,60841,70591,80118
			,90497,100501,110567,120945,130999,140574,150953,161332,171154
			,181514,191625,202004,212383,222762,232593,242878,252938,263149
			,273210,283473,293255,303043,313370,323522,333883,343663,353645
			,363929,374236,384554,394929,404712,414617,424747,434707,444489
			,454285,464398,474196,484050,493968,503752,514131,524510,534709
			,545088,555467,565846,576225,586604,596585,606367,616148,626448
			,636240,646022,655804,665666,675448,685230,Integer.MAX_VALUE};
	
	/**
	 * get the blockid
	 * @param nodeId
	 * @return
	 */
	public static int getBlockId(int nodeId) {
		int index = nodeId / 10000;
		if(index == 0) return 0;
		if(blockId[index - 1] <= nodeId) {
			return index;
		}
		else {
			return index - 1;
		}
	}
	
	/**
	 * determin if it is the first two nodes
	 * @param nodeId
	 * @return
	 */
	public static boolean isResult(int nodeId) {
		int BlockId = getBlockId(nodeId);
		if(BlockId == 0) return (nodeId == 0 || nodeId == 1);
		else  return (nodeId == blockId[BlockId - 1] || nodeId == blockId[BlockId - 1] + 1);
	}

}
