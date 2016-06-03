/**
 * to let the counter of MR to keep the overall residual 
 * and #of iterations inside a reducer in block MR
 * @author zhuchongwei
 *
 */
public class PageRankCounter {
	public enum counter {
		RESIDUAL,
		MR,
		EDGE
	}
}
