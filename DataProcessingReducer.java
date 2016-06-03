import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * just write key value get from mapper
 * @author zhuchongwei
 *
 */
public class DataProcessingReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text t : values) {
			context.write(key, t);
		}
	}
}
