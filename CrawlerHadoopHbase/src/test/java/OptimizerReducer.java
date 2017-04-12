import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * Created by lxing on 2017/4/11.
 */
public class OptimizerReducer extends
        TableReducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable> {

    public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {

    }

}