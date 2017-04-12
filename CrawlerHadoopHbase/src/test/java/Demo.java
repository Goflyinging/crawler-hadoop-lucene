import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by lxing on 2017/4/10.
 */
public class Demo {
    private static final Logger LOG = Logger.getLogger(Demo.class);
    public static void main(String[] args) {
        ImmutableBytesWritable key = new ImmutableBytesWritable("http://www.csdn.net/".getBytes());
        Text text = new Text();
        text.set(key.get());
        System.out.println(text.toString());
        System.out.println(Bytes.toString(key.get()));
        LOG.info("this is log");
        System.out.print("good!!");
    }
}
