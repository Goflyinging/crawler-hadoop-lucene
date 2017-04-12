import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lxing on 2017/4/11.
 */
public class OptimizerMapper extends TableMapper<ImmutableBytesWritable, LongWritable> {
    public static Logger logger = LoggerFactory.getLogger(OptimizerMapper.class);

    private LongWritable value = new LongWritable(1);

    public void map(ImmutableBytesWritable key, Result values,
                    Context context) throws IOException, InterruptedException {
        for (Cell cell : values.rawCells()) {
            if ("status".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                //状态为0开始抓取
                if ("0".equals(Bytes.toString(CellUtil.cloneValue(cell)))) {
                    logger.info("url：" + Bytes.toString(key.get()));
                    context.write(key, value);
                }
            }
        }
    }

}