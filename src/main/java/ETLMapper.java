import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text k = new Text();
    private StringBuilder sb = new StringBuilder();

    private Counter pass;
    private Counter fail;

    //计数清洗数据量
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //有效数据
        pass = context.getCounter("ETL", "Pass");
        //垃圾数据
        fail = context.getCounter("ETL", "Fail");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //以\t切分数据
        String[] fields = value.toString().split("\t");

        //数据清洗
        //当字段少于9，忽略，记录为垃圾数据（“ETL Fail” +1）
        if (fields.length < 9) {
            fail.increment(1);
        } else {

            //根据第4列的特殊性(有的电影具有多个类型，它们用&相连，如：Film & Animation)
            //先处理第四列
            //去除类型字段中的空格
            fields[3] = fields[3].replace(" ", "");

            sb.setLength(0);

            //重新拼接字段
            for (int i = 0; i < fields.length; i++) {
                //如果是最后一个字段，不进行字段拼接
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else if (i < 9) {
                    //如果为前8个字段，用\t拼接
                    sb.append(fields[i]).append("\t");
                } else {
                    //剩余字段(相关视频)，用&拼接(保证array类型的字段分隔符一致)
                    sb.append(fields[i]).append("&");
                }
            }

            pass.increment(1);

            k.set(sb.toString());
            context.write(k, NullWritable.get());

        }

    }
}