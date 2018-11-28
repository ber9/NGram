import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: berg
 * @Date: 18-10-5 下午9:35
 * @Description:
 **/
public class NGramLibraryBuilder {
    //LongWritable Text:input的key-value形式,Text IntWritable:output的key-value形式
    //Context:Allows the Mapper/Reducer to interact with the rest of the Hadoop System
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        int noGram;

        //setup在initialize mapper的时候会被调用一次，而且只被调用一次。数据只需读取一次，数据不改变。
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("noGram", 5);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: read sentence
            //I love big n=3
            /* I love -> 1
             * love big -> 1
             * I love big -> 1
             * */
//            Configuration conf = context.getConfiguration();
//            int noGram = conf.getInt("noGram", 5);
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+");
            if (words.length < 2) {
                return;
            }
            StringBuilder sb;
            for (int i = 0; i < words.length; i++) {
                sb = new StringBuilder();
                sb.append(words[i]);
                for (int j = 1; i + j < words.length && j < noGram; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString()), new IntWritable(1));
                }
            }

        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
    