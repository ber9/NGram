import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.hsqldb.lib.Collection;

import java.io.IOException;
import java.util.*;

/**
 * @Author: berg
 * @Date: 18-11-28 上午9:34
 * @Description:
 **/
public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;

        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            threshold = configuration.getInt("threshold", 20);
        }

        //input:I love big data\t10
        //output:key = I love big value = data=10
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //I love big data\t10
            String line = value.toString().trim();
            String[] wordsPlusCount = line.split("\t");
            if (wordsPlusCount.length < 2)
                return;
            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.parseInt(wordsPlusCount[1]);

            if (count < threshold)
                return;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]);
                sb.append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int topK;

        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            topK = configuration.getInt("topK", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            //key = i love big
            //value = <data=10,girl=100,boy=1000...>
            for (Text val : values) {
                String value = val.toString().trim();
                String word = value.split("=")[0].trim();
                int count = Integer.parseInt(value.split("=")[1].trim());
                if (tm.containsKey(word))
                    tm.get(count).add(word);
                else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }
            Iterator<Integer> iter = tm.keySet().iterator();
            for (int j = 0; iter.hasNext() && j < topK; ) {
                int keyCount = iter.next();
                List<String> words = tm.get(keyCount);
                for (int i = 0; i < words.size() && j < topK; i++) {
                    context.write(new DBOutputWritable(key.toString(), words.get(i),keyCount),NullWritable.get());
                    j++;
                }
            }
        }
    }
}
    