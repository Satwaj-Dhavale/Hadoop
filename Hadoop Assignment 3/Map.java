import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        
        String st [] = value.toString().split("\\s+");        
        for(String st1 :  st) {
   
        	if(st1.startsWith("z")) {
        		word.set("Z_count");
        		
        		context.write(word, new IntWritable(st1.length())); 
        	}
        }

    }
}
