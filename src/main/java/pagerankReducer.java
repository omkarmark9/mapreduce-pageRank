import java.io.IOException;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

public class pagerankReducer extends Reducer<Text, Text, Text, Text>{

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{

        String start = key.toString();
        double rank = 0;
        String[] word;
        StringBuffer out =  new StringBuffer();
        boolean flag = false;
        for (Text val:value){
            word = val.toString().split(" ");
            if(word[0].contentEquals("true")){
                rank += Double.valueOf(word[1]);
            }
            if (word[0].contentEquals("false")){
                flag = true;
                for (int i = 1; i< word.length; i++){
                    out.append(word[i]);
                    out.append(" ");
                }

            }
        }
        out.append(rank);
        if (flag) {
            context.write(new Text(start), new Text(out.toString()));
        }




    }
}

