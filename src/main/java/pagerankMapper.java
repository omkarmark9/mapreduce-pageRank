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


public class pagerankMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        String delimn = "\n";
        String delims = " ";
        String[] line = value.toString().split(delimn);
        for(int i = 0; i < line.length; i++){
            String[] word = line[i].split(delims);
            boolean isRank = true;
            int links = (word.length - 2);
            double rank = Double.valueOf(word[(word.length - 1)]);
            double finalRank = rank/(double)links;
            String add = "";
            for(int j = 1; j < (word.length) - 1; j++){
                String fill = "";
                add =add + word[j] + delims;
                fill = isRank + delims + finalRank;
                context.write(new Text(word[j]), new Text(fill));
            }
            isRank = false;
            context.write(new Text(word[0]), new Text(isRank+delims+add));

        }


    }

}

