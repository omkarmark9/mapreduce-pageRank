import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

public class pagerank {
    public static void main(String[] args) throws Exception {


        Path inputp = new Path(args[0]);
	Path outputg = null;
	for(int i =1 ; i<4; i++){
		outputg = new Path(args[1] + i);
		Job job0 = new Job();
        	job0.setJarByClass(pagerank.class);
        	job0.setJobName("Page Rank");

        	FileInputFormat.addInputPath(job0, inputp);
        	FileOutputFormat.setOutputPath(job0, outputg);

        	job0.setMapperClass(pagerankMapper.class);
        	job0.setReducerClass(pagerankReducer.class);

        	job0.setOutputKeyClass(Text.class);
        	job0.setOutputValueClass(Text.class);
        	job0.setNumReduceTasks(1);
		job0.waitForCompletion(true);
		inputp = new Path(args[1] + i + "/part-r-00000");
	}

        
        
    }
}

