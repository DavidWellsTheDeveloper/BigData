import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
public class  NGram {
    public static class MapGram extends Mapper<Object, Text, Text, NullWritable> {
		private Text word = new Text();
		
// 		Profile 1 
// 		Profile 2 key should be documentID since we are grouping by articles.
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
// 			get article title and docID split by special delimiter.
			while (itr.hasMoreTokens()) {
                String[] article = itr.nextToken("\n\n").split("<====>");
                Text title = new Text(article[0]);
                Text documentID = new Text(article[1]);
                String[] textOfArticle = article[2].split(" ");
//              Go through each word in the article
                for(String word: textOfArticle) {
                    context.write(sanatizeToken(word), NullWritable.get());
                }
				
			}
		}
		
		private Text sanatizeToken(String word) {
            String sanatizing = word.replaceAll("[^a-zA-Z0-9]", "");
            sanatizing = sanatizing.toLowerCase(); 
            return new Text(sanatizing);
        }
	}
	
	public static class PartitionerGram extends Partitioner<Text,NullWritable>{
        @Override
        public int getPartition(Text key, NullWritable value, int numReduceTasks){
            Double evenSplitRatio = 26 / numReduceTasks
            int ascii = (int)(key.toString().toLowerCase().charAt(0))
            if (numReduceTasks ==5) {
                
            }
        }
	}
	
// 	Sorting should occur here
	public static class ReduceGram extends Reducer<Text,NullWritable,Text,NullWritable> {
		public void reduce(Text key,Iterable<NullWritable> values, Context context) throws IOException, InterruptedException 
		{
			context.write(key, NullWritable.get());
		}
	}
	
    
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Create instance of a job
        Job job = Job.getInstance(conf, "N Gram");
        job.setJarByClass(NGram.class);
        
        use first arg as input and second as output files, third as a profile
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int profile = Integer.parseInt(args[2]);
        job.setMapperClass(NGram.MapGram.class);
        job.setReducerClass(NGram.ReduceGram.class);
        job.setNumreduceTasks(5)
        switch(profile) {
            case 1:
                
                break;
            case 2:
                job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
                break;
            case 3:
                
        }
        
        
        Take inputs and run job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
