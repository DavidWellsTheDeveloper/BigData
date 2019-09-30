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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
public class  NGram {

	
    public static class PartitionerGram extends Partitioner<Text,NullWritable>{
        @Override
        public int getPartition(Text key, NullWritable value, int numReduceTasks){
            if (key.toString().charAt(0) < 'c') {
                return 0;
            }
            if (key.toString().charAt(0) < 'h') {
                return 1;
            }
            if (key.toString().charAt(0) < 'm') {
                return 2;
            }
            if (key.toString().charAt(0) < 't') {
                return 3;
            }
            else {
                return 4;
            }
        }
    }

    public static class MapGram extends Mapper<Object, Integer, Text, NullWritable> {
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

    public static class ReduceGram extends Reducer<Text,NullWritable,Text,NullWritable> {
        public void reduce(Text key,Iterable<NullWritable> values, Context context) throws IOException, InterruptedException 
        {
            context.write(key, NullWritable.get());
        }
    }

    
    
    public static class MapGram2 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private TreeMap<Text, Integer> repToRecordMap = new TreeMap<Text, Integer>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            //  get article title and docID split by special delimiter.
            while (itr.hasMoreTokens()) {
                String[] article = itr.nextToken("\n\n").split("<====>");
                Text title = new Text(article[0]);
                Text documentID = new Text(article[1]);
                String[] textOfArticle = article[2].split(" ");
                // Go through each word in the article
                for(String word: textOfArticle) {
                    word = sanatizeToken(word).toString();
                    if (word != null) {
                        Text compositeKey = new Text(documentID.toString() + "<====>" + sanatizeToken(word).toString());
                        context.write(compositeKey, one);
                    }
                }
            }
        }
        private Text sanatizeToken(String word) {
            String sanatizing = word.replaceAll("[^a-zA-Z0-9]", "");
            sanatizing = sanatizing.toLowerCase();
        return new Text(sanatizing);
        }
    }

	
    public static class ReduceGram2 extends Reducer<Text,IntWritable,Text,NullWritable> {
        private IntWritable result = new IntWritable();
        private TreeMap<String, NullWritable> repToRecordMap = new TreeMap<String, NullWritable>();
        public void reduce(Text key,Iterable<IntWritable>values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
			for (IntWritable val :values)
			{
				sum += val.get();
			}
			result.set(sum);
			
			String[] keyarr = key.toString().split("<====>");
			if (keyarr.length == 2) {
                String strNum = padNumbers(sum);
                repToRecordMap.put(keyarr[0] + "<====>" + strNum + "<====>" + keyarr[1], NullWritable.get());
			}
			
//             if (repToRecordMap.size() > 500) {
//                 repToRecordMap.remove(repToRecordMap.firstKey());
//             }
        }
        private String padNumbers(Integer sum) {
            String strNum = Integer.toString(sum);
            while (strNum.length() < 2) {
                strNum = "0" + strNum;
            }
            return strNum;
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (repToRecordMap.size() >= 1) {
            String t = repToRecordMap.pollLastEntry().getKey();
            String[] record = t.split("<====>");
            Text text = new Text(record[0] + "\t" + record[2] + "\t" + record[1]);
                context.write(text, NullWritable.get());
            } 
        }
    }
    
    
    public static class MapGram3 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private TreeMap<Text, Integer> repToRecordMap = new TreeMap<Text, Integer>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            //  get article title and docID split by special delimiter.
            while (itr.hasMoreTokens()) {
                String[] article = itr.nextToken("\n\n").split("<====>");
                String[] textOfArticle = article[2].split(" ");
                // Go through each word in the article
                 for(String word: textOfArticle) {
                    word = sanatizeToken(word).toString();
                    if (word != null) {
                        Text w = new Text(word);
                        context.write(w, one);
                    }
                }
            }
        }
        private Text sanatizeToken(String word) {
            String sanatizing = word.replaceAll("[^a-zA-Z0-9]", "");
            sanatizing = sanatizing.toLowerCase();
            return new Text(sanatizing);
        }
    }
    
    
    public static class ReduceGram3 extends Reducer<Text,IntWritable,Text,NullWritable> {
        private IntWritable result = new IntWritable();
        private TreeMap<String, NullWritable> repToRecordMap = new TreeMap<String, NullWritable>();
        public void reduce(Text key,Iterable<IntWritable>values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
			for (IntWritable val :values)
			{
				sum += val.get();
			}
			result.set(sum);
            
            String strNum = padNumbers(sum);
            repToRecordMap.put(strNum + "<====>" + key, NullWritable.get());
			
            if (repToRecordMap.size() > 500) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
        private String padNumbers(Integer sum) {
            String strNum = Integer.toString(sum);
            while (strNum.length() < 5) {
                strNum = "0" + strNum;
            }
            return strNum;
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (repToRecordMap.size() >= 1) {
                Map.Entry t = repToRecordMap.pollFirstEntry();
                String[] record = t.getKey().toString().split("<====>");
                Text text = new Text(record[1] + "\t" + record[0]);
                context.write(text, NullWritable.get());
            } 
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//         Create instance of a job
        Job job = Job.getInstance(conf, "N Gram");
        job.setJarByClass(NGram.class);
        
//         use first arg as input and second as output files, third as a profile
        int profile = Integer.parseInt(args[2]);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        switch(profile) {
            case 1:
                job.setOutputValueClass(NullWritable.class);
                job.setMapperClass(NGram.MapGram.class);
                job.setReducerClass(NGram.ReduceGram.class);
                break;
            case 2:
                job.setOutputValueClass(IntWritable.class);
                job.setMapperClass(NGram.MapGram2.class);
                job.setReducerClass(NGram.ReduceGram2.class);
                break;
            case 3:
                job.setOutputValueClass(IntWritable.class);
                job.setMapperClass(NGram.MapGram3.class);
                job.setReducerClass(NGram.ReduceGram3.class);
                break;
                
        }
        
        
//         Take inputs and run job
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
