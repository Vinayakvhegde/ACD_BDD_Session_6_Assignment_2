import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class OlympicIndList {
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
if (args.length != 2) {
System.err.println("Usage: stdsubscriber <in> <out>");
System.exit(2);
}
Job job = new Job(conf, "Assignment 2 :Olympic India List");
job.setJarByClass(OlympicIndList.class);
job.setMapperClass(TokenizerMapper.class);
//job.setCombinerClass(SumReducer.class);
job.setReducerClass(SumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(LongWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}

	public static class SumReducer 
		extends	Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
	
		public void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
						throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			this.result.set(sum);
			context.write(key, this.result);
		}
	}

	public static class TokenizerMapper 
		extends	Mapper<Object, Text, Text, LongWritable> {

		Text year = new Text();
		LongWritable totalMedals = new LongWritable();

		public void map(Object key, Text value,	Mapper<Object, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");
			if (parts[CDRConstants.country].equalsIgnoreCase("India")) {
				year.set(parts[CDRConstants.year]);
				totalMedals.set(Long.parseLong(parts[CDRConstants.totalMedals]));
				context.write(year, totalMedals);
			}
		}
	}

}

class CDRConstants {

    public static int athleteName = 0;
    public static int athleteAge = 1;
    public static int country = 2;
    public static int year = 3;
    public static int closingDate = 4;
    public static int sport = 5;
    public static int goldCount = 6;
    public static int silverCount = 7;
    public static int bronzeCount = 8;
    public static int totalMedals = 9 ;
    

}