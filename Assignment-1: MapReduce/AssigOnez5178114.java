/*
 * Assignment-1: MapReduce
 * Created by Rongtao Shen on 19/10/2019
 * Student ID: z5178114
 * There are two jobs in my program, each job has one mapper and one reducer.
 * In addition, I create two custom writables, respectively MovieRatingWritable and MoviePairWritable.
 * In the first mapper(UserMapper), I split each row of data by "::" and store them in a string array.
 * After splitting the data, I use MovieRatingWritable to create "val" object to store user id, movie id
 * and rating by using set* method in MovieRatingWritable. The key of the first mapper is the user id
 * which I can gain by using getUser_id() method in MovieRatingWritable.
 * The value of the first mapper is "val".
 * After shuffling and sorting, the data is transfered to the first reducer(UserReducer).
 * Since the value is iterable, I traverse the values and use getMovie_Rating method in MovieRatingWritable
 * to connect the value which have the same key.
 * Another custom writable called MoviePairWritable which is used in the second job.
 * In the second mapper(MovieMapper), I firstly split each row of data which is created by the first reducer by "\t"
 * and store them in a string array.
 * The first element of the string array is the user id and I use MoviePairWritable to create an another object.
 * I use setUser_id method to store user_id and continue to split the second part of string array.
 * Then I use two "for" loop to combine two different movies as well as their ratings and store them in the object
 * by using setMovie_1, setMovie_2 setRating_1 and setRating_2 method in MoviePairWritable.
 * I use getMovie_Pair method to set the key of second mapper and the value is the MoviePairWritable object.
 * In second reducer, I use getTuple() method to connect every user and their rating pair.
 * In addition, I use the addList() method in MoviePairWritable to add every user and their rating pairs to the list.
 * Finally, the output is the movie pairs and the list of ratings of the user.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class AssigOnez5178114 {
	
	public static class MovieRatingWritable implements Writable {
		private Text user_id;
		private Text movie_id;
		private IntWritable rating;
		
		public MovieRatingWritable() {
			this.user_id = new Text("");
			this.movie_id = new Text("");
			this.rating = new IntWritable(-1);
		}
		
		public void setUser_id(Text user_id) {
			this.user_id = user_id;
		}
		
		public String getUser_id() {
			return user_id.toString();
		}
		
		public void setMoive_id(Text movie_id) {
			this.movie_id = movie_id;
		}
		
		public void setRating(IntWritable rating) {
			this.rating = rating;
		}
		
		public String getMovie_Rating() {
			return this.movie_id.toString() + "," + this.rating.toString() + " ";
		}
		
		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie_id.readFields(data);
			this.rating.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie_id.write(data);
			this.rating.write(data);
		}
	}

	public static class MoviePairWritable implements Writable {
		private Text movie_1;
		private Text movie_2;
		private Text user_id;
		private IntWritable rating_1;
		private IntWritable rating_2;
		
		public MoviePairWritable() {
			this.movie_1 = new Text("");
			this.movie_2 = new Text("");
			this.user_id = new Text("");
			this.rating_1 = new IntWritable(-1);
			this.rating_2 = new IntWritable(-1);
			
		}
		public void setMoive_1(Text movie_1) {
			this.movie_1 = movie_1;
		}
		
		public void setMovie_2(Text movie_2) {
			this.movie_2 = movie_2;
		}
		
		public void setUser_id(Text user_id) {
			this.user_id = user_id;
		}
		
		public void setRating_1(IntWritable rating_1) {
			this.rating_1 = rating_1;
		}
		
		public void setRating_2(IntWritable rating_2) {
			this.rating_2 = rating_2;
		}
		
		public String getMovie_Pair() {
			return "(" + this.movie_1.toString() + "," + this.movie_2.toString() + ")";
		}
		
		public String getTuple() {
			return "(" + this.user_id.toString() + "," + this.rating_1.toString() + "," + this.rating_2.toString() + ")" + ",";
		}
		
		public String addList(String User_RatingPairs) {
			return "[" + User_RatingPairs + "]";
		}
		
		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie_1.readFields(data);
			this.movie_2.readFields(data);
			this.user_id.readFields(data);
			this.rating_1.readFields(data);
			this.rating_2.readFields(data);	
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie_1.write(data);
			this.movie_2.write(data);
			this.user_id.write(data);
			this.rating_1.write(data);
			this.rating_2.write(data);
		}
	}

	public static class UserMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieRatingWritable>.Context context)
				throws IOException, InterruptedException {
			String [] parts = value.toString().split("::");
			MovieRatingWritable val = new MovieRatingWritable();
			val.setUser_id(new Text(parts[0]));
			val.setMoive_id(new Text(parts[1]));
			val.setRating(new IntWritable(Integer.parseInt(parts[2])));
			context.write(new Text(val.getUser_id()), val);
		}
	}
	
	public static class UserReducer extends Reducer<Text, MovieRatingWritable, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<MovieRatingWritable> values,
				Reducer<Text, MovieRatingWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			String val = new String();
			for(MovieRatingWritable value: values)
				val += value.getMovie_Rating();
			context.write(key, new Text(val));
		}
	}

	public static class MovieMapper extends Mapper<LongWritable, Text, Text, MoviePairWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MoviePairWritable>.Context context)
				throws IOException, InterruptedException {
			String [] key_value = value.toString().split("\t");
			MoviePairWritable val = new MoviePairWritable();
			val.setUser_id(new Text(key_value[0]));
			String [] movie_rating = key_value[1].split(" ");
			if (movie_rating.length > 1) {
				for(int i = 0; i < movie_rating.length; i++) {
					for(int j = i + 1; j < movie_rating.length; j++) {
						String [] pairs_1 = movie_rating[i].split(",");
						String [] pairs_2 = movie_rating[j].split(",");
						if (pairs_1[0].compareTo(pairs_2[0]) < 0) {
							val.setMoive_1(new Text(pairs_1[0]));
							val.setRating_1(new IntWritable(Integer.parseInt(pairs_1[1].toString())));
							val.setMovie_2(new Text(pairs_2[0]));
							val.setRating_2(new IntWritable(Integer.parseInt(pairs_2[1].toString())));
							context.write(new Text(val.getMovie_Pair()), val);
						} else if (pairs_1[0].compareTo(pairs_2[0]) > 0) {
							val.setMoive_1(new Text(pairs_2[0]));
							val.setRating_1(new IntWritable(Integer.parseInt(pairs_2[1].toString())));
							val.setMovie_2(new Text(pairs_1[0]));
							val.setRating_2(new IntWritable(Integer.parseInt(pairs_1[1].toString())));
							context.write(new Text(val.getMovie_Pair()), val);
						}
					}
				}
			}
		}
	}

	public static class MovieReducer extends Reducer<Text, MoviePairWritable, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<MoviePairWritable> values, Reducer<Text, MoviePairWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			MoviePairWritable output = new MoviePairWritable();
			String value = new String();
			for(MoviePairWritable val: values) {
				value += val.getTuple();
			}
			value = value.substring(0, value.length() - 1);
			context.write(key, new Text(output.addList(value)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		Job job1 = Job.getInstance(conf, "User Join");
		Job job2 = Job.getInstance(conf, "Movie Join");
		
		job1.setMapperClass(UserMapper.class);
		job1.setReducerClass(UserReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MovieRatingWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
		
		job1.waitForCompletion(true);

		job2.setMapperClass(MovieMapper.class);
		job2.setReducerClass(MovieReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MoviePairWritable.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path(out, "out1"));
		FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

		job2.waitForCompletion(true);
	}
}
