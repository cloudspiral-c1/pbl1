package posmining.enshu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.enshu.AveragePaymentByMonth.MyMapper;
import posmining.enshu.AveragePaymentByMonth.MyReducer;
import posmining.utils.CSKV;
import posmining.utils.PosUtils;

public class SalesCountByPrice {
	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(SalesCountByPrice.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014025");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "out/salesById";
		String outputpath = "out/SalesCountByPrice";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(12);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String keyvalue[] = value.toString().split("\t");
			String csv[] = keyvalue[1].split(",");

			// valueとなる販売個数を取得
			String month = csv[0];
			int cost = Integer.parseInt(csv[2]);
			if(month.equals("06"))
			if(cost>=0 && cost<100){
				context.write(new CSKV("100円以下"), new CSKV(1));
			}else if(cost>=100 && cost<200){
				context.write(new CSKV("100円台"), new CSKV(1));
			}else if(cost>=200 && cost<300){
				context.write(new CSKV("200円台"), new CSKV(1));
			}else if(cost>=300 && cost<400){
				context.write(new CSKV("300円台"), new CSKV(1));
			}else if(cost>=400 && cost<500){
				context.write(new CSKV("400円台"), new CSKV(1));
			}else if(cost>=500 && cost<600){
				context.write(new CSKV("500円台"), new CSKV(1));
			}else if(cost>=600 && cost<700){
				context.write(new CSKV("600円台"), new CSKV(1));
			}else if(cost>=700 && cost<800){
				context.write(new CSKV("700円台"), new CSKV(1));
			}else if(cost>=800 && cost<900){
				context.write(new CSKV("800円台"), new CSKV(1));
			}else if(cost>=900 && cost<1000){
				context.write(new CSKV("900円台"), new CSKV(1));
			}else if(cost>=1000){
				context.write(new CSKV("1000円以上"), new CSKV(1));
			}
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			// 合計
			int count=0;
			for(CSKV cskv : values){
				count++;
			}
			// emit
			context.write(new CSKV(key.toString()), new CSKV(count));
		}
	}

}
