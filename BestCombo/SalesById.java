package posmining.enshu;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * 全おにぎりの販売個数を出力する
 * @author akiba-mi (2014040)
 *
 */
public class SalesById {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(SalesById.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014040");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "salesById";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// ID
			String id = csv[PosUtils.RECEIPT_ID];
			// 販売額を設定
			String strCount = csv[PosUtils.ITEM_COUNT];
			int count = Integer.parseInt(strCount);
			String strPrice = csv[PosUtils.ITEM_PRICE];
			int price = Integer.parseInt(strPrice);
			int sales = count * price;
			String strSales = String.valueOf(sales);
			// 月
			String strMonth = csv[PosUtils.MONTH];
			// 時間
			String strHour = csv[PosUtils.HOUR];
			// カテゴリー
			String strCategory = csv[PosUtils.ITEM_CATEGORY_NAME];
			// 月、時間、販売額
			String strValue = strMonth + "," + strHour + "," + strSales + "," + strCategory;

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(id), new CSKV(strValue));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			String strMonth = "";
			String strHour = "";
			ArrayList<String> categories = new ArrayList<String>();
			for (CSKV value : values) {
				// csvファイルをカンマで分割して，配列に格納する
				String csv[] = value.toString().split(",");
				strMonth = csv[0];
				strHour  = csv[1];
				int sales = Integer.parseInt(csv[2]);
				sum += sales;
				categories.add(csv[3]);
			}
			String strSum = String.valueOf(sum);
			Collections.sort(categories);
			String strCategories = StringUtils.join(categories, "+");
			String strVal = strMonth + "," + strHour + "," + strSum + "," + strCategories;

			// emit
			context.write(key, new CSKV(strVal));
		}
	}
}
