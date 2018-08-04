package com.gyq.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class AppLogDataClean {
	
	public static class AppLogDataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		Text k = null;
		NullWritable v=null;
		SimpleDateFormat sdf=null;
//		多路输出器，在指定文件夹输出
		MultipleOutputs<Text, NullWritable> mos = null;
				
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			k =  new Text();
			v = NullWritable.get();
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			mos = new MultipleOutputs<>(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			
//			将string转换成json对象
			JSONObject jsonObj = JSON.parseObject(value.toString());
//			取header中的数据
			JSONObject headerObj = jsonObj.getJSONObject(GlobalConstants.HEADER);
			

			/**
			 * 过滤缺失必选字段的记录
			 */
			if (null == headerObj.getString("sdk_ver") || "".equals(headerObj.getString("sdk_ver").trim())) {
				return;
			}

			if (null == headerObj.getString("time_zone") || "".equals(headerObj.getString("time_zone").trim())) {
				return;
			}

			if (null == headerObj.getString("commit_id") || "".equals(headerObj.getString("commit_id").trim())) {
				return;
			}

			if (null == headerObj.getString("commit_time") || "".equals(headerObj.getString("commit_time").trim())) {
				return;
			}else{
				// 练习时追加的逻辑，替换掉原始数据中的时间戳
				String commit_time = headerObj.getString("commit_time");
				String format = sdf.format(new Date(Long.parseLong(commit_time)+695*24*60*60*1000L));
				headerObj.put("commit_time", format);
				
			}

			if (null == headerObj.getString("pid") || "".equals(headerObj.getString("pid").trim())) {
				return;
			}

			if (null == headerObj.getString("app_token") || "".equals(headerObj.getString("app_token").trim())) {
				return;
			}

			if (null == headerObj.getString("app_id") || "".equals(headerObj.getString("app_id").trim())) {
				return;
			}

			if (null == headerObj.getString("device_id") || headerObj.getString("device_id").length()<17) {
				return;
			}

			if (null == headerObj.getString("device_id_type")
					|| "".equals(headerObj.getString("device_id_type").trim())) {
				return;
			}

			if (null == headerObj.getString("release_channel")
					|| "".equals(headerObj.getString("release_channel").trim())) {
				return;
			}

			if (null == headerObj.getString("app_ver_name") || "".equals(headerObj.getString("app_ver_name").trim())) {
				return;
			}

			if (null == headerObj.getString("app_ver_code") || "".equals(headerObj.getString("app_ver_code").trim())) {
				return;
			}

			if (null == headerObj.getString("os_name") || "".equals(headerObj.getString("os_name").trim())) {
				return;
			}

			if (null == headerObj.getString("os_ver") || "".equals(headerObj.getString("os_ver").trim())) {
				return;
			}

			if (null == headerObj.getString("language") || "".equals(headerObj.getString("language").trim())) {
				return;
			}

			if (null == headerObj.getString("country") || "".equals(headerObj.getString("country").trim())) {
				return;
			}

			if (null == headerObj.getString("manufacture") || "".equals(headerObj.getString("manufacture").trim())) {
				return;
			}

			if (null == headerObj.getString("device_model") || "".equals(headerObj.getString("device_model").trim())) {
				return;
			}

			if (null == headerObj.getString("resolution") || "".equals(headerObj.getString("resolution").trim())) {
				return;
			}

			if (null == headerObj.getString("net_type") || "".equals(headerObj.getString("net_type").trim())) {
				return;
			}

			/**
			 * 生成user_id
			 */
			String user_id = "";
			if ("android".equals(headerObj.getString("os_name").trim())) {
				user_id = StringUtils.isNotBlank(headerObj.getString("android_id")) ? headerObj.getString("android_id")
						: headerObj.getString("device_id");
			} else {
				user_id = headerObj.getString("device_id");
			}
//			将生成的userid加到json对象中
			headerObj.put("user_id", user_id);
//			使用自定义的工具类将json数据转换成指定格式的字符串 方便后续导入hive处理
			k.set(JsonToStringUtil.toString(headerObj));
//			判断用户使用的终端操作系统 通过MutipleOutputs将不同系统的数据输出到不同文件夹中
			if("android".equals(headerObj.getString("os_name"))) {
				mos.write(k, v, "android/android");
			}else {
				mos.write(k, v, "ios/ios");
			}
			
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
//			关闭MultipleOutputs
			mos.close();
		}
	}
	
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(AppLogDataClean.class);
		job.setMapperClass(AppLogDataCleanMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
//		当没有数据输出时避免生成空文件 因为已经通过MultipleOutputs输出
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
//		文件输入输出路径在运行时通过参数传入
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
