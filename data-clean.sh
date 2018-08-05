#!/bin/bash
day_str=`date +'%y-%m-%d'`

inpath=/access_log/data/$day_str
outpath=/access_log/clean/$day_str

echo "准备清洗$day_str 的数据......"

hadoop jar /home/hadoop/dataclean.jar com.gyq.mr.AppLogDataClean $inpath $outpath
