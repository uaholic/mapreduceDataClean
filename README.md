# mapreduceDataClean（MR练习）
### 使用mapreduce做数据清洗过滤掉不符合格式的数据。
### 由于需求只是对数据进行过滤，所以没有reducer，所需要的数据在data.7z压缩包里。
#### 这只是大数据中离线数据分析类项目中的一小块需求，整体需求见doc文档，后续将陆续完善项目中的其他部分
如果想在hadoop集群中运行该程序可以将程序打包后放入集群中</br>
data-clean.sh脚本可以用来启动MapReduce 也可以手动使用hadoop jar 命令启动</br>
shell脚本不通用，需要根据不同的集群环境手动修改部分参数</br>
运行程序之前需要先将源数据导入到hdfs中，因为数据量不大，如果只是为了测试mr手动put到hdfs里也可以</br>
也可以使用flume导入，具体配置参考：https://github.com/uaholic/flume-conf
