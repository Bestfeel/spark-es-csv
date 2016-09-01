# spark-es-csv



##  spark  读取hive/HDFS  数据转换为json 或者csv, 可以写入第三方中间件如ES 


* spark->json
* spark->csv
* spark->es

### 使用方式案列:

```
#!/bin/bash

tt=$(date -d yesterday "+%Y-%m-%d")

year=$(date -d  $tt "+%Y")
month=$(date -d $tt "+=%m"|gawk '{print gensub("=0","","g",$0)}'|gawk '{print gensub("=","","g",$0)}')
day=$(date -d $tt "+=%d"|gawk '{print gensub("=0","","g",$0)}'|gawk '{print gensub("=","","g",$0)}')


IFS="|"

jobClass="com.giziwts.es.SparkEsWrite"

types=(
test
)

declare -A importjobparam=(
[dailyreport]="${tt}|csv|hdfs:////db/table/year=${year}/month=${month}/day=${day}/||||||gizwits_dailyreport/dailyreport|report:9200|json"
)

jobpathlength=${#types[@]}

for (( i=0; i<$jobpathlength; i++)); do

type=${types[${i}]}

importparam=${importjobparam[${type}]}

${SPARK_HOME}/bin/spark-submit  --class  ${jobClass}    --master local[4]   --driver-memory 2g --executor-memory 3g  --num-executors 1  --executor-cores 2    ${sparkesjar}    ${importparam}    2>>${logdir}/${logdate}_${jobClass}.err>>${logdir}/${logdate}_${jobClass}.log

done

```