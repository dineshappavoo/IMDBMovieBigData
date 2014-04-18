pig -x mapreduce Q1.pig



pig -x mapreduce Q2.pig



pig -x mapreduce Q3.pig



javac -classpath pig-0.10.1.jar -d . FORMAT_GENRE.java

jar -cvf pig_udf.jar -C . . 

pig -x mapreduce Q4.pig



hive -f Q5.hive



hive -f Q6.hive



hive -f Q7.hive



hive -f Q8.hive



javac -classpath hive-exec-0.9.0.jar:/usr/local/hadoop-1.0.4/hadoop-core-1.2.1.jar -d . Genre_Formatting.java

jar -cvf Q9.jar -C . .

hive -f Q9.hive





