# CC_miniproject3
## part 1
We edit the cassandra.yaml on each node. The screenshot below is an example of master's configuration.
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.38.10.png)
![](https://github.com/chunchuliu/CC_miniproject3/raw/master/image.png)
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.39.44.png)
We start the services on all the nodes, this is the status of our nodes. All three nodes are up.
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.41.02.png)
This is the result of test CQL using Cassandra.
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.43.11.png)

## part 2
```
CREATE KEYSPACE project3 WITH replication={'class':'SimpleStrategy', 'replication_factor':3};
CREATE TABLE project3.part2(ip text, c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, primary key(ip, c1, c2, c3));
COPY project3.part2 FROM 'access_log' with delimiter=' ' and HEADER = TRUE;
```
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.43.48.png)

```
cd spark
bin/spark-shell --master yarn --deploy-mode client
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.TimestampType

val logData=sc.textFile("access_log")
val rdd = logData.map(x=>x.split(" ")).map(x=>(x(0),x(3).stripPrefix("["),x(6)))
val df = rdd.toDF("IP", "Time","url")
df.show()
df.repartition(1).write.format("com.databricks.spark.csv").save("urldata.csv")

cd /usr/local/hadoop
hadoop fs -copyToLocal urldata.csv /home/student
mv urldata.csv access.csv
```

```
CREATE TABLE project3.part3(ip text, time text, url text, primary key(ip, time));
COPY project3.part3 FROM 'access.csv' with HEADER = TRUE;
```
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.44.01.png)

## part 3
### part 3-1
```
select count(*) from project3.part3 where url='/assets/img/release-schedule-logo.png' allow filtering;
```

![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.44.09.png)
### part3-2
```
select count() from project3.part2 where ip='10.207.188.188';
```
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2016.44.17.png)

### part3-3
```
sudo vim /etc/cassandra/cassandra.yaml
enable_user_defined_functions=true
read_request_timeout_in_ms: 3600000
range_request_timeout_in_ms: 3600000
write_request_timeout_in_ms: 3600000
counter_write_request_timeout_in_ms: 3600000
cas_contention_timeout_in_ms: 3600000
truncate_request_timeout_in_ms: 3600000
request_timeout_in_ms: 3600000
slow_query_log_timeout_in_ms: 3600000
sudo vim /etc/bin/cqlsh.py
DEFAULT_REQUEST_TIMEOUT_SECONDS=3600
```
```
use project3;
CREATE OR REPLACE FUNCTION state_group_and_count( state map<text, int>, type text )
CALLED ON NULL INPUT
RETURNS map<text, int>
LANGUAGE java AS '
Integer count = (Integer) state.get(type);
if (count == null) count = 1;
else count++;
state.put(type, count);
return state;';
```
```
CREATE OR REPLACE FUNCTION final_group_and_count( state map<text, int>)
CALLED ON NULL INPUT
RETURNS text
LANGUAGE java AS '
String maxKey = null;
int max_count=0;
for(String key: state.keySet()){
int value= state.get(key);
if(value > max_count){maxKey=key; max_count=value;}}
return maxKey;';
```
```
CREATE OR REPLACE AGGREGATE max_group_and_count(text)
SFUNC state_group_and_count
STYPE map<text, int>
FINALFUNC final_group_and_count
INITCOND {};
```
```
CREATE OR REPLACE FUNCTION final_value_group_and_count( state map<text, int>)
CALLED ON NULL INPUT
RETURNS int
LANGUAGE java AS '
String maxKey = null;
int max_count=0;
for(String key: state.keySet()){
int value= state.get(key);
if(value > max_count){maxKey=key; max_count=value;}}
return max_count;';
```
```
CREATE OR REPLACE AGGREGATE max_value_group_and_count(text)
SFUNC state_group_and_count
STYPE map<text, int>
FINALFUNC final_value_group_and_count
INITCOND {};
```
```
select max_value_group_and_count(url) from part3;
```
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2017.05.01.png)
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2017.06.00.png)
### part3-4
```
select max_group_and_count(ip) from part3;
select max_value_group_and_count(ip) from part3;
```
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2017.06.09.png)
![](https://github.com/chunchuliu/CC_miniproject3/blob/master/%E6%88%AA%E5%B1%8F2020-04-15%2017.06.16.png)
