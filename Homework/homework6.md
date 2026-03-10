### Import Hadoop setup function


```python
# Download hadoop_import.py from https://github.com/sharadgupta27/data-engineering/blob/main/Homework/hadoop_import.py
# and keep it in the same directory as this script. Then run this script to set up Hadoop for Windows.
import os
from hadoop_import import setup_hadoop_for_windows
setup_hadoop_for_windows()
```

    winutils.exe already present at C:\hadoop\bin\winutils.exe
    hadoop.dll already present at C:\hadoop\bin\hadoop.dll
    
    HADOOP_HOME = C:\hadoop
    Both winutils.exe and hadoop.dll are in place. Restart kernel if SparkSession was already created.

    True



**Question 1**: Install Spark and PySpark


```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
```


```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```


```python
spark.version
```
  '4.1.1'



Asnwer: **4.1.1**

------------------------------------------------------------

**Question 2**: Yellow November 2025


```python
!wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```


```python
# !ls -lh yellow_tripdata_2025-11.parquet
```


```python
import pandas as pd

df_pd = pd.read_parquet("yellow_tripdata_2025-11.parquet")
df_pd.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>VendorID</th>
      <th>tpep_pickup_datetime</th>
      <th>tpep_dropoff_datetime</th>
      <th>passenger_count</th>
      <th>trip_distance</th>
      <th>RatecodeID</th>
      <th>store_and_fwd_flag</th>
      <th>PULocationID</th>
      <th>DOLocationID</th>
      <th>payment_type</th>
      <th>fare_amount</th>
      <th>extra</th>
      <th>mta_tax</th>
      <th>tip_amount</th>
      <th>tolls_amount</th>
      <th>improvement_surcharge</th>
      <th>total_amount</th>
      <th>congestion_surcharge</th>
      <th>Airport_fee</th>
      <th>cbd_congestion_fee</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7</td>
      <td>2025-11-01 00:13:25</td>
      <td>2025-11-01 00:13:25</td>
      <td>1.0</td>
      <td>1.68</td>
      <td>1.0</td>
      <td>N</td>
      <td>43</td>
      <td>186</td>
      <td>1</td>
      <td>14.9</td>
      <td>0.00</td>
      <td>0.5</td>
      <td>1.50</td>
      <td>0.00</td>
      <td>1.0</td>
      <td>22.15</td>
      <td>2.5</td>
      <td>0.00</td>
      <td>0.75</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2025-11-01 00:49:07</td>
      <td>2025-11-01 01:01:22</td>
      <td>1.0</td>
      <td>2.28</td>
      <td>1.0</td>
      <td>N</td>
      <td>142</td>
      <td>237</td>
      <td>1</td>
      <td>14.2</td>
      <td>1.00</td>
      <td>0.5</td>
      <td>4.99</td>
      <td>0.00</td>
      <td>1.0</td>
      <td>24.94</td>
      <td>2.5</td>
      <td>0.00</td>
      <td>0.75</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>2025-11-01 00:07:19</td>
      <td>2025-11-01 00:20:41</td>
      <td>0.0</td>
      <td>2.70</td>
      <td>1.0</td>
      <td>N</td>
      <td>163</td>
      <td>238</td>
      <td>1</td>
      <td>15.6</td>
      <td>4.25</td>
      <td>0.5</td>
      <td>4.27</td>
      <td>0.00</td>
      <td>1.0</td>
      <td>25.62</td>
      <td>2.5</td>
      <td>0.00</td>
      <td>0.75</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>2025-11-01 00:00:00</td>
      <td>2025-11-01 01:01:03</td>
      <td>3.0</td>
      <td>12.87</td>
      <td>1.0</td>
      <td>N</td>
      <td>138</td>
      <td>261</td>
      <td>1</td>
      <td>66.7</td>
      <td>6.00</td>
      <td>0.5</td>
      <td>0.00</td>
      <td>6.94</td>
      <td>1.0</td>
      <td>86.14</td>
      <td>2.5</td>
      <td>1.75</td>
      <td>0.75</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>2025-11-01 00:18:50</td>
      <td>2025-11-01 00:49:32</td>
      <td>0.0</td>
      <td>8.40</td>
      <td>1.0</td>
      <td>N</td>
      <td>138</td>
      <td>37</td>
      <td>2</td>
      <td>39.4</td>
      <td>7.75</td>
      <td>0.5</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>1.0</td>
      <td>48.65</td>
      <td>0.0</td>
      <td>1.75</td>
      <td>0.00</td>
    </tr>
  </tbody>
</table>
</div>




```python
schema = types.StructType([
    types.StructField('tpep_pickup_datetime', types.TimestampType(), True),
    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('passenger_count', types.IntegerType(), True),
    types.StructField('trip_distance', types.FloatType(), True),
    types.StructField('fare_amount', types.FloatType(), True),
])
```


```python
df = spark.read \
    .parquet('yellow_tripdata_2025-11.parquet', schema=schema)
    
df.show()
```

    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
    |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
    |       7| 2025-11-01 00:13:25|  2025-11-01 00:13:25|              1|         1.68|         1|                 N|          43|         186|           1|       14.9|  0.0|    0.5|       1.5|         0.0|                  1.0|       22.15|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:49:07|  2025-11-01 01:01:22|              1|         2.28|         1|                 N|         142|         237|           1|       14.2|  1.0|    0.5|      4.99|         0.0|                  1.0|       24.94|                 2.5|        0.0|              0.75|
    |       1| 2025-11-01 00:07:19|  2025-11-01 00:20:41|              0|          2.7|         1|                 N|         163|         238|           1|       15.6| 4.25|    0.5|      4.27|         0.0|                  1.0|       25.62|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:00:00|  2025-11-01 01:01:03|              3|        12.87|         1|                 N|         138|         261|           1|       66.7|  6.0|    0.5|       0.0|        6.94|                  1.0|       86.14|                 2.5|       1.75|              0.75|
    |       1| 2025-11-01 00:18:50|  2025-11-01 00:49:32|              0|          8.4|         1|                 N|         138|          37|           2|       39.4| 7.75|    0.5|       0.0|         0.0|                  1.0|       48.65|                 0.0|       1.75|               0.0|
    |       2| 2025-11-01 00:21:11|  2025-11-01 00:31:39|              1|         0.85|         1|                 N|          90|         100|           2|       10.7|  1.0|    0.5|       0.0|         0.0|                  1.0|       16.45|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:07:31|  2025-11-01 00:25:44|              1|         3.01|         1|                 N|         142|         170|           1|       19.1|  1.0|    0.5|       1.0|         0.0|                  1.0|       25.85|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:46:52|  2025-11-01 01:38:55|              3|         3.82|         1|                 N|         237|         144|           1|       42.2|  1.0|    0.5|      9.59|         0.0|                  1.0|       57.54|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:56:59|  2025-11-01 01:02:05|              1|         0.89|         1|                 N|         162|         161|           2|        7.2|  1.0|    0.5|       0.0|         0.0|                  1.0|       12.95|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:10:43|  2025-11-01 00:39:25|              3|         2.28|         1|                 N|         234|         162|           1|       24.0|  1.0|    0.5|      8.93|         0.0|                  1.0|       38.68|                 2.5|        0.0|              0.75|
    |       1| 2025-11-01 00:00:03|  2025-11-01 00:42:25|              2|          3.3|         1|                 N|         158|          88|           1|       35.9| 4.25|    0.5|      2.35|         0.0|                  1.0|        44.0|                 2.5|        0.0|              0.75|
    |       1| 2025-11-01 00:43:53|  2025-11-01 00:56:46|              2|          1.5|         1|                 N|          88|         148|           1|       12.8| 4.25|    0.5|       1.0|         0.0|                  1.0|       19.55|                 2.5|        0.0|              0.75|
    |       1| 2025-11-01 00:58:02|  2025-11-01 01:32:36|              2|          4.7|         1|                 N|         148|         236|           1|       32.4| 4.25|    0.5|       9.5|         0.0|                  1.0|       47.65|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:52:48|  2025-11-01 01:23:18|              1|         5.61|         1|                 N|          87|         255|           1|       33.1|  1.0|    0.5|       0.0|         0.0|                  1.0|       38.85|                 2.5|        0.0|              0.75|
    |       1| 2025-11-01 00:05:53|  2025-11-01 00:58:03|              1|          3.9|         1|                 N|         231|          43|           1|       40.8| 4.25|    0.5|       0.0|         0.0|                  1.0|       46.55|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:13:44|  2025-11-01 00:19:30|              2|         1.14|         1|                 N|         141|         262|           1|        7.9|  1.0|    0.5|       2.0|         0.0|                  1.0|        14.9|                 2.5|        0.0|               0.0|
    |       1| 2025-11-01 00:03:18|  2025-11-01 00:06:48|              2|          0.6|         1|                 N|         238|          24|           1|        5.1|  1.0|    0.5|      1.52|         0.0|                  1.0|        9.12|                 0.0|        0.0|               0.0|
    |       1| 2025-11-01 00:19:55|  2025-11-01 00:45:37|              1|          4.3|         1|                 N|         236|         147|           1|       24.7|  1.0|    0.5|       2.0|         0.0|                  1.0|        29.2|                 0.0|        0.0|               0.0|
    |       1| 2025-11-01 00:45:55|  2025-11-01 01:11:30|              1|          3.0|         1|                 N|         231|         137|           1|       24.0| 4.25|    0.5|       3.0|         0.0|                  1.0|       32.75|                 2.5|        0.0|              0.75|
    |       2| 2025-11-01 00:11:12|  2025-11-01 00:15:02|              1|         0.69|         1|                 N|         237|         237|           2|        6.5|  1.0|    0.5|       0.0|         0.0|                  1.0|        11.5|                 2.5|        0.0|               0.0|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
    only showing top 20 rows



```python
df.printSchema()
```

    root
     |-- VendorID: integer (nullable = true)
     |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
     |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
     |-- passenger_count: long (nullable = true)
     |-- trip_distance: double (nullable = true)
     |-- RatecodeID: long (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- PULocationID: integer (nullable = true)
     |-- DOLocationID: integer (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- total_amount: double (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- Airport_fee: double (nullable = true)
     |-- cbd_congestion_fee: double (nullable = true)
    



```python
df = df.repartition(4)

df.write.parquet('data/pq/yellow/2025/11/', mode='overwrite')
```


```python
df = spark.read.parquet('data/pq/yellow/2025/11/')
```


```python
!du -h data/pq/yellow
```

    99M	data/pq/yellow/2025/11
    99M	data/pq/yellow/2025
    99M	data/pq/yellow



```python
!ls -lh data/pq/yellow/2025/11
```

    total 98M
    -rw-r--r-- 1 gupta INTRANET+Group(513)   0 Mar  9 03:01 _SUCCESS
    -rw-r--r-- 1 gupta INTRANET+Group(513) 25M Mar  9 03:01 part-00000-d721aaa4-babc-439f-bd40-ddd04e1a0cfc-c000.snappy.parquet
    -rw-r--r-- 1 gupta INTRANET+Group(513) 25M Mar  9 03:01 part-00001-d721aaa4-babc-439f-bd40-ddd04e1a0cfc-c000.snappy.parquet
    -rw-r--r-- 1 gupta INTRANET+Group(513) 25M Mar  9 03:01 part-00002-d721aaa4-babc-439f-bd40-ddd04e1a0cfc-c000.snappy.parquet
    -rw-r--r-- 1 gupta INTRANET+Group(513) 25M Mar  9 03:01 part-00003-d721aaa4-babc-439f-bd40-ddd04e1a0cfc-c000.snappy.parquet


Answer: ~**100MB**

------------------------------------------------------------------

**Question 3**: How many taxi trips were there on November 15?


```python
from pyspark.sql import functions as F
```


```python
df \
    .withColumn('tpep_pickup_datetime', F.to_date(df.tpep_pickup_datetime)) \
    .filter("tpep_pickup_datetime = '2025-11-15'") \
    .count()
```




    162604




```python
df.createOrReplaceTempView('yellow_2025_11')
```


```python
spark.sql("""
SELECT
    COUNT(1)
FROM 
    yellow_2025_11
WHERE
    to_date(tpep_pickup_datetime) = '2025-11-15';
""").show()
```

    +--------+
    |count(1)|
    +--------+
    |  162604|
    +--------+
    


Answer: **162604**

----------------------------------------------------------------------

**Question 4**: Longest trip for each day


```python
df.columns
```




    ['VendorID',
     'tpep_pickup_datetime',
     'tpep_dropoff_datetime',
     'passenger_count',
     'trip_distance',
     'RatecodeID',
     'store_and_fwd_flag',
     'PULocationID',
     'DOLocationID',
     'payment_type',
     'fare_amount',
     'extra',
     'mta_tax',
     'tip_amount',
     'tolls_amount',
     'improvement_surcharge',
     'total_amount',
     'congestion_surcharge',
     'Airport_fee',
     'cbd_congestion_fee']




```python
df \
    .withColumn('duration_hours', (F.unix_timestamp(df.tpep_dropoff_datetime) - F.unix_timestamp(df.tpep_pickup_datetime))/3600) \
    .withColumn('pickup_date', F.to_date(df.tpep_pickup_datetime)) \
    .groupBy('pickup_date') \
        .max('duration_hours') \
    .orderBy('max(duration_hours)', ascending=False) \
    .limit(10) \
    .show()
```

    +-----------+-------------------+
    |pickup_date|max(duration_hours)|
    +-----------+-------------------+
    | 2025-11-26|  90.64666666666666|
    | 2025-11-27|  76.94833333333334|
    | 2025-11-03|  76.21388888888889|
    | 2025-11-07|  69.28861111111111|
    | 2025-11-18|  67.08055555555555|
    | 2025-11-22|  63.36833333333333|
    | 2025-11-01| 56.382222222222225|
    | 2025-11-05| 42.720555555555556|
    | 2025-11-06| 41.614444444444445|
    | 2025-11-24| 38.074444444444445|
    +-----------+-------------------+
    



```python
spark.sql("""
SELECT
    to_date(tpep_pickup_datetime) AS pickup_date,
    MAX((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600) AS duration_hours
FROM 
    yellow_2025_11
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 10;
""").show()
```

    +-----------+------------------+
    |pickup_date|    duration_hours|
    +-----------+------------------+
    | 2025-11-26| 90.64666666666666|
    | 2025-11-27| 76.94833333333334|
    | 2025-11-03| 76.21388888888889|
    | 2025-11-07| 69.28861111111111|
    | 2025-11-18| 67.08055555555555|
    | 2025-11-22| 63.36833333333333|
    | 2025-11-01|56.382222222222225|
    | 2025-11-05|42.720555555555556|
    | 2025-11-06|41.614444444444445|
    | 2025-11-24|38.074444444444445|
    +-----------+------------------+
    


Answer: **90.6 hours**

-------------------------------------------------------------------

**Question 5**: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

Answer: **4040**

-----------------------------------------------------------------------

**Question 6**: Least frequent `pickup location zone`


```python
!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

    --2026-03-09 03:02:59--  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
    Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 2600:9000:21ca:3c00:b:20a5:b140:21, 2600:9000:21ca:4e00:b:20a5:b140:21, 2600:9000:21ca:6a00:b:20a5:b140:21, ...
    Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|2600:9000:21ca:3c00:b:20a5:b140:21|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 12331 (12K) [text/csv]
    Saving to: 'taxi_zone_lookup.csv.1'
    
         0K .......... ..                                         100%  231M=0s
    
    2026-03-09 03:02:59 (231 MB/s) - 'taxi_zone_lookup.csv.1' saved [12331/12331]
    



```python
# Load zone lookup into a Spark temp view

df_zones = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('taxi_zone_lookup.csv')

df_zones.show(5)
df_zones.createOrReplaceTempView('zones')
```

    +----------+-------------+--------------------+------------+
    |LocationID|      Borough|                Zone|service_zone|
    +----------+-------------+--------------------+------------+
    |         1|          EWR|      Newark Airport|         EWR|
    |         2|       Queens|         Jamaica Bay|   Boro Zone|
    |         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
    |         4|    Manhattan|       Alphabet City| Yellow Zone|
    |         5|Staten Island|       Arden Heights|   Boro Zone|
    +----------+-------------+--------------------+------------+
    only showing top 5 rows



```python
spark.sql("""
SELECT
    z.Zone,
    COUNT(1) AS pickup_count
FROM
    yellow_2025_11 t
    JOIN zones z ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY pickup_count ASC, z.Zone ASC
LIMIT 5;
""").show(truncate=False)
```

    +---------------------------------------------+------------+
    |Zone                                         |pickup_count|
    +---------------------------------------------+------------+
    |Arden Heights                                |1           |
    |Eltingville/Annadale/Prince's Bay            |1           |
    |Governor's Island/Ellis Island/Liberty Island|1           |
    |Port Richmond                                |3           |
    |Great Kills                                  |4           |
    +---------------------------------------------+------------+
    


Answer: **Arden Heights**
