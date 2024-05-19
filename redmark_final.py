from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead
from pyspark.sql.window import Window
from timeit import default_timer as timer
import shutil
import os

start = timer()

# 目标目录
output_dir = "redmark13"

# 删除目标目录中的所有文件
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

# 创建 SparkSession 对象，设置为本地模式
spark = SparkSession.builder \
    .appName("SuperRedMark") \
    .master("local[*]") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "12g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()

# 读取基站信息文件
cdinfo_df = spark.read.option("header", "false").option("inferSchema", "true").csv("cdinfo.txt")
cdinfo_df = cdinfo_df.withColumnRenamed("_c0", "station_id")\
                     .withColumnRenamed("_c1", "time")\
                     .withColumnRenamed("_c2", "event")\
                     .withColumnRenamed("_c3", "phone")

# 将time列转换为字符串类型
cdinfo_df = cdinfo_df.withColumn("time", col("time").cast("string"))

# 读取感染者信息文件
infected_df = spark.read.option("header", "false").option("inferSchema", "true").csv("infected.txt")
infected_df = infected_df.withColumnRenamed("_c0", "infected_phone")

# 过滤出感染者的基站进出信息
infected_cdinfo_df = cdinfo_df.join(infected_df, cdinfo_df.phone == infected_df.infected_phone)

# 窗口函数按基站和手机分组并排序
window_spec = Window.partitionBy("phone", "station_id").orderBy("time")

# 使用窗口函数计算进出基站的时间
infected_cdinfo_df = infected_cdinfo_df.withColumn("next_event", lead("event").over(window_spec))\
                                       .withColumn("next_time", lead("time").over(window_spec))

# 计算感染者在每个基站的停留时间
infected_cdinfo_df = infected_cdinfo_df.withColumn("infected_start_time", col("time"))\
                                       .withColumn("infected_end_time", col("next_time"))\
                                       .filter((col("event") == 1) & (col("next_event") == 2))

# 提取感染者的停留区间
infected_intervals_df = infected_cdinfo_df.select("station_id", "infected_phone", "infected_start_time", "infected_end_time")

# 窗口函数按基站和手机分组并排序（对所有人的基站进出记录）
all_window_spec = Window.partitionBy("phone", "station_id").orderBy("time")

# 计算所有人在每个基站的进出时间
all_cdinfo_df = cdinfo_df.withColumn("next_event", lead("event").over(all_window_spec))\
                         .withColumn("next_time", lead("time").over(all_window_spec))

# 计算所有人在每个基站的停留时间
all_cdinfo_df = all_cdinfo_df.withColumn("start_time", col("time"))\
                             .withColumn("end_time", col("next_time"))\
                             .filter((col("event") == 1) & (col("next_event") == 2))

# 将所有基站进出信息与感染者的停留区间进行比较，找出在相同基站和时间区间有重叠的手机号码
redmark_df = all_cdinfo_df.join(infected_intervals_df, "station_id")\
                          .filter((col("start_time") <= col("infected_end_time")) &
                                  (col("end_time") >= col("infected_start_time")) &
                                  (col("phone") != col("infected_phone")))\
                          .select(col("phone").cast("string")).distinct()

# 创建一个 DataFrame，仅包含感染者手机号
infected_phone_df = infected_intervals_df.select(col("infected_phone").cast("string"))

# 将感染者手机号加入到最终结果中
redmark_df_filtered = redmark_df.union(infected_phone_df).distinct()

# 将phone列转换为数值类型进行排序
redmark_df_sorted = redmark_df_filtered.withColumn("phone_num", col("phone").cast("long")).orderBy("phone_num")

# 转换回字符串类型并保存结果到文件
redmark_df_sorted = redmark_df_sorted.select(col("phone_num").cast("string").alias("phone")).coalesce(1)

# 计算最终的手机号码数量
phone_count = redmark_df_sorted.count()
print(f"最终手机号码数量: {phone_count}")

# 保存结果到单个文件
redmark_df_sorted.write.mode("overwrite").text("redmark13")

print("共运行时间" + f"{timer() - start}")

# 停止SparkSession
spark.stop()
