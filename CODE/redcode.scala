package red_code


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object redcode {


    def main(args: Array[String]): Unit = {
      // 获取开始时间
      val startTime = System.currentTimeMillis()
      //建立Spark连接
      val spark = SparkSession.builder().appName("redcode").master("local[*]").getOrCreate()
      val sc = spark.sparkContext
      // 将txt文件按照csv格式读入
      val cdinfo = spark.read.csv("cdinfo(1)/cdinfo.txtjion")

      val infected = spark.read.csv("infected2(1).txt")
      val infected_tlp=infected.select("_c0").rdd.map(row => row(0)).collect.toList.distinct
      println("读取结束")
      println("开始处理")
      // 窗口函数按基站和手机分组并排序（对所有人的基站进出记录）
      val windowSpec = Window.partitionBy("_c3", "_c0").orderBy("_c1")

      // 找出感染的基站
      val filtered = cdinfo.filter(cdinfo("_c3").isin(infected_tlp:_*)).orderBy("_c0","_c3","_c1")
      // 计算感染者在每个基站的停留时间
      val infected_base = filtered.withColumn("infected_base_end", lead("_c1", 1).over(windowSpec)).filter(col("_c2")===1).
        withColumnRenamed("_c1","infected_base_start").drop("_c2","_c3")


      // 计算所有人在每个基站的停留时间
      val may_infected = cdinfo.withColumn("leave_time", lead("_c1", 1).over(windowSpec)).filter(col("_c2")===1).withColumnRenamed("_c1","start_time").drop("_c2")


      //将所有基站进出信息与感染者的停留区间进行比较，找出在相同基站和时间区间有重叠的手机号码
      val final_infected= may_infected.join(infected_base,"_c0")
        .filter((col("start_time") <= col("infected_base_end")) &&
        (col("leave_time") >= col("infected_base_start")))
      println("处理完成")
      // 将结果写入txt文件
      val final_task= final_infected.select(("_c3")).distinct().sort("_c3")
      final_task.coalesce(1).write.mode("overwrite").text("remark13.txt")
      println("写入结束")
      //获取结束时间
     val endTime = System.currentTimeMillis()

      //计算时间差
      val duration = (endTime - startTime) / 1000
      println(s"Execution time: $duration s")
     }
}
