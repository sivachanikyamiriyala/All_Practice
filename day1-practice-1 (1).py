#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys

os.environ["SPARK_HOME"] = "/usr/spark2.4.3"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# In below two lines, use /usr/bin/python2.7 if you want to use Python 2
os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda/bin/python" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/anaconda/bin/python"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[2]:


from pyspark.sql import SparkSession


# In[3]:


import getpass


# In[4]:


appname=getpass.getuser()


# In[5]:


appname


# In[6]:


#creation of sparkdriver process 
sparkdriver=SparkSession.builder.master('local').appName(appname+'application').enableHiveSupport().getOrCreate()


# In[7]:


sparkdriver


# In[8]:


sparkdriver.sparkContext.uiWebUrl


# In[9]:


sparkdriver.sparkContext.defaultParallelism


# In[10]:


database_name="miriyala"


# In[11]:


sparkdriver.catalog.setCurrentDatabase(database_name)


# In[12]:


df_hive=sparkdriver.sql("select * from sales")


# In[13]:


df1=df_hive.alias("df1")


# In[14]:


df1.show()


# In[15]:


df1.printSchema()


# In[16]:


df1.createOrReplaceTempView("sales")


# In[17]:


sparkdriver.sql("select * from sales").show()


# In[18]:


df1.show()


# In[20]:


sparkdriver.sql("select name,year from sales ").show(truncate=0)


# In[21]:


df1.select("name","year").show(truncate=0)


# In[22]:


sparkdriver.sql("select name,year from sales where year>2011 and year <2013").show(truncate=0)


# In[23]:


df1.select("name","year").where("year>2011 and year<2013").show(truncate=0)


# In[24]:


df1.select("name","year").filter("year>2011 and year<2013").show()


# In[25]:


sparkdriver.sql("""select name,count(*) from sales group by name""").show()


# In[26]:


df1.groupBy("name").count().show()


# In[27]:


from pyspark.sql import functions


# In[28]:


from pyspark.sql.functions import *


# In[29]:


df1.groupBy("name").agg(count("name").alias("cnt")).show()


# In[30]:


sparkdriver.sql("""select name,min(year),max(year),count(*) from sales group by name""").show()


# In[31]:


df1.groupBy("name").agg(count("name"),min("year"),max("year")).show()


# In[35]:


sparkdriver.sql("""select substr(name,1,1) as ch,count(*) from sales group by ch""").show()


# In[33]:


df1.show()


# In[34]:


df1.groupBy(substring("name",1,1)).count().show()


# In[ ]:





# In[37]:


df1.groupBy(col("name").substr(1,1)).count().show()


# In[38]:


df1.orderBy("year").show()


# In[39]:


df1.orderBy(desc("year")).show()


# In[40]:


df=df1.alias("df")


# In[41]:


df.count()


# In[42]:


df.printSchema()


# In[43]:


df2=sparkdriver.sql("select * from products")


# In[44]:


df2.createOrReplaceTempView("products")


# In[45]:


cond=[df1.name==df2.name]


# In[46]:


df1.join(df2,cond,'inner').show()


# In[47]:


df1.join(df2,"name",'inner').show()


# In[50]:


df1.join(df2,df1.name==df2.name,'inner').show()


# In[51]:


df1.join(df2,df1.name==df2.name,'left_outer').show()


# In[52]:


df1.join(df2,df1.name==df2.name,'right_outer').show()


# In[53]:


df1.join(df2,df1.name==df2.name,'full_outer').show()


# In[54]:


df1.join(df2,df1.name==df2.name,'left_semi').show()


# In[55]:


df1.join(df2,df1.name==df2.name,'left_anti').show()


# In[56]:


df1.join(df2.hint('broadcast'),df1.name==df2.name,'inner').show()


# In[57]:


df1.join(broadcast(df2),df1.name==df2.name,'inner').show()


# In[58]:


sparkdriver.sql("""select s.*,p.* from sales s join products p on s.name=p.name""").show()


# In[59]:


sparkdriver.sql("""select s.*,p.* from sales s left outer join products p on s.name=p.name""").show()


# In[60]:


sparkdriver.sql("""select s.*,p.* from sales s right outer join products p on s.name=p.name""").show()


# In[61]:


sparkdriver.sql("""select s.*,p.* from sales s full outer join products p on s.name=p.name""").show()


# In[62]:


sparkdriver.sql("""select s.* from sales s left semi join products p on s.name=p.name""").show()


# In[63]:


sparkdriver.sql("""select /*+mapjoin(p) */ s.*,p.* from sales s left outer join products p on s.name=p.name""").show()


# In[64]:


sparkdriver.sql("""select /*+mapjoin(s) */ s.*,p.* from sales s right outer join products p on s.name=p.name""").show()


# In[67]:


df1.collect()


# # 

# In[68]:


df1.show()


# In[69]:


df2.show()


# In[70]:


df1.join(broadcast(df2),"name",'inner').show()


# In[71]:


from pyspark.storagelevel import *


# In[72]:


df1.persist(StorageLevel.MEMORY_AND_DISK_SER_2)


# In[73]:


df2.persist(StorageLevel.MEMORY_AND_DISK_SER_2)


# In[74]:


df1.printSchema()


# In[75]:


df1.columns


# In[76]:


def f1(x):
    return x*x


# In[77]:


from pyspark.sql.types import *


# In[78]:


fun1=udf(f1,LongType())


# In[80]:


df1.select("year",fun1("year")).show()


# In[82]:


sparkdriver.udf.register("func2",f1,LongType())


# In[83]:


sparkdriver.sql("select year,func2(year) from sales").show()


# In[84]:


df1.withColumn("new",lit(100)).show()


# In[85]:


df1.withColumn("new",current_date()).show()


# In[86]:


df1.withColumn("new",date_add(current_date(),1)).show()


# In[88]:


df1.withColumn("newstatus",when(col("name")=='hadoop','ha').when(col("name")=='hive','hi').otherwise(col("name"))).show()


# In[89]:


df1.withColumn("newstatus",when(col("name")=='hadoop','ha').when(col("name")=='hive','hi').otherwise(lit(100))).show()


# In[90]:


df1.selectExpr("year*100 as myannualyear").show()


# In[91]:


df1.withColumnRenamed("name","myname").show()


# In[92]:


df1.select("name").distinct().show()


# In[93]:


df1.dropDuplicates(['name']).show()


# In[96]:


df1.unionAll(df2).show()


# In[ ]:





# In[97]:


df1.drop("name").show()


# In[ ]:





# In[98]:


rdd1=sparkdriver.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,10],2)


# In[99]:


rdd1.getNumPartitions()


# In[100]:


rdd1.glom().collect()


# In[101]:


rdd1.getNumPartitions()


# In[102]:


rdd1.glom().collect()


# In[103]:


rdd1.repartition(3).glom().collect()


# In[104]:


rdd1.coalesce(3,True).glom().collect()


# In[114]:


input_path='file:///home/sivachanikyamiriyala3826/siva/wordcount'


# In[115]:


rdd1=sparkdriver.sparkContext.textFile(input_path)


# In[ ]:





# In[ ]:





# In[116]:


rdd1.collect()


# In[120]:


rdd1.map(lambda x:x.split(" ")).map(lambda x:(x,1)).collect()


# In[121]:


rdd1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).collect()


# In[ ]:





# In[122]:


r1=sparkdriver.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,10],2)


# In[123]:


r1.aggregate(0,lambda x,y:x+y,lambda x,y:x+y)


# In[124]:


r1.aggregate(2,lambda x,y:x+y,lambda x,y:x+y)


# In[125]:


r1.fold(0,lambda x,y:x+y)


# In[126]:


r1.fold(2,lambda x,y:x+y)


# In[127]:


r1.reduce(lambda x,y:x+y)


# In[128]:


l1=[('a',5),('b',2),('c',2),('a',1),('b',1),('a',2),('r',3),('t',1),('c',1)]


# In[129]:


l2=[('a',5),('b',2),('a',2),('b',1),('q',1),('a',2)]


# In[130]:


r1=sparkdriver.sparkContext.parallelize(l1,3)


# In[131]:


r1.glom().collect()


# In[132]:


r2=sparkdriver.sparkContext.parallelize(l2,2)


# In[133]:


r2.glom().collect()


# In[134]:


r1.aggregateByKey(0,lambda x,y:x+y,lambda x,y:x+y).collect()


# In[135]:


r1.aggregateByKey(2,lambda x,y:x+y,lambda x,y:x+y).collect()


# In[137]:


r1.foldByKey(2,lambda x,y:x+y).collect()


# # 

# In[138]:


r1.reduceByKey(lambda x,y:x+y).collect()


# In[139]:


r1.glom().collect()


# In[140]:


r1.join(r2).collect()


# In[141]:


r1.collect()


# In[142]:


r2.collect()


# In[143]:


r1.leftOuterJoin(r2).collect()


# In[144]:


r1.rightOuterJoin(r2).collect()


# In[145]:


r1.fullOuterJoin(r2).collect()


# In[146]:


rdd=sparkdriver.sparkContext.textFile('file:///home/sivachanikyamiriyala3826/siva/wordcount',2)


# In[148]:


rdd.glom().collect()


# In[149]:


rdd2=rdd.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)


# In[150]:


rdd2.glom().collect()


# In[151]:


rdd2.collect()


# In[153]:


rdd2.sortBy(lambda x:x,False,3).glom().collect()


# In[154]:


rdd2.saveAsTextFile('file:///home/sivachanikyamiriyala3826/wc_op')


# In[155]:


get_ipython().system(' ls -ltr ')


# In[156]:


get_ipython().system(' ls ')


# In[1]:


df1.show()


# In[2]:


import os
import sys

os.environ["SPARK_HOME"] = "/usr/spark2.4.3"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# In below two lines, use /usr/bin/python2.7 if you want to use Python 2
os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda/bin/python" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/anaconda/bin/python"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[3]:


from pyspark.sql import SparkSession


# In[4]:


sparkdriver=SparkSession.builder.master('local').appName('sampleapp').config('spark.jars.packages','mysql:mysql-connector-java:5.1.42').getOrCreate()


# In[5]:


sparkdriver


# In[8]:


df_mysql=sparkdriver.read.format('jdbc').option('url','jdbc:mysql://cxln2.c.thelab-240901.internal:3306').                                         option('driver','com.mysql.jdbc.Driver').                                         option('user','sqoopuser').                                         option('password','NHkkP876rp').                                         option('dbtable','sqoopex.sales').load()


# In[9]:


df_mysql.show()


# In[10]:


rdd=sparkdriver.sparkContext.textFile('file:///home/sivachanikyamiriyala3826/calllog.txt')


# In[11]:


rdd.count()


# In[12]:


rdd.take(5)


# In[13]:


rdd_s=rdd.filter(lambda x:'SUCCESS' in x)


# In[14]:


rdd_s.count()


# In[15]:


rdd_s.take(5)


# In[16]:


rdd_d=rdd.filter(lambda x:'DROPPED' in x)


# In[17]:


rdd_d.take(5)


# In[18]:


rdd_d.count()


# In[19]:


rdd_f=rdd.filter(lambda x:'FAILED' in x)


# In[20]:


rdd_f.count()


# In[21]:


rdd_f.take(5)


# In[22]:


rdd.count()


# In[23]:


rdd_s.count()


# In[24]:


rdd_d.count()


# In[25]:


rdd_f.count()


# In[26]:


rdd_s.union(rdd_d).count()


# In[30]:


rdd_s.cartesian(rdd_d).getNumPartitions()


# In[31]:


rdd.count()


# In[32]:


rdd.count()


# In[34]:


rdd.countApprox(5)


# In[35]:


rdd.min()


# In[36]:


rdd.max()


# In[38]:


rdd1=rdd


# In[39]:


rdd.id


# In[40]:


rdd1.id


# In[41]:


rdd.top(4)


# In[42]:


rdd.take(4)


# In[51]:


r1=sparkdriver.sparkContext.range(1,11,1,2)


# In[52]:


r1.getNumPartitions()


# In[53]:


r1.glom().collect()


# In[ ]:





# In[55]:


r1.min()


# # 

# In[56]:


r1.max()


# In[57]:


r1.sum()


# In[58]:


r1.mean()


# In[59]:


r1.stdev()


# In[60]:


r1.stats()


# In[61]:


r1.top(5)


# In[62]:


r1.take(5)


# In[63]:


r1.takeOrdered(5)


# In[65]:


r1=sparkdriver.sparkContext.wholeTextFiles('file:///home/sivachanikyamiriyala3826/siva/wordcount')


# In[66]:


r1.collect()


# In[67]:


rdd=r1.map(lambda x:x[1])


# In[68]:


rdd.collect()


# In[77]:


rdd2=rdd.map(lambda x:x.split("\n"))


# In[78]:


rdd2.collect()


# In[79]:


rdd3=rdd2.flatMap(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)


# In[80]:


rdd3.collect()


# In[ ]:





# In[82]:


df_mysql.show()


# In[83]:


df=df_mysql.alias("df")


# In[84]:


df.createOrReplaceTempView("sales")


# In[85]:


sparkdriver.catalog.listTables()


# In[86]:


sparkdriver.catalog.dropTempView("sales")


# In[ ]:





# In[87]:


sparkdriver.catalog.listTables()


# In[88]:


df.createOrReplaceTempView("sales")


# In[89]:


sparkdriver.catalog.listTables()


# In[90]:


sparkdriver.sql("select * from sales").show()


# In[91]:


df.cube("transaction_id").count().show()


# In[94]:


df.cube("transaction_id").count().na.fill(500).show()


# In[95]:


df.cube("transaction_id").count().na.fill("hi").show()


# In[97]:


df.cube("transaction_id").count().na.replace(7,77,'transaction_id').show()


# In[99]:


sparkdriver.sql("""select transaction_id,count(*) from sales group by cube(transaction_id)""").show()


# In[100]:


df.show()


# In[101]:


sparkdriver.sql("""select transaction_id,amount,count(*) from sales group by cube(transaction_id,amount)""").show()


# In[102]:


df.show()


# In[103]:


sparkdriver.sql("select *,case when amount=100.0 then 10 when amount=150.0 then 15 else amount end as status from sales").show()


# In[104]:


sparkdriver.stop()


# In[105]:


sparkdriver=SparkSession.builder.master('local').appName('sampleapp').enableHiveSupport().getOrCreate()


# In[ ]:





# In[106]:


sparkdriver


# In[107]:


sparkdriver.sparkContext.uiWebUrl


# In[111]:


df=sparkdriver.sql("select * from miriyala.emp")


# In[112]:


df.count()


# In[113]:


df.show()


# In[115]:


df=df.filter("empno is not null")


# In[116]:


df.count()


# In[117]:


df.show()


# In[118]:


from pyspark.sql import functions 


# In[119]:


from pyspark.sql.functions import *


# In[120]:


from pyspark.sql.types import *


# In[122]:


from pyspark.sql.window import Window


# In[ ]:





# In[123]:


win=Window.partitionBy("job").orderBy(desc("sal"))


# In[126]:


df.withColumn("dense_rank",rank().over(win)).show()


# In[ ]:





# In[127]:


df.show()


# In[128]:


sparkdriver.catalog.listDatabases()


# In[130]:


df.show()


# In[131]:


df.createOrReplaceTempView("emp")


# In[133]:


sparkdriver.sql("select sal,comm,sal+nvl(comm,0) as annualsal from emp ").show()


# In[134]:


df.createOrReplaceGlobalTempView("s")


# In[135]:


sparkdriver.sql("select * from global_temp.s").show()


# In[136]:


spark1=sparkdriver.newSession()


# In[139]:


spark1.sql("select * from global_temp.s where comm is not null").show()


# In[142]:


df.where("comm is not null").show()


# In[ ]:




