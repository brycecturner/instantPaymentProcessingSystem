#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from delta import *
from delta.tables import *


# In[2]:


#    .config("spark.jar.packages", "/home/bcturner/instantPaymentProcessingSystem/main/classpath/delta-core_2.12-2.3.0.jar,delta-core_2.12-2.3.0.jar/delta-storage-2.3.0.jar")

spark = SparkSession.builder.appName('myapp')\
    .config("spark.jar.packages", "io.delta:delta-core_2.12:2.3.0,io.delta:delta-storage:2.3.0")\
    .config("spark.sql.extension", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .enableHiveSupport()\
    .getOrCreate()


# In[3]:


data = spark.read.format('csv').option('header', 'true').load("/home/bcturner/instantPaymentProcessingSystem/main/data/test_data.csv")


# In[4]:


data.show()


# In[5]:


data.write.format('delta').save("/home/bcturner/instantPaymentProcessingSystem/main/data/test_data")


# In[ ]:




