# Databricks notebook source
# DBTITLE 1,import libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Import transformations directly from utils directory
import sys
sys.path.insert(0, '/Workspace/Users/svarunachala96@outlook.com/spotify_dab/utils')
from transformations import *

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimUser**

# COMMAND ----------

# DBTITLE 1,read the data as a whole batch
df_batch = spark.read.format('parquet').option('pathGlobFilter','*.parquet').load("abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/DimUser")

# display
df_batch.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **AUTOLOADER**

# COMMAND ----------

df_user = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option(
        "cloudFiles.schemaLocation",
        "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimUser/checkpoint/",
    )
    .option("pathGlobFilter", "*.parquet")
    .load("abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/DimUser/")
)

# COMMAND ----------

df_user.display()

# COMMAND ----------

# DBTITLE 1,convert  user names to Upper case
df_user = df_user.withColumn("user_name",upper(col("user_name"))) 

# check
df_user.display()

# COMMAND ----------

# DBTITLE 1,remove redundant columns
from transformations import *
df_user_obj = Reusable()

# remove "_rescued_data" column
df_user = df_user_obj.dropColumns(df_user, ["_rescued_data"])

# check
df_user.display()


# COMMAND ----------

# DBTITLE 1,Deduplication
# Reload the module to pick up the fix
import importlib
import transformations
importlib.reload(transformations)
from transformations import Reusable
df_user_obj = Reusable()

df_user = df_user_obj.deDuplicate(df_user,["user_id"])



# COMMAND ----------

df_user.display()

# COMMAND ----------

# DBTITLE 1,Writing to delta table
table_name = "spotify_catalog.silver.DimUser"
df_user.writeStream.format('delta')\
    .outputMode("append")\
        .option("checkpointLocation","abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimUser/checkpoint/")\
        .trigger(once=True).option(
    "path",
    "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimUser/data/",
)
            .table('spotify_catalog.silver.DimUser')

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimArtist**

# COMMAND ----------

# DBTITLE 1,Read the data using Autoloader
df_artist = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option(
        "cloudFiles.schemaLocation",
        "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimArtist/checkpoint/",
    )
    .option("pathGlobFilter", "*.parquet")
    .load("abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/DimArtist/")
)

# COMMAND ----------

# check
df_artist.display()

# COMMAND ----------

# DBTITLE 1,remove redundant columns
df_artist_obj = Reusable()

df_artist = df_artist_obj.dropColumns(df_artist, ["_rescued_data"])

# check
df_artist.display()

# COMMAND ----------

# DBTITLE 1,Deduplication
df_artist = df_artist_obj.deDuplicate(df_artist,["artist_id"])

# check
df_artist.display()

# COMMAND ----------

# DBTITLE 1,Writing to a delta table
df_artist.writeStream.format("delta").outputMode("append").option(
    "checkpointLocation",
    "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimArtist/checkpoint/",
).trigger(once=True).option(
    "path",
    "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimArtist/data/",
).table(
    "spotify_catalog.silver.DimArtist"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimDate**

# COMMAND ----------

# DBTITLE 1,Read the data using Autoloader
df_date = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option(
        "cloudFiles.schemaLocation",
        "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimDate/checkpoint/",
    )
    .option("pathGlobFilter", "*.parquet")
    .load("abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/DimDate/")
)

# check
df_date.display()

# COMMAND ----------

# DBTITLE 1,remove redundant columns
df_date_obj = Reusable()

df_date = df_date_obj.dropColumns(df_date, ["_rescued_data"])

# check
df_date.display()

# COMMAND ----------

# DBTITLE 1,Deduplication
df_date = df_date_obj.deDuplicate(df_date,["date_key"])

# check
df_date.display()

# COMMAND ----------

# DBTITLE 1,Writing to delta table
df_date.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option(
    "checkpointLocation",
    "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimDate/checkpoint/",
).option(
    "path", "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimDate/data/"
).table(
    "spotify_catalog.silver.DimDate"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimTrack**

# COMMAND ----------

# DBTITLE 1,Read the data using Autoloader
df_track = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option(
        "cloudFiles.schemaLocation",
        "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimTrack/checkpoint/",
    )
    .option("pathGlobFilter", "*.parquet")
    .load("abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/DimTrack/")
)

# check
df_track.display()

# COMMAND ----------

# DBTITLE 1,remove redundant columns
df_track_obj = Reusable()

df_track = df_track_obj.dropColumns(df_track, ["_rescued_data"])

# check
df_track.display()

# COMMAND ----------

# DBTITLE 1,Deduplication
df_track = df_track_obj.deDuplicate(df_track,["track_id"])

# check
df_track.display()

# COMMAND ----------

# DBTITLE 1,Writing to delta table
df_track.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option(
    "checkpointLocation",
    "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimTrack/checkpoint/",
).option(
    "path", "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimTrack/data/"
).table(
    "spotify_catalog.silver.DimTrack")

# COMMAND ----------

df_track = spark.read.table("spotify_catalog.silver.DimTrack")

# check
df_track.display()

# COMMAND ----------

# DBTITLE 1,create a duration flag
df_track = df_track.withColumn("durationFlag", when(col("duration_sec") < 150, "low")\
                                .when((col("duration_sec") >= 150) & (col("duration_sec") < 300), "medium").otherwise("high"))

# check
df_track.display()

# COMMAND ----------

# DBTITLE 1,replace hypens with space in track_name column
df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"),"-"," "))

# check
df_track.select("track_name").display()

# COMMAND ----------

# DBTITLE 1,overwrite the data to the delta table
df_track.write.format("delta").mode("overwrite").option("mergeSchema","true").option(
    "path", "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/DimTrack/data/"
).saveAsTable(
    "spotify_catalog.silver.DimTrack"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check
# MAGIC SELECT *
# MAGIC FROM spotify_catalog.silver.dimtrack;

# COMMAND ----------

# MAGIC %md
# MAGIC # **FactStream**

# COMMAND ----------

# DBTITLE 1,Read the data using Autoloader
df_stream = spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet").option("cloudFiles.schemaLocation","abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/FactStream/checkpoint/").option("pathGlobFilter","*.parquet").load("abfss://bronze@spotifydeprojectstorage.dfs.core.windows.net/FactStream/")

# display
df_stream.display()

# COMMAND ----------

# DBTITLE 1,remove redundant columns
from transformations import Reusable

df_stream_obj = Reusable()

df_stream = df_stream_obj.dropColumns(df_stream,["_rescued_data"])

# check 
df_stream.display()

# COMMAND ----------

# DBTITLE 1,Deduplication
df_stream = df_stream_obj.deDuplicate(df_stream,["stream_id"])

# check
df_stream.display()

# COMMAND ----------

# DBTITLE 1,writing to delta table
df_stream.writeStream.format("delta").outputMode("append").trigger(availableNow=True).option(
    "checkpointLocation",
    "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/FactStream/checkpoint/",
).option(
    "path", "abfss://silver@spotifydeprojectstorage.dfs.core.windows.net/FactStream/data/"
).table(
    "spotify_catalog.silver.FactStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check all the tables
# MAGIC SHOW TABLES FROM spotify_catalog.silver

# COMMAND ----------

# MAGIC %md
# MAGIC