# Databricks notebook source
#this is the new code change after 1st commit
#again commiting in test branch

# COMMAND ----------

storageAccount="anirbanstoragegen2"
mountpoint = "/mnt/Gen2"
storageEndPoint ="abfss://rawdata@{}.dfs.core.windows.net/".format(storageAccount)
print ('Mount Point ='+mountpoint)

#ClientId, TenantId and Secret is for the Application(ADLSGen2App) was have created as part of this recipe
clientID ="b2b2d3ae-7c44-4d86-ae49-b61e75747af8"
tenantID ="6c83dbdd-8cff-419c-ab35-6f31f9613cca"
clientSecret ="lP-7Q~kwkK161xaxzzbrWw.zBEho1Lr_Hn-vC"
oauth2Endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(tenantID)


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": oauth2Endpoint}

try:
  dbutils.fs.mount(
  source = storageEndPoint,
  mount_point = mountpoint,
  extra_configs = configs)
except:
    print("Already mounted...."+mountpoint)



# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls /mnt/Gen2

# COMMAND ----------

display(dbutils.fs.ls("/mnt/Gen2"))

# COMMAND ----------

# Reading Orders.csv file in a dataframe
df_ord= spark.read.format("csv").option("header",True).load("dbfs:/mnt/Gen2/Orders.csv")


# COMMAND ----------

display(df_ord)

# COMMAND ----------

#Run this command to unmount the mount point
dbutils.fs.unmount("/mnt/Gen2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Storage Account AccessKey for authentication to read files from ADLS Gen-2

# COMMAND ----------

#This is ADLS Gen-2 accountname and access key details
storageaccount="anirbanstoragegen2"
acct_info=f"fs.azure.account.key.{storageaccount}.dfs.core.windows.net"
accesskey="vcWxWV5fth2ahqQlE/WUuGZNPdpqHnrNq+4SaFYp2bh93L3TP8/2ohd2Dna1WCm94BPHZM+5peEt65JHN+ix8Q==" 
print(acct_info)

# COMMAND ----------

#Setting account credentials in  notebook session configs
spark.conf.set(
    acct_info,
   accesskey)

# COMMAND ----------

dbutils.fs.ls("abfss://rawdata@anirbanstoragegen2.dfs.core.windows.net/Orders.csv")

# COMMAND ----------

ordersDF =spark.read.format("csv").option("header",True).load("abfss://rawdata@anirbanstoragegen2.dfs.core.windows.net/Orders.csv")

# COMMAND ----------

display(ordersDF)
