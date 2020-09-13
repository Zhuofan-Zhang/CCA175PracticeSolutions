#hadoop commands
hadoop fs -ls /public/retail_db

Found 7 items
drwxr-xr-x   - hdfs hdfs          0 2017-10-18 20:46 /public/retail_db_json/categories
drwxr-xr-x   - hdfs hdfs          0 2017-10-18 20:57 /public/retail_db_json/customers
drwxr-xr-x   - hdfs hdfs          0 2017-10-18 20:08 /public/retail_db_json/departments
drwxr-xr-x   - hdfs hdfs          0 2018-05-06 00:24 /public/retail_db_json/order_details
drwxr-xr-x   - hdfs hdfs          0 2017-10-18 20:44 /public/retail_db_json/order_items
drwxr-xr-x   - hdfs hdfs          0 2017-10-18 20:39 /public/retail_db_json/orders
drwxr-xr-x   - hdfs hdfs          0 2017-10-18 20:54 /public/retail_db_json/products

#json practice
order_items=spark.read.json("/public/retail_db_json/order_items")
order_details=spark.read.json("/public/retail_db_json/order_details")
customers=spark.read.json("/public/retail_db_json/customers")

order_details.select(month(from_utc_timestamp(col("order_date"),"GMT")).alias("month")).show()
order_details.select(concat(month(from_utc_timestamp(col("order_date"),"GMT")).alias("month"),lit('|'),col('order_customer_id')).alias("_c0"))

#retail_db_practice

hadoop fs -ls /public/retail_db
Found 6 items
drwxr-xr-x   - hdfs hdfs          0 2016-12-19 03:52 /public/retail_db/categories
drwxr-xr-x   - hdfs hdfs          0 2016-12-19 03:52 /public/retail_db/customers
drwxr-xr-x   - hdfs hdfs          0 2016-12-19 03:52 /public/retail_db/departments
drwxr-xr-x   - hdfs hdfs          0 2020-07-12 16:50 /public/retail_db/order_items
drwxr-xr-x   - hdfs hdfs          0 2020-07-14 01:35 /public/retail_db/orders
drwxr-xr-x   - hdfs hdfs          0 2016-12-19 03:52 /public/retail_db/products

#here to regulate dataframes
orders=spark.read.csv("/public/retail_db/orders")
customers=spark.read.csv("/public/retail_db/customers")
order_items=spark.read.csv("/public/retail_db/order_items")
departments=spark.read.csv("/public/retail_db/departments")
products=spark.read.csv("/public/retail_db/products")
categories=spark.read.csv("/public/retail_db/categories")

Practice1
#Problem1
Requirements:
	Result should be saved in /user/cloudera/practice1/question3/output/
	Output should consist of only order_id,order_status
	Output file should be saved as Parquet file in gzip Compression.

orders=spark.read.csv('/public/retail_db/orders').select(col("_c0").alias("order_id"),col("_c3").alias("order_status"))
orders.write.option("compression","gzip").parquet("/Users/finnzhang/Desktop")

#Problem2:
Requirements:
	Order status should be COMPLETE
	Output should have customer_id,customer_fname,count
	Save the results in json format.
	Result should be order by count of orders in ascending fashion.
	Result should be saved in /user/cloudera/p1/p4/output

orders=spark.read.csv("/public/retail_db/orders").filter(col("_c3")=='COMPLETE').select(col("_c0").alias("order_id"),col("_c2").alias("customer_id")).groupBy(col("customer_id")).count().orderBy("count")
customers=spark.read.csv("/public/retail_db/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"))
joinDF=order.join(customer,"customer_id")
joinDF.write.mode("overwrite").json("path")

#Problem3:
Requirements:
	From provided parquet files located at hdfs location :
	/user/cloudera/practice1/problem5
	Get maximum product_price in each product_category
	order the results by maximum price descending.
	Final output should be saved in below hdfs location:
	/user/cloudera/practice1/problem5/output
	Final output should have product_category_id and max_price separated by pipe delimiter
	Ouput should be saved in text format with Gzip compression
	Output should be stored in a single file.


products=spark.read.csv("/public/retail_db/products").select(col("_c1").alias("product_category_id"),col("_c4").alias("price").cast("double")).groupBy("product_category_id").max("price").withColumnRenamed("max(price)","Max_Price").orderBy(desc("Max_Price")).select(concat(col("product_category_id"),lit("|"),col("Max_Price"))).withColumnRenamed("concat(product_category_id, |, Max_Price)","combined").write.mode("overwrite").option("compression","gzip").format("text").save("/user/cloudera/practice1/problem5/output")

#Problem4:
Instructions:
	Provided customer tab delimited files at below HDFS location.
	Input folder is  /user/cloudera/practice1/problem6
	Find all customers that lives 'Caguas' city.
Output Requirement:
	Result should be saved in /user/cloudera/practice1/problem6/output
	Output file should be saved in avro format in deflate compression.

from pyspark.sql.functions import col
spark.read.option("sep","\t").csv("/public/retail_db/customers").filter(col("_c6")=='Caguas').write.mode("overwrite").option("compression","deflate").format("avro").save("/user/cloudera/practice1/problem6/output")


#Problem5:
Instructions:
	Convert avro data-files stored at hdfs location /user/cloudera/practice1/problem7/customer/avro  into tab delimited file

Output Requirement:
	Result should be saved in /user/cloudera/practice1/problem7/customer_text_bzip2
	Output file should be saved as tab delimited file in bzip2 Compression.
	Output should consist of customer_id   customer_name(only first three letter)   customer_lname
	Sample Output:

	21    And   Smith

	111    Mar    Jons


from pyspark.sql.functions import col,concat,lit
#because itversity didn't provide avro files, here use the cloudera path to read files
spark.read.format("avro").load("/user/cloudera/practice1/problem7/customer/avro")
	.withColumn('customer_name',col('_c1').substr(0,3)).select(concat(col('_c0')
		,lit('\t'),col('customer_name'),lit('\t'),col('_c2')))
	.write.mode("overwrite").option("compression","bzip2").format("text")
	.save("/user/cloudera/practice1/problem7/customer_text_bzip2")

#solutions to run on itversity
customer.withColumn('customer_name',col('_c1').substr(0,3)).select(concat(col('_c0'),lit('\t'),col('customer_name'),lit('\t'),col('_c2')).alias('new_col')).show()

#Problem6:
Instructions:
	Get products from metastore table named "product_replica" whose price > 100 and save the results 
	in HDFS in parquet format.

Output Requirement:

	Result should be saved in /user/cloudera/practice1/problem8/product/output as parquet file

	Files should be saved in uncompressed format.


spark.sql("select * from product_replica p where p.product_price>100").write.mode("overwrite")
	.option("compression","uncompressed")
	.parquet("/user/cloudera/practice1/problem8/product/output")


#Problem7:
Instructions:
	Find out all PENDING_PAYMENT orders in March 2014.
	order_date format is in unix_timestamp
	Input file is parquet file stored at hdfs location
	/user/cloudera/practice1/question8
Output Requirement:
	Output should be date and total pending order for that date.
	Output should be saved at below hdfs location
	/user/cloudera/practice1/question8/output
	Output should be json file format.


from pyspark.sql.functions import unix_timestamp,col,to_date,from_unixtime


spark.read.format("parquet").load("/user/cloudera/practice1/question8")
	.filter("_c1 LIKE '2014-03%'and _c3=='PENDING_PAYMENT'")
	#.where((col('_c1').between('2014-03','2014-04')) & (col('_c3')=='PENDING_PAYMENT'))
	.withColumn('order_date',unix_timestamp('_c1'))
	.select('order_date','_c3')
	.groupBy(col('order_date'))
	.count()
	.write
	.mode("overwrite")
	.format("json")
	.save("/user/cloudera/practice1/question8/output")

orders
	.where((col('_c1').between('2014-03','2014-04')) & (col('_c3')=='PENDING_PAYMENT'))
	.withColumn('order_date',unix_timestamp('_c1'))
	.select('order_date','_c3')
	.groupBy(col('order_date'))
	.count()
	.show()

#Problem8:
Instructions:
	Find out total number of orders placed by each customers in year 2013.
	Order status should be COMPLETE
	order_date format is in unix_timestamp
	Input customer & order files are stored as avro file at below hdfs location
	/user/cloudera/practice2/question8/orders
	/user/cloudera/practice2/question8/customers
Output Requirement:
	Output should be stored in a hive table named "customer_order" with three columns customer_fname,customer_lname and orders_count.
	Hive tables should be partitioned by customer_state.

from pyspark.sql.functions import unix_timestamp,col
customers=spark.read.csv('/public/retail_db/customers').select(col('_c1').alias('customer_fname'),col('_c2').alias('customer_lname'),col('_c7').alias('customer_state'),col('_c8').alias('customer_id'))
orders.where((col('_c1').between('2013','2014')) & (col('_c3')=='COMPLETE')).withColumn('order_date',unix_timestamp('_c1')).select(col('_c0').alias('order_id'),'order_date',col('_c2').alias('customer_id'),col('_c3').alias('order_status')).groupBy('customer_id').count().withColumnRenamed('count','orders_count')
joinDF=orders.join(customers,'customer_id')
results=joinDF.select('customer_fname','customer_lname','orders_count')
#Setting spark configuration to enable Hive partitioning
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#save result into Hive table
results.write.partitionBy("customer_state").format("hive").saveAsTable("try_table")


Practice2

#Problem1
Instructions：
	Get all customers who have placed order of amount more than 200.
	Input files are tab delimeted files placed at below HDFS location:
	/user/cloudera/practice2/problem3/customers
	/user/cloudera/practice2/problem3/orders
	/user/cloudera/practice2/problem3/order_items
Output Requirements:
	Output should be placed in below HDFS Location
	/user/cloudera/practice2/problem3/joinResults
	Output file should be comma seperated file with customer_fname,customer_lname,customer_city,order_amount.
	Below header should be added to the output
	fname, lname,city,price

from pyspark.sql.functions import col
orders=spark.read.csv("/public/retail_db/orders").select(col("_c0").alias("order_id"),col("_c1").alias("order_date"),col("_c2").alias("order_customer_id"),col("_c3").alias("order_status"))
order_items=spark.read.csv("/public/retail_db/order_items").select(col("_c0").alias("order_item_id"),col("_c1").alias("order_item_order_id"),col("_c2").alias("order_item_product_id"),col("_c3").alias("order_item_quantity"),col("_c4").alias("order_item_subtotal").cast("double"),col("_c5").alias("order_item_product_price"))
customers=spark.read.csv("/public/retail_db/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"),col("_c2").alias("customer_lname"),col("_c3").alias("customer_email"),col("_c4").alias("customer_password"),col("_c5").alias("customer_street"),col("_c6").alias("customer_city"),col("_c7").alias("customer_state"),col("_c8").alias("customer_zipcode"))


orderitemJoin=orders.join(order_items,order_items['order_item_order_id']==orders['order_id']).select("order_item_subtotal","order_customer_id").groupBy("order_customer_id").sum().withColumnRenamed("sum(order_item_subtotal)","order_amount").filter(col("order_amount")>200)

customer_order=orderitemJoin.join(customers,orderitemJoin['order_customer_id']==customers['customer_id']).select(col('customer_fname').alias("fname"),col('customer_lname').alias("lname"),col('customer_city').alias("city"),col('order_amount'))

customer_order.write.mode("overwrite").option("header","true").csv("/user/cloudera/practice2/problem3/joinResults")
#Check the header of output
hadoop fs -cat /path/to/filr | head


#Problem2
Instructions:
	Get Customers from metastore table named "customers_hive" whose fname is like "Rich" and save the results in HDFS.

Output Requirement:
	Result should be saved in /user/cloudera/practice2/problem4/customers/output.
	Output should contain only fname, lname and city
	Output should be saved in text format.
	Output should be sorted by customer_city
	fname and lname should seperated by tab with city seperated by colon
Sample Output 
	Richard Plaza:Francisco
	Rich Smith:Chicago

spark.sql("select concat(customer_fname,'\t',customer_lname,':',customer_city) from customers_hive where customer_fname like '%Rich%' order by customer_city").write.mode("overwrite").format("text").save("/user/cloudera/practice2/problem4/customers/output")


#Problem3
Instructions:
	Provided pipe delimited file, get total numbers customers in each state whose first name starts with 'M'   and  save results in HDFS.
	Input folder
	/user/cloudera/problem2/customer/tab

Output Requirement:
	Result should be saved in a hive table "customer_m"
	File format should be parquet file with gzip compression.
	Output should have state name followed by total number of customers in that state.

customers.where("customer_fname like 'M%'").groupBy("customer_state").count().write.mode("overwrite").option("compression","gzip").option("fileFomat","parquet").format("hive").saveAsTable("customer_m")


#Problem4
Instructions:
	Provided  a meta-store table named product_ranked consisting of product details ,find the most expensive product in each category.

Output Requirement:
	Output should have product_category_id ,product_name,product_price,rank.
	Result should be saved in /user/cloudera/pratice4/output/  as pipe delimited text file

#Solution1:
products=spark.read.csv("/public/retail_db/products").select(col("_c1").alias("product_category_id"),col("_c2").alias("product_name"),col("_c4").alias("product_price").cast("double"))
from pyspark.sql.window import Window
import pyspark.sql.functions as func
spec=Window.partitionBy(products["product_category_id"]).orderBy(products["product_price"].desc())
from pyspark.sql.functions import dense_rank,col
products.withColumn("rank", dense_rank().over(spec)).filter("rank=1").show()

#Solution2:
spark.sql("select p.product_category_id, p.product_name, p.product_price,dense_rank() over (partition by p.product_category_id order by p.product_price desc) as product_price_rank from product_df p").filter("product_price_rank=1").show()

#Problem5
Instructions:
	Fetch all pending orders from  data-files stored at hdfs location /user/cloudera/problem3/parquet and save it  into json file  in HDFS

Output Requirement:
	Result should be saved in /user/cloudera/problem3/orders_pending
	Output file should be saved as json file.
	Output file should Gzip compressed.

spark.read.parquet("/user/cloudera/problem3/parquet").filter("order_status=='PENDING_PAYMENT'").write.mode("overwrite").option("compression","gzip").format("json").save("/user/cloudera/problem3/orders_pending")

#Problem6:
Instructions:
	provided tab delimited file at hdfs location /user/cloudera/problem3/all/customer/input 
	save only first 4 field in the result as pipe delimited file in HDFS

Output Requirement:
	Result should be saved in /user/cloudera/problem3/all/customer/output
	Output file should be saved in text format.
from pyspark.sql.functions import concat,lit

spark.read.option("sep","\t").csv("/user/cloudera/problem3/all/customer/output").select(concat("_c0","|","_c1","|","_c2","|","_c3")).write.mode("overwrite").format("text").save("/user/cloudera/problem3/all/customer/output")


spark.read.csv("/public/retail_db/customers").select(concat("_c0",lit("|"),"_c1",lit("|"),"_c2",lit("|"),"_c3")).show()


#Problem7
Instructions:
	Find top 10 products which has made highest revenue. 
	Products and order_items data are placed in HDFS directory /user/cloudera/practice4_ques6/order_items/ and /user/cloudera/practice4_ques6/products/  respectively. 
Output Requirement:
	Output should have product_id and revenue seperated with ':'  and should be saved in /user/cloudera/practice4_ques6/output


products=spark.read.csv("/public/retail_db/products").select(col("_c0").alias("product_id"),col("_c1").alias("product_name"))
order_items=spark.read.csv("/public/retail_db/order_items").select(col("_c2").alias("product_id"),col("_c4").alias("order_item_subtotal").cast("double"))
joinDF=products.join(order_items,"product_id").groupBy("product_id").sum("order_item_subtotal").orderBy(desc("sum(order_item_subtotal)")).withColumnRenamed("sum(order_item_subtotal)","revenue").limit(10)
joinDF.select(concat("product_id",lit(':'),"revenue")).write.mode("overwrite").format("text").save("/user/cloudera/practice4_ques6/output")

#Problem8
Instructions:
	Find all customers that lives 'Brownsville' city and save the result into HDFS.
	Input folder is  /user/cloudera/problem6/customer/text
Requirements:
	Result should be saved in /user/cloudera/problem6/customer_Brownsville 
	Output file should be saved in Json format
#cloudera solution
spark.read.option("sep","\t").csv("/user/cloudera/problem6/customer/text").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"),col("_c2").alias("customer_lname"),col("_c3").alias("customer_email"),col("_c4").alias("customer_password"),col("_c5").alias("customer_street"),col("_c6").alias("customer_city"),col("_c7").alias("customer_state"),col("_c8").alias("customer_zipcode")).filter("customer_city=='Brownsville")
.write.mode("overwrite").save("/user/cloudera/problem6/customer_Brownsville")
#itversity solution
spark.read.csv("/public/retail_db/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"),col("_c2").alias("customer_lname"),col("_c3").alias("customer_email"),col("_c4").alias("customer_password"),col("_c5").alias("customer_street"),col("_c6").alias("customer_city"),col("_c7").alias("customer_state"),col("_c8").alias("customer_zipcode")).filter("customer_city=='Brownsville'").show()

#Practice3:

#Problem1:
Instructions:
	From the provided avro files at below HDFS location
	/user/hive/warehouse/orders
	/user/hive/warehouse/customers
	Find out customers who have not placed any order in March 2014.
Output Requirement:
	Output should be stored in json format at below HDFS location
	/user/cca/practice3/ques1
	Output should have two fields customer_fname:customer_lname and customer_city:customer_state.
#cloudera
orders=spark.read.format("avro").load("/user/hive/warehouse/orders").select(col("_c0").alias("order_id"),col("_c1").alias("order_date"),col("_c2").alias("order_customer_id"),col("_c3").alias("order_status")).where("order_date like '2014-03%'")
customers=spark.read.format("avro").load("/user/hive/warehouse/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"),col("_c2").alias("customer_lname"),col("_c3").alias("customer_email"),col("_c4").alias("customer_password"),col("_c5").alias("customer_street"),col("_c6").alias("customer_city"),col("_c7").alias("customer_state"),col("_c8").alias("customer_zipcode"))
joinDF=customers.join(orders,customers['customer_id']==orders['order_customer_id'],'left_anti')
joinDF.select(concat('customer_fname',lit(':'),'customer_lname').alias('name'),concat('customer_city',lit(':'),'customer_city').alias('place')).write.mode("overwrite").format("json").save("/user/cca/practice3/ques1")


#itversity
customers=spark.read.csv("/public/retail_db/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"),col("_c2").alias("customer_lname"),col("_c3").alias("customer_email"),col("_c4").alias("customer_password"),col("_c5").alias("customer_street"),col("_c6").alias("customer_city"),col("_c7").alias("customer_state"),col("_c8").alias("customer_zipcode"))
orders=spark.read.csv("/public/retail_db/orders").select(col("_c0").alias("order_id"),col("_c1").alias("order_date"),col("_c2").alias("order_customer_id"),col("_c3").alias("order_status")).where("order_date like '2014-03%'")
joinDF=customers.join(orders,customers['customer_id']==orders['order_customer_id'],'left_anti')
joinDF.select(concat('customer_fname',lit(':'),'customer_lname').alias('name'),concat('customer_city',lit(':'),'customer_city').alias('place'))




#Problem2:
Instructions:
	Get count of customers in each city who have placed order of amount more than 100 and  
	whose order status is not PENDING.
	Input files are tab delimeted files placed at below HDFS location:
		/user/cloudera/practice3/problem3/customers
		/user/cloudera/practice3/problem3/orders
		/user/cloudera/practice3/problem3/order_items

Output Requirements:
	Output should be placed in below HDFS Location
	/user/cloudera/practice3/problem3/joinResults
	Output file should be tab separated file with deflate compression.

from pyspark.sql.functions import col
orders=spark.read.csv("/public/retail_db/orders").select(col("_c0").alias("order_id"),col("_c1").alias("order_date"),col("_c2").alias("order_customer_id"),col("_c3").alias("order_status"))
order_items=spark.read.csv("/public/retail_db/order_items").select(col("_c0").alias("order_item_id"),col("_c1").alias("order_item_order_id"),col("_c2").alias("order_item_product_id"),col("_c3").alias("order_item_quantity"),col("_c4").alias("order_item_subtotal").cast("double"),col("_c5").alias("order_item_product_price"))
customers=spark.read.csv("/public/retail_db/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"),col("_c2").alias("customer_lname"),col("_c3").alias("customer_email"),col("_c4").alias("customer_password"),col("_c5").alias("customer_street"),col("_c6").alias("customer_city"),col("_c7").alias("customer_state"),col("_c8").alias("customer_zipcode"))
customers.select('customer_id','customer_city')

order_amount=order_items.select('order_item_order_id','order_item_subtotal').groupBy('order_item_order_id').sum('order_item_subtotal')
fullOrder=orders.join(customers,customers['customer_id']==orders['order_customer_id']).where("order_status not like '%PENDING%'")
resultDF=fullOrder.select('order_id','customer_id','order_status','customer_city').join(order_amount,orders['order_id']==order_amount['order_item_order_id']).where(col('sum(order_item_subtotal)')>100)
result=resultDF.groupBy('customer_city').count()

#Problem4

Instructions:
	Save the result to hdfs using no compression as sequence file.
Output Requirement:
	Result should be saved in at /user/cloudera/problem4_ques6/output
	fields should be seperated by pipe delimiter.
	Key should be order_id, value should be all fields seperated by a pipe.
data=spark.read.parquet("/user/cloudera/problem4_ques6/output")
data.rdd.map



result.rdd.map(lambda x:(x[0],x.mkString("|")))

	x=>(x(0).toString,x.mkString("|"))).saveAsSequenceFile("path")





#Problem5
Instructions:
	using product_ranked_new metastore table, Find the top 5 most expensive products within each category 
Output Requirement:
	Output should have product_category_id,product_name,product_price.
	Result should be saved in /user/cloudera/pratice4/question2/output
	Output should be saved as pipe delimited file.
from pyspark.sql.window import Window
from pyspark.sql.functions import col,desc,dense_rank
spec=Window.partitionBy('product_category_id').orderBy(desc('product_price'))
products.withColumn("rank", dense_rank().over(spec)).filter('rank <=5').show()
products.select(concat('product_category_id',lit('|'),'product_name',lit('|'),'product_price').alias('new_col'))
.write.mode("overwrite").format("/user/cloudera/pratice4/question2/output").save("/user/cloudera/pratice4/question2/output")


#Problem6
Instructions:
	Input file is provided at below HDFS location
	/user/cloudera/prac3/ques6/xml
	Append first three character of firstname with first two character 
	of last name with a colon.
Output Requirement:
	Output should be saved in xml file with rootTag as persons and rowTag as person.
	Output should be saved at below HDFS location
	/user/cloudera/prac3/ques6/output/xml

from pyspark.sql.functions import col,concat,lit
customers=spark.read.csv("/public/retail_db/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"),col("_c2").alias("customer_lname"),col("_c3").alias("customer_email"),col("_c4").alias("customer_password"),col("_c5").alias("customer_street"),col("_c6").alias("customer_city"),col("_c7").alias("customer_state"),col("_c8").alias("customer_zipcode"))
customers.select(concat(col('customer_fname').substr(0,3),lit(':'),col('customer_lname').substr(0,2)).alias("new_col")).show()
.write.mode("overwrite").format("com.databricks.spark.xml").option("rootTag","persons").option("rowTag","person").save("/user/cloudera/prac3/ques6/output/xml")

#Final Practice
#Q2-(T1Q1)
from pyspark.sql.functions import col
spark.read("/user/cloudera/practice1/question3 ").select(col("_c0").alias("order_id"),col("_c3").alias("order_status")).write.mode("overwrite").option("compression","gzip").parquet("/user/cloudera/practice1/question3/output/")

#Q3-(T2Q6)
from pyspark.sql.functions import col,concat,lit
spark.read.option("sep","\t").csv("/user/cloudera/problem3/all/customer/input")
.select(concat(col("_c0"),lit("|"),col("_c1"),lit("|"),col("_c2"),lit("|").col("_c3")).alias("new_col")).write.mode("overwrite").format("text").save("/user/cloudera/problem3/all/customer/output")

#Q4-(T2Q2)
from pyspark.sql.functions import col,concat,lit

spark.sql("select concat(customer_fname,'\t',customer_lname,':',customer_city)from customers_hive where customer_fname like '%Rich%' order by customer_city").write.mode("overwrite").format('text').save("/user/cloudera/practice2/problem4/customers/output")

#Q5-(T1Q2)
from pyspark.sql.functions import col,concat,lit
orders=spark.read.csv("/public/retail_db/orders").select(col("_c0").alias("order_id"),col("_c1").alias("order_date"),col("_c2").alias("order_customer_id"),col("_c3").alias("order_status")).filter(col('order_status')=='COMPLETE').groupBy(col('order_customer_id')).count().filter("count>=4")

customers=spark.read.csv("/public/retail_db/customers").select(col("_c0").alias("customer_id"),col("_c1").alias("customer_fname"))
orders.join(customers,customers['customer_id']==orders['order_customer_id']).select('customer_id','customer_fname','count').orderBy("count").write.mode("overwrite").format("json").save("/user/cloudera/p1/p4/output")

#Q6-(T2Q7)
from pyspark.sql.functions import col,concat,lit

order_items=spark.read.csv("/public/retail_db/order_items").select(col("_c2").alias("order_item_product_id"),col("_c4").alias("order_item_subtotal").cast("double")).groupBy("order_item_product_id").sum("order_item_subtotal")
products=spark.read.csv("/public/retail_db/products").select(col("_c0").alias("product_id"),col("_c1").alias("product_name"))
order_items.join(products,order_items['order_item_product_id']==products['product_id']).select(concat('product_id',lit(':'),'sum(order_item_subtotal)').alias('new_col')).write.mode("overwrite").format("text").save("/user/cloudera/practice4_ques6/output")

#Q7-(T1Q5)
from pyspark.sql.functions import col,concat,lit
customers=spark.read.format("avro").load("/user/cloudera/practice1/problem7/customer/avro")
customers.select(concat(col('customer_id'),lit('\t'),col('customer_fname').substr(0,3),lit('\t'),col('customer_lname')).alias("id\tfname\tlname")).write.mode("overwrite").option("compression","bzip2").format("text").save("/user/cloudera/practice1/problem7/customer_text_bzip2")

#Q8-(T1Q7)
from pyspark.sql.functions import col,concat,lit,unix_timestamp
orders=spark.read.csv("/public/retail_db/orders").select(col("_c0").alias("order_id"),col("_c1").alias("order_date"),col("_c2").alias("order_customer_id"),col("_c3").alias("order_status")).filter("order_status like '%PENDING_PAYMENT%'").filter("order_date like '2014-03%'")
output=orders.filter("order_status like '%PENDING_PAYMENT%'").groupBy("order_date").count().select(unix_timestamp(col("order_date")).alias("order_date"),"count")
output.write.mode("overwrite").format("json").save("/user/cloudera/practice1/question8/output")

#Q9-(T2Q3)
from pyspark.sql.functions import col,cancat,lit
customers=spark.read.option("sep","|").csv("/user/cloudera/problem2/customer/tab")
customers.filter("customer_fname like 'M%'").groupBy('customer_state').count().write.format("parquet").option("compression","gzip").saveAsTable("customer_m")

#Q10-(T3Q4)
orders.rdd.map(lambda x: "|".join(map(str,x)))
orders.rdd.map(lambda line: "|".join([str(x) for x in line]))


sqoop import --connect jdbc:mysql://quickstart:3306/retail_db -username= -password= -table=
