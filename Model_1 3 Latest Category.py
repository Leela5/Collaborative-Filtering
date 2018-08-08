### Recommendation system on Ecommerce category data

## Data Cleaning

# events data
events = spark.read.options(header=True, inferSchema=True).csv('/user/maria_dev/events.csv')
events.count()
2756101
events.show(5)
+-------------+---------+-----+------+-------------+
|    timestamp|visitorid|event|itemid|transactionid|
+-------------+---------+-----+------+-------------+
|1433221332117|   257597| view|355908|         null|
|1433224214164|   992329| view|248676|         null|
|1433221999827|   111016| view|318965|         null|
|1433221955914|   483717| view|253185|         null|
|1433221337106|   951259| view|367447|         null|
+-------------+---------+-----+------+-------------+
# Drop timestamp and transactionid as a part of cleaning NAs and noise coulmn in events data

events_clean = events.drop('timestamp', 'transactionid')

events_clean.show(5)
+---------+-----+------+
|visitorid|event|itemid|
+---------+-----+------+
|   257597| view|355908|
|   992329| view|248676|
|   111016| view|318965|
|   483717| view|253185|
|   951259| view|367447|
+---------+-----+------+

# item data
item1 = spark.read.options(header=True,inferSchema=True).csv('/user/capstone/item_properties_part1.csv')

item1.show(5)
+-------------+------+----------+--------------------+
|    timestamp|itemid|  property|               value|
+-------------+------+----------+--------------------+
|1435460400000|460429|categoryid|                1338|
|1441508400000|206783|       888|1116713 960601 n2...|
|1439089200000|395014|       400|n552.000 639502 n...|
|1431226800000| 59481|       790|          n15360.000|
|1431831600000|156781|       917|              828513|
+-------------+------+----------+--------------------+

item1.count()
10999999


item2 = spark.read.options(header=True,inferSchema=True).csv('/user/capstone/item_properties_part2.csv')

item2.show(5)
+-------------+------+--------+---------------+
|    timestamp|itemid|property|          value|
+-------------+------+--------+---------------+
|1433041200000|183478|     561|         769062|
|1439694000000|132256|     976|n26.400 1135780|
|1435460400000|420307|     921|1149317 1257525|
|1431831600000|403324|     917|        1204143|
|1435460400000|230701|     521|         769062|
+-------------+------+--------+---------------+

item2.count()
9275903
item = item1.unionAll(item2)

item.count()
20275902

item.show(5)
+-------------+------+----------+--------------------+
|    timestamp|itemid|  property|               value|
+-------------+------+----------+--------------------+
|1435460400000|460429|categoryid|                1338|
|1441508400000|206783|       888|1116713 960601 n2...|
|1439089200000|395014|       400|n552.000 639502 n...|
|1431226800000| 59481|       790|          n15360.000|
|1431831600000|156781|       917|              828513|
+-------------+------+----------+--------------------+

# Select items only with categoryid

itemsWithCategory = item[item.property == 'categoryid']

itemsWithCategory.show(5)
+-------------+------+----------+-----+
|    timestamp|itemid|  property|value|
+-------------+------+----------+-----+
|1435460400000|460429|categoryid| 1338|
|1432436400000|281245|categoryid| 1277|
|1435460400000| 35575|categoryid| 1059|
|1437274800000|  8313|categoryid| 1147|
|1437879600000| 55102|categoryid|   47|
+-------------+------+----------+-----+

itemsWithCategory.count()
788214
# itemsWithCategory groupBy items with max timestamp

itemsWithCategoryidMaxTimestamp = itemsWithCategory.groupBy('itemid').max('timestamp')

itemsWithCategoryidMaxTimestamp.show(5)
+------+--------------+
|itemid|max(timestamp)|
+------+--------------+
| 30903| 1442113200000|
|114206| 1431226800000|
| 56987| 1431226800000|
|205270| 1431226800000|
|376650| 1442113200000|
+------+--------------+

itemsWithCategoryidMaxTimestamp.count()
417053


# Rename the max(timestamp) as timestamp to merge in itemsWithCategory

item_time = itemsWithCategoryidMaxTimestamp.toDF('itemid','timestamp')

item_time.show(5)
+------+-------------+
|itemid|    timestamp|
+------+-------------+
| 30903|1442113200000|
|114206|1431226800000|
| 56987|1431226800000|
|205270|1431226800000|
|376650|1442113200000|
+------+-------------+

# Join the itemsWithCategory and item_with_ max_time

itemsWithMaxTimestamCategory = itemsWithCategory.join(item_time, ['itemid','timestamp'])

itemsWithMaxTimestamCategory.show(5)
+------+-------------+----------+-----+
|itemid|    timestamp|  property|value|
+------+-------------+----------+-----+
|   261|1433041200000|categoryid|  816|
|   682|1433041200000|categoryid|  217|
|   715|1433041200000|categoryid|  403|
|   925|1431226800000|categoryid|  959|
|  1101|1442113200000|categoryid| 1244|
+------+-------------+----------+-----+

itemsWithMaxTimestamCategory.count()
417053

# Join the itemsWithMaxtimestampCategory with events data

eventItemsWithMaxtimestamCategory = events_clean.join(itemsWithMaxtimestamCategory, ['itemid'])

eventItemsWithMaxtimestamCategory.count()
2500516

eventItemsWithMaxtimestamCategory.show(5)
+------+---------+-----+-------------+----------+-----+
|itemid|visitorid|event|    timestamp|  property|value|
+------+---------+-----+-------------+----------+-----+
|   496|   198722| view|1431226800000|categoryid|  707|
|   496|   198722| view|1431226800000|categoryid|  707|
|   496|   198722| view|1431226800000|categoryid|  707|
|   496|   580567| view|1431226800000|categoryid|  707|
|   496|   566277| view|1431226800000|categoryid|  707|
+------+---------+-----+-------------+----------+-----+


# Give values to variable event-view=1, addtocart=2,transaction=3

view = eventItemsWithMaxtimestamCategory.replace({'view':'1'})

addtocart = view.replace({'addtocart':'2'})

CatVisitor = addtocart.replace({'transaction':'3'})

CatVisitor.show(5)
+------+---------+-----+-------------+----------+-----+
|itemid|visitorid|event|    timestamp|  property|value|
+------+---------+-----+-------------+----------+-----+
|   496|   198722|    1|1431226800000|categoryid|  707|
|   496|   198722|    1|1431226800000|categoryid|  707|
|   496|   198722|    1|1431226800000|categoryid|  707|
|   496|   580567|    1|1431226800000|categoryid|  707|
|   496|   566277|    1|1431226800000|categoryid|  707|
+------+---------+-----+-------------+----------+-----+


# Rename the value variable as category and drop property column

CatVisitor_1 = CatVisitor.drop('property')

CatVisitor_1.show(2)
+------+---------+-----+-------------+-----+
|itemid|visitorid|event|    timestamp|value|
+------+---------+-----+-------------+-----+
|   496|   198722|    1|1431226800000|  707|
|   496|   198722|    1|1431226800000|  707|
+------+---------+-----+-------------+-----+


CatVisitor_2 = CatVisitor_1.toDF('itemid','visitorid','event','timestamp','categoryid')

CatVisitor_2.show(2)
+------+---------+-----+-------------+----------+
|itemid|visitorid|event|    timestamp|categoryid|
+------+---------+-----+-------------+----------+
|   496|   198722|    1|1431226800000|       707|
|   496|   198722|    1|1431226800000|       707|
+------+---------+-----+-------------+----------+

# Extract columns visitorid, categoryid and event to create recommender matrix

CatVisitor_3 = CatVisitor_2['visitorid','categoryid','event']

CatVisitor_3.show(2)
+---------+----------+-----+
|visitorid|categoryid|event|
+---------+----------+-----+
|   198722|       707|    1|
|   198722|       707|    1|
+---------+----------+-----+

# Drop duplicates-take distinct values

final = CatVisitor_3.distinct()

final.count()
1502956

# Save finalMarix into hdfs

final.coalesce(1).write.option('header','true').csv('/user/maria_dev/CategoryWiseRec.csv')

# Creating RDD file

# Option A: Text data
# lines = spark.read.text('/user/maria_dev/CategoryWiseRec.csv).rdd
# from pyspark.sql import Row
# parts = lines.map(lambda row: row.value.split(","))
# CountRating = parts.map(lambda p: Row(visitorid=int(p[0]),itemid=int(p[1]),event=float(p[2])))

# Option B : .csv file

data = sc.textFile('/user/maria_dev/CategoryWiseRec.csv')

type(data)
<class 'pyspark.rdd.RDD'>

data.first()
u'visitorid,categoryid,event'

header=data.first()

# Removing header from RDD data

dataWithNoHeader = data.filter(lambda x: x!=header)

dataWithNoHeader.first()
u'1342555,707,1'

CatRating = dataWithNoHeader.map(lambda x: x.split(','))\
.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))

CatRating.take(5)
[Rating(user=1342555, product=707, rating=1.0),
Rating(user=244210, product=242, rating=1.0),
Rating(user=1229304, product=1660, rating=1.0),
Rating(user=672853, product=342, rating=1.0),
Rating(user=1056465, product=921, rating=1.0)]

type(CatRating)
<class 'pyspark.rdd.PipelinedRDD'>

##Data exploration on CatRating

# Creating DataFrame from RDD

df = sqlContext.createDataFrame(CatRating)

# Distinct Users

df.select('user').distinct().show(5)
+-------+
|   user|
+-------+
|1187945|
| 993085|
| 901600|
|1030029|
|1073116|
+-------+

# Distinct Users's count

df.select('user').distinct().count()
1236032

# Distinct Products

df.select('product').distinct().show(2)

df.select('product').distinct().count() # Category=product
1086

# GroupBy User

df.groupBy('user').count().take(2)
+-------+
|product|
+-------+
|    474|
|     29|

# GroupBy User stats

df.groupBy('user').count()
[Row(user=1187945, count=1), Row(user=993085, count=1)]

df.groupBy('user').count().select('count').describe().show()
+-------+------------------+
|summary|             count|
+-------+------------------+
|  count|           1236032|
|   mean| 1.215952337803552|
| stddev|2.9331561296274162|
|    min|                 1|
|    max|               967|
+-------+------------------+

df.stat.crosstab('user','rating').show(2)
+-----------+---+---+---+
|user_rating|1.0|2.0|3.0|
+-----------+---+---+---+
|     669325|  1|  0|  0|
|     834212|  1|  0|  0|
+-----------+---+---+---+

## 1. Build the recommendation model using Alternating Least Squares

# Splitting the CatRating data to apply ALS

(training,test) = CatRating.randomSplit([0.8,0.2])

training.count()
1202345

test.count()
300611

# Call the ALS.train mehod to train the model

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

rank = 10
numIterations = 10
model = model = ALS.trainImplicit(training, rank, numIterations, alpha=0.01)

model
<pyspark.mllib.recommendation.MatrixFactorizationModel object at 0x7fb12319b810>

# Evaluate the model on training data

testdata = test.map(lambda r: (r[0],r[1]))

type(testdata)
<class 'pyspark.rdd.PipelinedRDD'>

testdata.take(5)
[(278716, 1558), (1387683, 324), (639095, 1192), (240681, 646), (31895, 969)]


# Making predictions

pred_ind = model.predict(248925, 745)

pred_ind
0.005986103252579589


model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2])).take(5)

[((129434, 248), 0.0003330421141751461),
((444564, 1258), 0.0006727534048898695),
((409494, 1493), 0.0007292980295591346),
((1167720, 532), 8.258755924978831e-05),
((975632, 438), 0.0012485413970216514)]

# Recommend Products For Users method to generate the top N item recommendations for users

recommendItemsToUsers = model.recommendProductsForUsers(10)


recommendItemsToUsers.take(1)
[(185012,
(Rating(user=185012, product=491, rating=0.04871482515216735),
Rating(user=185012, product=1051, rating=0.028699492512778257),
Rating(user=185012, product=48, rating=0.02862404496957833),
Rating(user=185012, product=342, rating=0.019202382534929077),
Rating(user=185012, product=1279, rating=0.019065606952296678),
Rating(user=185012, product=683, rating=0.016388326487131587),
Rating(user=185012, product=1483, rating=0.013969881764322206),
Rating(user=185012, product=1173, rating=0.006706425900091805),
Rating(user=185012, product=707, rating=0.005186738334121345),
Rating(user=185012, product=242, rating=0.0030714082576446533)))]


recommendItemsToUsers.count()
1011459

recommendUsersToItems = model.recommendUsersForProducts(10)

recommendUsersToItems.take(1)
[(692,
(Rating(user=1150086, product=692, rating=3.815991179675704e-06),
Rating(user=152963, product=692, rating=3.602256942357526e-06),
Rating(user=530559, product=692, rating=3.520049637823572e-06),
Rating(user=76757, product=692, rating=3.3283541941733388e-06),
Rating(user=247235, product=692, rating=3.2208352348381957e-06),
Rating(user=684514, product=692, rating=3.163277380057813e-06),
Rating(user=286616, product=692, rating=2.986332752841223e-06),
Rating(user=163561, product=692, rating=2.9641948204477643e-06),
Rating(user=138131, product=692, rating=2.931711763002885e-06),
Rating(user=757355, product=692, rating=2.9176650252686586e-06)))]

# productFeatures
productFeatures = model.productFeatures().collect()

productFeatures_1 = sqlContext.createDataFrame(productFeatures)
model.userFeatures().collect()
productFeatures_1.show(3)
+---+--------------------+
| _1|                  _2|
+---+--------------------+
|  0|[-0.0064263292588...|
|  4|[-0.0041954130865...|
|  6|[-3.2375278533436...|
+---+--------------------+

first_product = model.productFeatures().take(1)[0]
latents = first_product[1]
len(latents)
5


first_user = model.userFeatures().take(1)[0]
latents = first_user[1]
len(latents)
10

# userFeatures

userFeatures = model.userFeatures().collect()
userFeatures_1 = sqlContext.createDataFrame(userFeatures)
userFeatures_1.show(2)
+---+--------------------+
| _1|                  _2|
+---+--------------------+
|  0|[-0.0089762806892...|
|  2|[-0.0013274035882...|
+---+--------------------+

first_user = model.userFeatures().take(1)[0]
latents = first_user[1]
len(latents)
5

# Calculate rmse
from math import sqrt


ratesAndPreds = CatRating.map(lambda r: ((r[0],r[1]), r[2])).join(predictions)

ratesAndPreds.take(5)
[((1325500, 154), (1.0, 0.0003349065846972463)),
((804836, 1450), (1.0, 1.1383323696916125e-05)),
((804836, 1450), (1.0, 1.1383323696916125e-05)),
((804836, 1450), (3.0, 1.1383323696916125e-05)),
((804836, 1450), (3.0, 1.1383323696916125e-05))]

MSE = ratesAndPreds.map(lambda r: (r[1][0]-r[1][1])**2).mean()

print("MeanSquareError = " + str(MSE))
MeanSquareError = 2.18612432379

# Root Mean Square Error

rmse = sqrt(MSE)

print("Root_Mean_Square_Error = " + str(rmse))
Root_Mean_Square_Error = 1.47855480919


# 2.  Model Evluation and Selection with Hyper Parameter Tuning

df = sqlContext.createDataFrame(CatRating)

type(df)

df.show(5)
+-------+-------+------+
|   user|product|rating|
+-------+-------+------+
|1342555|    707|   1.0|
| 244210|    242|   1.0|
|1229304|   1660|   1.0|
| 672853|    342|   1.0|
|1056465|    921|   1.0|
+-------+-------+------+

# Split the input data into training and test datasets

(training, test) = df.randomSplit([0.8,0.2])



# Apply implicit parameter to the data

from pyspark.ml.recommendation import ALS

als = ALS(implicitPrefs=True, userCol="user", itemCol="product", ratingCol="rating", coldStartStrategy="drop")

als
ALS_4fdeaff285b75ff8d702

als.explainParams()
"alpha: alpha for implicit preference (default: 1.0)" \
\ncheckpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1).
E.g. 10 means that the cache will get checkpointed every 10 iterations.
Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext. (default: 10)
\ncoldStartStrategy: strategy for dealing with unknown or new users/items at prediction time.
This may be useful in cross-validation or production scenarios,
for handling user/item ids the model has not seen in the training data.
Supported values: 'nan', 'drop'. (default: nan, current: drop)\nfinalStorageLevel:
StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)\nimplicitPrefs:
whether to use implicit preference (default: False, current: True)\nintermediateStorageLevel:
StorageLevel for intermediate datasets. Cannot be 'NONE'.
(default: MEMORY_AND_DISK)\nitemCol: column name for item ids.
Ids must be within the integer value range. (default: item, current: product)
\nmaxIter: max number of iterations (>= 0). (default: 10)
\nnonnegative: whether to use nonnegative constraint for least squares (default: False)
\nnumItemBlocks: number of item blocks (default: 10)
\nnumUserBlocks: number of user blocks (default: 10)
\npredictionCol: prediction column name. (default: prediction)
\nrank: rank of the factorization (default: 10)
\nratingCol: column name for ratings (default: rating, current: rating)
\nregParam: regularization parameter (>= 0). (default: 0.1)
\nseed: random seed. (default: 593367982098446717)
\nuserCol: column name for user ids. Ids must be within the integer value range. (default: user, current: user)"


from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[als])

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Set the params

paramImplicit = ParamGridBuilder()\
.addGrid(als.rank,[5,10])\
.addGrid(als.maxIter,[5,10])\
.addGrid(als.regParam,[1.0,5.0])\
.build()


from pyspark.ml.evaluation import RegressionEvaluator

evaluatoR = RegressionEvaluator(metricName='rmse', labelCol='rating')

# Fit the estimator using params

cvImplicit = CrossValidator(estimator=als, estimatorParamMaps=paramImplicit, evaluator=evaluatoR, numFolds=3)

cvModel = cvImplicit.fit(training)

# Fit the model

# Select the model produced by the best performance set of params

preds = cvModel.bestModel.transform(test)

preds.take(1)
[Row(user=1337833, product=496, rating=1.0, prediction=0.0007150661549530923)]

evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')

# Find Root Mean Square Error

rmse = evaluator.evaluate(preds)

print("Root-mean-square error = " + str(rmse))
Root_mean_square_error = 1.29930373737



