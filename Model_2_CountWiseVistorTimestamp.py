>>> events = spark.read.options(header=True, inferSchema=True).csv('/user/capstone/events.csv')
>>> events.count()
2756101
>>> visitorGroupBy = events.groupBy('visitorid').count()
>>> visitorGroupBy.show(5)

+---------+-----+
|visitorid|count|
+---------+-----+
|   361387|   15|
|  1282360|    5|
|  1354794|    1|
|   126191|    1|
|   957827|    1|
+---------+-----+
only showing top 5 rows

>>> visitorGroupBy.count()
1407580
>>> visitorGt1 = visitorGroupBy.filter(visitorGroupBy['count']>1)
>>> visitorGt1.count()
406020
>>> eventsJoinVisitorGt1 = events.join(visitorGt1, ['visitorid'])
>>> eventsJoinVisitorGt1.count()
1754541
>>> eventsJoinVisitorGt1.show(5)
+---------+-------------+-----+------+-------------+-----+
|visitorid|    timestamp|event|itemid|transactionid|count|
+---------+-------------+-----+------+-------------+-----+
|      496|1438745738297| view|269803|         null|    2|
|      496|1439922144484| view|395920|         null|    2|
|     1238|1438970280581| view| 76426|         null|    3|
|     1238|1438970177401| view| 76426|         null|    3|
|     1238|1438967704727| view| 76426|         null|    3|
+---------+-------------+-----+------+-------------+-----+

# distinct data with timestamp


eventsJoinVisitorGt1Distinct = eventsJoinVisitorGt1.distinct()
eventsJoinVisitorGt1Distinct.count()
1754081

# Assign values to event wise

>>> viewreplace = eventsJoinVisitorGt1Distinct.replace({'view':'1'})
>>> addtocartreplace = viewreplace.replace({'addtocart':'2'})
>>> final = addtocartreplace.replace({'transaction':'3'})
>>> final.show(5
+---------+-------------+-----+------+-------------+-----+
|visitorid|    timestamp|event|itemid|transactionid|count|
+---------+-------------+-----+------+-------------+-----+
|      496|1438745738297|    1|269803|         null|    2|
|      496|1439922144484|    1|395920|         null|    2|
|     1238|1438970280581|    1| 76426|         null|    3|
|     1238|1438970177401|    1| 76426|         null|    3|
|     1238|1438967704727|    1| 76426|         null|    3|
+---------+-------------+-----+------+-------------+-----+

# Extract the columns for MatrixFactorization

>>> data = final['visitorid','itemid','event']
>>> data.show(5)
+---------+------+-----+
|visitorid|itemid|event|
+---------+------+-----+
|      496|269803|    1|
|      496|395920|    1|
|     1238| 76426|    1|
|     1238| 76426|    1|
|     1238| 76426|    1|
+---------+------+-----+

#  Save the data file into hdfs

data.coalesce(1).write.option('header','true').csv('/user/capstone/CountWiseVisiter.csv')

## Building Recommendation model by using ALS

# Creating RDD file

# option A Text data
# from pyspark.sql import Row
# lines = spark.read.text('/user/maria_dev/CountWiseVisitor.csv').rdd
# parts = lines.map(lambda row: row.value.split(','))
# CountRating = parts.map(lambda p: Row(visitorid=int(p[0]),itemid=int(p[1]),event=float(p[2])))

# option B .csv file

dataCountWise = sc.textFile('/user/maria_dev/CountWiseVisiter.csv')

type(data)
<class 'pyspark.rdd.RDD'>

dataCountWise.first()
Row(value=u'visitorid,itemid,event')

header=dataCountWise.first()

# Removing header from RDD data

dataWithNoHeader = dataCountWise.filter(lambda x: x!=header)

dataWithNoHeader.first()
Row(value=u'496,269803,1')

CountRating = dataWithNoHeader.map(lambda x: x.split(','))\
.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))

# Load ALS module, MatrixFactorizationModel, Rating

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

CountRating = dataWithNoHeader.map(lambda x: x.split(','))\
.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))

# create data frame CountRating

df = sqlContext.createDataFrame(CountRating)
df.count()
1754081

# Data exploration on CountVisitor

# Distinct Users

df.select('user').distinct().show(5)
+---------+
|visitorid|
+---------+
|    15846|
|   160820|
|   342088|
|   652700|
|   703028|
+---------+


# Distinct Users's count

df.select('user').distinct().count()
406020


# Distinct Products

df.select('product').distinct().show(5)
+------+
|itemid|
+------+
|186376|
|241022|
| 55474|
|421024|
| 43367|
+------+

# GroupBy User
df.groupBy('user').count().take(5)
[Row(visitorid=15846, count=2),
Row(visitorid=160820, count=4),
Row(visitorid=342088, count=2),
Row(visitorid=652700, count=54),
Row(visitorid=703028, count=3)]

# GroupBy User stats

df.groupBy('user').count().select('count').describe().show()
+-------+-----------------+
|summary|            count|
+-------+-----------------+
|  count|           406020|
|   mean|4.321316683907197|
| stddev|23.25583913418666|
|    min|                2|
|    max|             7757|
+-------+-----------------+


df.stat.crosstab('user','rating').show(5)
+---------------+---+---+---+
|visitorid_event|1.0|2.0|3.0|
+---------------+---+---+---+
|        1253013|  8|  0|  0|
|         895344|  2|  0|  0|
|         999043|  3|  0|  0|
|         921558|  2|  0|  0|
|        1042949|  4|  0|  0|
+---------------+---+---+---+


# Splitting the CountRating data

(training,test) = CountRating.randomSplit([0.8,0.2])

training.count()
1403787
test.count()
350294

# 1. Build the recommendation model using Alternating Least Squares

# Call the ALS.train mehod to train the model

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

rank = 10
numIterations = 10
model = ALS.train(training, rank, numIterations)

model
<pyspark.mllib.recommendation.MatrixFactorizationModel object at 0x7f7afa38e850>

# Evaluate the model on training data

testdata = test.map(lambda r: (r[0],r[1]))

type(testdata)

testdata.take(5)
[(2866, 90950), (2866, 83908), (3997, 137454), (4101, 376365), (4101, 320620)]
# Making predictions

pred_ind = model.predict(496, 395920)
pred_ind
0.2152288449572859

predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2]))
type(predictions)
<class 'pyspark.rdd.PipelinedRDD'>

model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2])).take(5)
[((182316, 141578), 9.360027964671747e-13),
((182316, 453394), 1.6839190168335928e-09),
((1174034, 405862), 1.5333713377778234e-05),
((1174034, 332909), 6.701442390861636e-08),
((449586, 412676), 7.436301929458929e-07)]


# Recommend Products For Users method to generate the top N item recommendations for users

recommendItemsToUsers = model.recommendProductsForUsers(10)

recommendItemsToUsers.take(1)
[(637100,
(Rating(user=637100, product=119736, rating=0.005223899146737956),
Rating(user=637100, product=461686, rating=0.0023576194790546536),
Rating(user=637100, product=89323, rating=0.0018385901926089086),
Rating(user=637100, product=329133, rating=0.0016680327341917545),
Rating(user=637100, product=456056, rating=0.0016493104816850334),
Rating(user=637100, product=338395, rating=0.0015658005422669505),
Rating(user=637100, product=248455, rating=0.0013883330325328554),
Rating(user=637100, product=190000, rating=0.001352606204599175),
Rating(user=637100, product=174724, rating=0.0013474394347564613),
Rating(user=637100, product=7943, rating=0.0013246638674322487)))]


recommendUsersToProducts = model.recommendUsersForProducts(10)

recommendUsersToProducts.take(1)
[(185012,
(Rating(user=1150086, product=185012, rating=3.9819698650934155e-06),
Rating(user=416496, product=185012, rating=2.597633650136737e-06),
Rating(user=530559, product=185012, rating=2.5946297378327828e-06),
Rating(user=816229, product=185012, rating=2.5009509376204212e-06),
Rating(user=388556, product=185012, rating=1.8932917814169548e-06),
Rating(user=163561, product=185012, rating=1.5923644765318703e-06),
Rating(user=286616, product=185012, rating=1.539282245279953e-06),
Rating(user=684514, product=185012, rating=1.5258517524466915e-06),
Rating(user=152963, product=185012, rating=1.4641960065364423e-06),
Rating(user=776534, product=185012, rating=1.4395471554589254e-06)))]


# productFeatures
productFeatures = model.productFeatures().collect()

productFeatures_1 = sqlContext.createDataFrame(productFeatures)
model.userFeatures().collect()
productFeatures_1.show(3)
+---+--------------------+
| _1|                  _2|
+---+--------------------+
|  6|[-9.7645797723089...|
| 16|[-4.3652999011101...|
| 22|[-1.2225323189340...|
+---+--------------------+

first_product = model.productFeatures().take(1)[0]
latents = first_product[1]
len(latents)
10

# userFeatures

first_user = model.userFeatures().take(1)[0]
latents = first_user[1]
len(latents)
10


userFeatures = model.userFeatures().collect()
userFeatures_1 = sqlContext.createDataFrame(userFeatures)
userFeatures_1.show(2)
+---+--------------------+
| _1|                  _2|
+---+--------------------+
|  0|[1.02194735518423...|
|  2|[4.18885378167033...|
+---+--------------------+


first_user = model.userFeatures().take(1)[0]
latents = first_user[1]
len(latents)


# Calculate rmse
from math import sqrt

ratesAndPreds = CountRating.map(lambda r: ((r[0],r[1]), r[2])).join(predictions)

ratesAndPreds.take(5)
[((100417, 11001), (1.0, 1.0741786558519472e-15)),
((348522, 976), (1.0, 5.492003741511924e-12)),
((348522, 976), (1.0, 5.492003741511924e-12)),
((1027606, 43568), (1.0, 9.780988911393504e-07)),
((1165148, 85774), (1.0, 0.08382794911641386))]


MeanSquareError = ratesAndPreds.map(lambda r: (r[1][0]-r[1][1])**2).mean()

print("MeanSquareError = " + str(MeanSquareError))


# Root Mean Square Error

from math import sqrt
rmse = sqrt(MeanSquareError)
print("Root_Mean_Square_Error = " + str(rmse))
Root_Mean_Square_Error = 14.489856721


# 2.  Model Evluation and Selection with Hyper Parameter Tuning

df = sqlContext.createDataFrame(CountRating)

type(df)
<class 'pyspark.sql.dataframe.DataFrame'>

df.show(5)
+----+-------+------+
|user|product|rating|
+----+-------+------+
| 496| 269803|   1.0|
| 496| 395920|   1.0|
|1238|  76426|   1.0|
|1238|  76426|   1.0|
|1238|  76426|   1.0|
+----+-------+------+

# Split the input data into separate training and test datasets

(training, test) = df.randomSplit([0.8,0.2])

training.count()
1403894

test.count()
350187

# Apply implicit parameter to the data
from pyspark.ml.recommendation import ALS

als = ALS(implicitPrefs=True, userCol="user", itemCol="product", ratingCol="rating", coldStartStrategy="drop")

als

ALS_4ce5ad5e1cf6ca5e84d9

als.explainParams()
"alpha: alpha for implicit preference (default: 1.0)" \
\ncheckpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1).
E.g. 10 means that the cache will get checkpointed every 10 iterations. Note:
this setting will be ignored if the checkpoint directory is not set in the SparkContext. (default: 10)
\ncoldStartStrategy: strategy for dealing with unknown or new users/items at prediction time.
This may be useful in cross-validation or production scenarios,
for handling user/item ids the model has not seen in the training data.
Supported values: 'nan', 'drop'. (default: nan, current: drop)
\nfinalStorageLevel: StorageLevel for ALS model factors. (default:
MEMORY_AND_DISK)\nimplicitPrefs: whether to use implicit preference (default: False, current: True)
\nintermediateStorageLevel: StorageLevel for intermediate datasets.
Cannot be 'NONE'. (default: MEMORY_AND_DISK)\nitemCol:
column name for item ids. Ids must be within the integer value range. (default: item, current: product)
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

type(pipeline)
<class 'pyspark.ml.pipeline.Pipeline'>


# Set the params

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

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


evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')

# Find Root Mean Square Error

rmse = evaluator.evaluate(preds)

print("Root-mean-square error =" +str(rmse))
Root-mean-square error =1.10259915379

