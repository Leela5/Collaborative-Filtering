>>> events = spark.read.options(header=True, inferSchema=True).csv('/user/capstone/events.csv')
>>> events.count()
2756101

events.show(3)
+-------------+---------+-----+------+-------------+
|    timestamp|visitorid|event|itemid|transactionid|
+-------------+---------+-----+------+-------------+
|1433221332117|   257597| view|355908|         null|
|1433224214164|   992329| view|248676|         null|
|1433221999827|   111016| view|318965|         null|
+-------------+---------+-----+------+-------------+

# GroupBy user's count on event

visitorGroupBy = events.groupBy('visitorid').count()

visitorGroupBy.show(5)
+---------+-----+
|visitorid|count|
+---------+-----+
|   361387|   15|
|  1282360|    5|
|  1354794|    1|
+---------+-----+

# filter the visitors who has more than 2 event

visitorGt2 = visitorGroupBy.filter(visitorGroupBy['count']>2)

visitorGt2.count()
200028

eventsJoinVisitorGt2 = events.join(visitorGt2, ['visitorid'])

eventsJoinVisitorGt2.count()
1342557


>>> eventsJoinVisitorGt2.show(5)
+---------+-------------+-----+------+-------------+-----+
|visitorid|    timestamp|event|itemid|transactionid|count|
+---------+-------------+-----+------+-------------+-----+
|     1238|1438970280581| view| 76426|         null|    3|
|     1238|1438970177401| view| 76426|         null|    3|
|     1238|1438967704727| view| 76426|         null|    3|
|     2659|1439873167520| view|444820|         null|    3|
|     2659|1439873142540| view|444820|         null|    3|
+---------+-------------+-----+------+-------------+-----+


# distinct data with timestamp

eventsJoinVisitorGt2Distinct = eventsJoinVisitorGt1.distinct()

eventsJoinVisitorGt2Distinct.count()
1342128

eventsJoinVisitorGt2Distinct.show(5)
+---------+-------------+-----+------+-------------+-----+
|visitorid|    timestamp|event|itemid|transactionid|count|
+---------+-------------+-----+------+-------------+-----+
|     1238|1438970280581| view| 76426|         null|    3|
|     1238|1438970177401| view| 76426|         null|    3|
|     1238|1438967704727| view| 76426|         null|    3|
|     2659|1439873167520| view|444820|         null|    3|
|     2659|1439873142540| view|444820|         null|    3|
+---------+-------------+-----+------+-------------+-----+


# Assign values to event wise

viewreplace = eventsJoinVisitorGt1Distinct.replace({'view':'1'})

addtocartreplace = viewreplace.replace({'addtocart':'2'})

final = addtocartreplace.replace({'transaction':'3'})

final.show(5)
+---------+-------------+-----+------+-------------+-----+
|visitorid|    timestamp|event|itemid|transactionid|count|
+---------+-------------+-----+------+-------------+-----+
|      496|1438745738297|    1|269803|         null|    2|
|      496|1439922144484|    1|395920|         null|    2|
|     1238|1438970280581|    1| 76426|         null|    3|
|     1238|1438970177401|    1| 76426|         null|    3|
|     1238|1438967704727|    1| 76426|         null|    3|
+---------+-------------+-----+------+-------------+-----+

# Extract the columns for MatrixFactorization to apply ALS

data_1 = final['visitorid','itemid','event']

data_1.show(5)
+---------+------+-----+
|visitorid|itemid|event|
+---------+------+-----+
|      496|269803|    1|
|      496|395920|    1|
|     1238| 76426|    1|
|     1238| 76426|    1|
|     1238| 76426|    1|
+---------+------+-----+

# distinct final data to avoid redudence

data = data_1.distinct()

data.count()
881845

data.show(5)
+---------+------+-----+
|visitorid|itemid|event|
+---------+------+-----+
|     1238| 76426|    1|
|     2659|444820|    1|
|     2659|293297|    1|
|     2866| 90950|    1|
|     2866|294615|    1|
+---------+------+-----+


#  Save the data file into hdfs

data.coalesce(1).write.option('header','true').csv('/user/capstone/CountGt2Visitors.csv')

## Building Recommendation model by using ALS

# Creating RDD file

# option A Text data
# from pyspark.sql import Row
# lines = spark.read.text('/user/maria_dev/CountWiseVisitor.csv').rdd
# parts = lines.map(lambda row: row.value.split(','))
# CountRating = parts.map(lambda p: Row(visitorid=int(p[0]),itemid=int(p[1]),event=float(p[2])))

# option B .csv file

dataGt2 = sc.textFile('/user/maria_dev/CountGt2Visitors.csv')

type(data)
<class 'pyspark.sql.dataframe.DataFrame'>

dataGt2.first()
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
881845

# Data exploration on Frequency Visitor

# Distinct Users

df.select('user').distinct().show(5)
+-------+
|   user|
+-------+
| 160820|
| 652700|
| 703028|
| 978440|
|1181937|
+-------+


# Distinct Users's count

df.select('user').distinct().count()
200028


# Distinct Products

df.select('product').distinct().show(5)
+-------+
|product|
+-------+
| 186376|
| 241022|
|  55474|
| 161736|
| 253815|
+-------+

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
705486

test.count()
176359

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
<class 'pyspark.rdd.PipelinedRDD'>

testdata.take(5)
[(2866, 90950), (2866, 83908), (3997, 137454), (4101, 376365), (4101, 320620)]
# Making predictions

pred_ind = model.predict(1238, 76426)

pred_ind
0.001846907528547799

predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2]))
type(predictions)
<class 'pyspark.rdd.PipelinedRDD'>

model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2])).take(5)
[((637100, 192463), 0.00010765068490892046),
((182316, 141578), -6.44559387549092e-10),
((182316, 453394), 4.6285229011965986e-07),
((1174034, 405862), 0.0002770810063859945),
((1174034, 167019), 1.7614237709621636e-06)]


# Recommend Products For Users method to generate the top N item recommendations for users

recommendItemsToUsers = model.recommendProductsForUsers(10)

recommendItemsToUsers.take(1)
[(637100,
(Rating(user=637100, product=257040, rating=0.0035265957187764056),
Rating(user=637100, product=234255, rating=0.0027571984738903274),
Rating(user=637100, product=309778, rating=0.0024207167131620262),
Rating(user=637100, product=38965, rating=0.002147159295198723),
Rating(user=637100, product=9877, rating=0.001876074814609792),
Rating(user=637100, product=119736, rating=0.0018677960976958255),
Rating(user=637100, product=241555, rating=0.001646654776817716),
Rating(user=637100, product=367664, rating=0.0015888999990227321),
Rating(user=637100, product=432478, rating=0.0015526300678421956),
Rating(user=637100, product=58638, rating=0.0015180707320581912)))]

recommendItemsToUsers.count()
200028

# Recommend Users For Products method to generate the top N Users recommendations for For Products

recommendUsersToItems = model.recommendUsersForProducts(10)

recommendUsersToItems.take(1)
[(185012,
(Rating(user=684514, product=185012, rating=8.711422605056984e-05),
Rating(user=350566, product=185012, rating=7.379856688516866e-05),
Rating(user=53842, product=185012, rating=6.039371941493027e-05),
Rating(user=286616, product=185012, rating=5.4088474732414744e-05),
Rating(user=163561, product=185012, rating=5.23055602343625e-05),
Rating(user=316850, product=185012, rating=4.563920688524824e-05),
Rating(user=757355, product=185012, rating=4.552637629254714e-05),
Rating(user=645525, product=185012, rating=4.469243732285364e-05),
Rating(user=518659, product=185012, rating=4.458737222271997e-05),
Rating(user=76757, product=185012, rating=3.33992858097255e-05)))]

recommendUsersToItems.count()
134109


# productFeatures

productFeatures = model.productFeatures().collect()

productFeatures_1 = sqlContext.createDataFrame(productFeatures)

model.userFeatures().collect()

productFeatures_1.show(3)
+---+--------------------+
| _1|                  _2|
+---+--------------------+
|  6|[-1.5856137906666...|
| 16|[-0.0010589133016...|
| 22|[1.45783760672202...|
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
|  0|[-1.5221920330077...|
|  2|[-0.0043335868977...|
+---+--------------------+


first_user = model.userFeatures().take(1)[0]

latents = first_user[1]

len(latents)
10

# Calculate rmse

predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2]))

type(predictions)
<class 'pyspark.rdd.PipelinedRDD'>

from math import sqrt

ratesAndPreds = CountRating.map(lambda r: ((r[0],r[1]), r[2])).join(predictions)

ratesAndPreds.take(5)
[((1214450, 221060), (1.0, 3.117905050205359e-09)),
((1158236, 273754), (1.0, 0.0008067676435537761)),
((1143818, 129032), (1.0, 1.1995722119950643e-05)),
((1038623, 78827), (1.0, 3.7521940423676754e-06)),
((529424, 212006), (1.0, 0.0025446120008257717))]

MeanSquareError = ratesAndPreds.map(lambda r: (r[1][0]-r[1][1])**2).mean()

print("MeanSquareError = " + str(MeanSquareError))

MeanSquareError = 1.65695238006

# Root Mean Square Error

from math import sqrt
rmse = sqrt(MeanSquareError)

print("Root_Mean_Square_Error = " + str(rmse))

Root_Mean_Square_Error = 1.28722662343


# 2.  Model Evluation and Selection with Hyper Parameter Tuning

df = sqlContext.createDataFrame(CountRating)

type(df)
<class 'pyspark.sql.dataframe.DataFrame'>

df.show(5)
+----+-------+------+
|user|product|rating|
+----+-------+------+
|1238|  76426|   1.0|
|2659| 444820|   1.0|
|2659| 293297|   1.0|
|2866|  90950|   1.0|
|2866| 294615|   1.0|
+----+-------+------+


# Split the input data into separate training and test data sets

(training, test) = df.randomSplit([0.8,0.2])

training.count()
705355

test.count()
176490

# Apply implicit parameter to the data
from pyspark.ml.recommendation import ALS

als = ALS(implicitPrefs=True, userCol="user", itemCol="product", ratingCol="rating", coldStartStrategy="drop")

als
ALS_44cc9b1d16cbfe4d4867


als.explainParams()
"alpha: alpha for implicit preference (default: 1.0)" \
\ncheckpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1)." \
E.g. 10 means that the cache will get checkpointed every 10 iterations. "
\Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext. (default: 10)
\ncoldStartStrategy: strategy for dealing with unknown or new users/items at prediction time.
This may be useful in cross-validation or production scenarios,
for handling user/item ids the model has not seen in the training data.
Supported values: 'nan', 'drop'. (default: nan, current: drop)
\nfinalStorageLevel: StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
\nimplicitPrefs: whether to use implicit preference (default: False, current: True)
\nintermediateStorageLevel: StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
\nitemCol: column name for item ids. Ids must be within the integer value range. (default: item, current: product)
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

# Fit the model

cvModel = cvImplicit.fit(training)


# Select the model produced by the best performance set of params

preds = cvModel.bestModel.transform(test)

evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')

evaluator
RegressionEvaluator_41e3aca241b084b69b8d

# Find Root Mean Square Error

rmse = evaluator.evaluate(preds)

print("Root-mean-square error =" +str(rmse))
Root-mean-square error =1.18395144634

