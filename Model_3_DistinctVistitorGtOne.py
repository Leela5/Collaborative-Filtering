### Recommend a model on count who as more than 1 events (view, addtocart, transaction)

# Events data

events = spark.read.options(header=True, inferSchema=True).csv('/user/capstone/events.csv')
events.count()
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

visitorGt1 = visitorGroupBy.filter(visitorGroupBy['count']>1)

visitorGt1.count()
406020

eventsJoinVisitorGt1 = events.join(visitorGt1, ['visitorid'])

eventsJoinVisitorGt1.count()
1754541


eventsJoinVisitorGt1.show(5)
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


eventsJoinVisitorGt1Distinct.show(5)
+---------+-------------+-----+------+-------------+-----+
|visitorid|    timestamp|event|itemid|transactionid|count|
+---------+-------------+-----+------+-------------+-----+
|      496|1438745738297| view|269803|         null|    2|
|      496|1439922144484| view|395920|         null|    2|
|     1238|1438970280581| view| 76426|         null|    3|
|     1238|1438970177401| view| 76426|         null|    3|
|     1238|1438967704727| view| 76426|         null|    3|
+---------+-------------+-----+------+-------------+-----+


# Assign values to event wise

viewreplace = eventsJoinVisitorGt1Distinct.replace({'view':'1'})

addtocartreplace = viewreplace.replace({'addtocart':'2'})

final = addtocartreplace.replace({'transaction':'3'})


# Extract the columns for MatrixFactorization for ALS

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

# distinct final data to avoid redudence of data

data = data_1.distinct()

data.count()
1213862

data.show(5)
+---------+------+-----+
|visitorid|itemid|event|
+---------+------+-----+
|      496|269803|    1|
|      496|395920|    1|
|     1238| 76426|    1|
|     1580| 29100|    1|
|     1645|  6432|    1|
+---------+------+-----+


#  Save the data file into hdfs

data.coalesce(1).write.option('header','true').csv('/user/capstone/CountGt1DistinctVisitors.csv')

## Building Recommendation model by using ALS

# Creating RDD file

# option A Text data
# from pyspark.sql import Row
# lines = spark.read.text('/user/maria_dev/CountWiseVisitor.csv').rdd
# parts = lines.map(lambda row: row.value.split(','))
# CountRating = parts.map(lambda p: Row(visitorid=int(p[0]),itemid=int(p[1]),event=float(p[2])))

# option B .csv file

dataGt1 = sc.textFile('/user/maria_dev/CountGt1DistinctVisitors.csv')

type(data)
<class 'pyspark.sql.dataframe.DataFrame'>


dataGt1.count()
1213863

dataGt1.first()
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

type(CountRating)
<class 'pyspark.rdd.PipelinedRDD'>

# create data frame CountRating

df = sqlContext.createDataFrame(CountRating)
df.count()
1213862

# Data exploration on Frequency Visitor

# Distinct Users

df.select('user').distinct().show(5)
+------+
|  user|
+------+
| 15846|
|160820|
|342088|
|652700|
|703028|
+------+


# Distinct Users's count

df.select('user').distinct().count()
406020

# Distinct Products

df.select('product').distinct().show(5)
+-------+
|product|
+-------+
| 186376|
| 241022|
|  55474|
| 421024|
|  43367|

# GroupBy User
df.groupBy('user').count().take(5)
[Row(user=15846, count=2),
Row(user=160820, count=4),
Row(user=342088, count=2),
Row(user=652700, count=20),
Row(user=703028, count=2)]

# GroupBy User stats

df.groupBy('user').count().select('count').describe().show()
+-------+------------------+
|summary|             count|
+-------+------------------+
|  count|            406020|
|   mean|  2.98966060785183|
| stddev|15.823784488161328|
|    min|                 1|
|    max|              4964|
+-------+------------------+


df.stat.crosstab('user','rating').show(5)
+-----------+---+---+---+
|user_rating|1.0|2.0|3.0|
+-----------+---+---+---+
|    1253013|  3|  0|  0|
|     895344|  2|  0|  0|
|     999043|  2|  0|  0|
|     921558|  2|  0|  0|
|    1042949|  4|  0|  0|
+-----------+---+---+---+


# Splitting the CountRating data

(training,test) = CountRating.randomSplit([0.8,0.2])

training.count()
971322

test.count()
242540

# 1. Build the recommendation model using Alternating Least Squares

# Call the ALS.train mehod to train the model

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

rank = 10
numIterations = 10
model = ALS.train(training, rank, numIterations)

model
<pyspark.mllib.recommendation.MatrixFactorizationModel object at 0x7f5b0267a850>

# Evaluate the model on training data

testdata = test.map(lambda r: (r[0],r[1]))

type(testdata)


testdata.take(5)
[(496, 269803), (2866, 83908), (3997, 158114), (4101, 115244), (4101, 376365)]

# Making predictions

pred_ind = model.predict(1238, 76426)

pred_ind
0.0019262091676213755


model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2])).take(5)
[((182316, 141578), 1.0882278222742986e-09),
((182316, 453394), 4.328501229010258e-07),
((1339720, 158498), 1.6347024624925272e-06),
((1133078, 130865), 0.004666932087039164),
((449586, 412676), 4.384616472672641e-06)]


# Recommend Products For Users method to generate the top N item recommendations for users

recommendItemsToUsers = model.recommendProductsForUsers(10)

recommendItemsToUsers.take(1)
[(637100,
(Rating(user=637100, product=38965, rating=0.0026915312215843707),
Rating(user=637100, product=234255, rating=0.0021679220537586703),
Rating(user=637100, product=241555, rating=0.0021358531506244453),
Rating(user=637100, product=9877, rating=0.001998215137496734),
Rating(user=637100, product=325310, rating=0.0017399707527331987),
Rating(user=637100, product=420960, rating=0.001638734455105893),
Rating(user=637100, product=206241, rating=0.0015982654318399594),
Rating(user=637100, product=22926, rating=0.0015734659137447548),
Rating(user=637100, product=290999, rating=0.0015383782746477709),
Rating(user=637100, product=60980, rating=0.0015096630750091027)))]


recommendItemsToUsers.count()
166753


# Recommend Users For Products method to generate the top N Users recommendations for For Products

recommendUsersToItems = model.recommendUsersForProducts(10)

recommendUsersToItems.take(1)
[(185012,
(Rating(user=163561, product=185012, rating=8.791158823074979e-05),
Rating(user=286616, product=185012, rating=8.49404700178371e-05),
Rating(user=138131, product=185012, rating=6.983311817191418e-05),
Rating(user=53842, product=185012, rating=4.8477522615636893e-05),
Rating(user=389532, product=185012, rating=4.062623982005371e-05),
Rating(user=350566, product=185012, rating=3.985964789837738e-05),
Rating(user=227091, product=185012, rating=3.083768228187945e-05),
Rating(user=530559, product=185012, rating=3.047925132225478e-05),
Rating(user=809822, product=185012, rating=2.903318886163379e-05),
Rating(user=79627, product=185012, rating=2.7232511522191876e-05)))]



recommendUsersToItems.count()
406020



# productFeatures

productFeatures = model.productFeatures().collect()

productFeatures_1 = sqlContext.createDataFrame(productFeatures)

model.userFeatures().collect()

productFeatures_1.show(3)
+---+--------------------+
| _1|                  _2|
+---+--------------------+
|  6|[-1.1539863771758...|
| 16|[-0.0018911607330...|
| 22|[5.15686315338825...|
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
|  0|[-0.0011578487465...|
|  2|[-0.0034697726368...|
+---+--------------------+


# Calculate rmse

predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]),r[2]))

type(predictions)
<class 'pyspark.rdd.PipelinedRDD'>

from math import sqrt

ratesAndPreds = CountRating.map(lambda r: ((r[0],r[1]), r[2])).join(predictions)

ratesAndPreds.take(5)
[((1165148, 85774), (1.0, 0.08495247098147887)),
((836198, 167880), (1.0, 1.7943257177058e-07)),
((224323, 126335), (1.0, 5.192126618937253e-07)),
((462622, 143096), (1.0, 7.421782758725422e-06)),
((1038623, 78827), (1.0, 3.772172403236311e-06))]


MeanSquareError = ratesAndPreds.map(lambda r: (r[1][0]-r[1][1])**2).mean()

print("MeanSquareError = " + str(MeanSquareError))
MeanSquareError = 1.52786336687

# Root Mean Square Error

from math import sqrt
rmse = sqrt(MeanSquareError)

print("Root_Mean_Square_Error = " + str(rmse))
Root_Mean_Square_Error = 1.23606770319



# 2.  Model Evluation and Selection with Hyper Parameter Tuning

df = sqlContext.createDataFrame(CountRating)

type(df)

df.show(5)



# Split the input data into separate training and test data sets

(training, test) = df.randomSplit([0.8,0.2])

training.count()
970800

test.count()
243062

# Apply implicit parameter to the data
from pyspark.ml.recommendation import ALS

als = ALS(implicitPrefs=True, userCol="user", itemCol="product", ratingCol="rating", coldStartStrategy="drop")

als
ALS_4a72ade28fdabf7f4090


als.explainParams()
"alpha: alpha for implicit preference (default: 1.0)" \
\ncheckpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1).
E.g. 10 means that the cache will get checkpointed every 10 iterations. Note:
this setting will be ignored if the checkpoint directory is not set in the SparkContext. (default: 10)
\ncoldStartStrategy: strategy for dealing with unknown or new users/items at prediction time.
This may be useful in cross-validation or production scenarios,
for handling user/item ids the model has not seen in the training data.
Supported values: 'nan', 'drop'. (default: nan, current: drop)
\nfinalStorageLevel: StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
\nimplicitPrefs: whether to use implicit preference (default: False, current: True)
\nintermediateStorageLevel: StorageLevel for intermediate datasets.
Cannot be 'NONE'. (default: MEMORY_AND_DISK)\nitemCol: column name for item ids.
Ids must be within the integer value range. (default: item, current: product)
\nmaxIter: max number of iterations (>= 0). (default: 10)\nnonnegative:
whether to use nonnegative constraint for least squares (default: False)
\nnumItemBlocks: number of item blocks (default: 10)\nnumUserBlocks: number of user blocks (default: 10)
\npredictionCol: prediction column name. (default: prediction)\nrank: rank of the factorization (default: 10)
\nratingCol: column name for ratings (default: rating, current: rating)
\nregParam: regularization parameter (>= 0). (default: 0.1)
\nseed: random seed. (default: 593367982098446717)
\nuserCol: column name for user ids. Ids must be within the integer value range. (default: user, current: user)"

from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[als])

type(pipeline)

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

preds.take(1)
[Row(user=77390, product=496, rating=1.0, prediction=0.00019151998276356608)]

evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')

evaluator
RegressionEvaluator_4ad9a3786aa744000d6d

# Find Root Mean Square Error

rmse = evaluator.evaluate(preds)

print("Root-mean-square error =" +str(rmse))
Root-mean-square error =1.15712494152
