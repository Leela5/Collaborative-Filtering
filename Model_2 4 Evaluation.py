# Load Category data

data = sc.textFile('/user/maria_dev/CountWiseVisitorTime.csv')

data.first()
u'visitorid,categoryid,event'

data.count()
1754083

header = data.first()

df= data.filter(lambda x: x!=header)

df.count()
1754082

# Ranking systems evaluation

from pyspark.mllib.recommendation import ALS, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics

def parseLine(line):
    fields = line.split(',')
    return Rating(int(fields[0]), int(fields[1]), float(fields[2]) - 1.5)
ratings = df.map(lambda r: parseLine(r))

# Train a model on to predict user-product ratings
model = ALS.train(ratings, 10, 10, 0.01)

# Get predicted ratings on all existing user-product pairs
testData = ratings.map(lambda p: (p.user, p.product))

predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))

ratingsTuple = ratings.map(lambda r: ((r.user, r.product), r.rating))

ratingsTuple.first()
((496, 269803), -0.5)

scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])

scoreAndLabels.take(2)
[(-0.4941928189181514, -0.5), (-0.4941928189181514, -0.5)]

# Instantiate regression metrics to compare predicted and actual ratings
metrics = RegressionMetrics(scoreAndLabels)

# Root mean squared error
print("RMSE = %s" % metrics.rootMeanSquaredError)

RMSE = 0.264504154662

# R-squared
print("R-squared = %s" % metrics.r2)

R-squared = 0.379424878877