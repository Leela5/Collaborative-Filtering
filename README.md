### Collaborative-Filtering on implicit Ecommerce' users' behavirol data 
It has two files like events and item_properties.
Made four diffrent MatrixFactorizationModels based on data availability.
Applied ALS algorithms on different matrixFactorizationModels to find Top N predictions for users and products.

Step_1: Data Cleaning and Preprocessing 

Model_1: For this model, latest category across items is derived and joined with item properties to derive the item-category combinations and the output is joined with events data to recommend top N categories based on user preferences. The matrix factorization variables for this model are visitorid , categoryid and event with distinct values. 

Model_2: Visitors having more than one event is considered for this model and matrix factorization variables for this model are ‘visitor ID’, ‘itemid’ and ‘event’.Here we considered events data only.

Model_3: Visitors having more than one event is considered for this model, with distinct values considered for matrix factorization across VisitorID, ItemID and Event. Here we considered events data only.

Model_4: Visitors having more than two events is considered for this model, with distinct values considered for matrix factorization across VisitorID, ItemID and Event. Here we considered events data only.

Step_2: RS’ Collaborative Filtering –RDD based API.

Each Model in this project follows the same steps in order to recommend the top N items based on user preferences as well as top N users based on item preferences.

Step_3: Model selection and hyper parameter tuning.

Model selection or model evaluation is used to find the optimal parameters. Spark provides the number of algorithms to tune the model evaluation for either individual algorithms or the entire model building pipeline.  Here, we used CrossValidator and TrainValidationSplit classes.

Step_4: Model Evaluation by using Ranking System.

The main goal is to measure the effectiveness of recommendations



Data source : https://www.kaggle.com/retailrocket/ecommerce-dataset
