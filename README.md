YelpDataSetChallenge
====================

Class Project for Information Retrieval. Two tasks are done as part of challenge. 

Task 1: Assigne Categories to different business in Yelp Data Set
Task 2: Recommend Liked Dishes and disliked Dishes using reviews and tips from Yelp DataSet


@AUTHORS: Bipra De, Nihar Khetan, Anand Sharma, Satvik Shetty
@COLLABORATOR: Professor Xiazhong Liu

Semester Project For
ILS Z 534 - Information Retreival  
Indiana University Bloomington

Description
====================

Usage and details
====================
JavaClasses and their functionality:
------------------------------------
CreateTrainingAndTestCollections.java - Reads Data from given Yelp Dataset and Created two collections (training and test) in MongoDB 
generateIndex.java - Reads data from MongoDB and creates Training and Test Lucene index
FeatureSetExtractor -  Reads data from Lucene Training Index and extract top features for a category. It also dumps them to MongoDB
CategorySimilatityComparer - finds out similar categories on the basis of a threshold. For example: given category1 and category2 with 
			     some feature set and threshold as 70%, category1 is supposed to contain category2 is featureset(category1) and
			     featureset(category2) are 70% similar
AssignCategories.java - Reads data from Lucene Test Index and assign categories to them. It is alos capable of assignning multiple categories to a business			   
MeasurePerformance.java - Reads computed resuls from MongoDB and output the results to a file using evaluation metrics

 MongoDB collections:
 --------------------
 test_set: Dump of test data
 training_set: Dump of training data
 feature_set: Categories and their top features
 categories_assigned_from_code: Businesses which are assigned new categories by code
 
Project Page
====================

Work Under Progress:

Ping us at nkhetan@indiana.edu if you wish to appreciate/criticize/contribute to the project

Bipra De -
Satvik Shetty -
Anand Sharma - 
