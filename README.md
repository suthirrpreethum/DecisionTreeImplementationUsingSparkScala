# Decision Tree Implementation Using Spark Scala

Decsion tree is generated with Entropy as impurity measure

The CSV file is taken as input and splitted into training and testing data.
The decision tree is created for the training data in a recursive approach for numerical data countaining 30 features.

After creating the decsion tree, The testing data is checked with the condition of the tree created and the confusion matrix is generated.
The Accuracy is calculated with the help of generated confusion matrix.

Accurary=(TP+TN)/(TP + TN + FP + FN)
