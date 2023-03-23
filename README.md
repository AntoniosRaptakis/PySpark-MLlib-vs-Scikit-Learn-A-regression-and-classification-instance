Apache Spark is a very powerful tool used for Big data. It has been used by companies and universities. The role of this small tutorial is to show similarities/differences between the ML library of PySpark vs Pandas and Scikit-Learn, a conventional python library for machine learning, from the documentation to the results. 

There are two jupyter notebooks, one for pyspark and another one for scikit-learn. I follow the same steps in both cases: 

- Data Cleaning
- Data Preprocessing
- Correlations with the targets
- Two Regression instances
- A Classification instance

In the scikit-learn notebook there is an EDA part.


**Working Scenario**:
A data scientist of a telecommunications company has been asked to predict the monthly & total cost for the services that a client has. The essential, however, would be to forecast the client who is going to resigns the contract.


This dataset can be found in kaggle(https://www.kaggle.com/datasets/blastchar/telco-customer-churn). IBM has also a similar dataset (https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=samples-telco-customer-churn).


Python version: 3.8.8
Spark verion: 3.2.1
Anaconda verion: 1.7.2


### Read the dataset

##### With Pandas
<img width="600" alt="Dataset_pandas" src="https://user-images.githubusercontent.com/86191637/227224110-3059f4e8-ea2a-4be0-8254-ab15f809a9d6.png"/>

##### With PySpark
<img width="600" alt="Dataset_PySpark" src="https://user-images.githubusercontent.com/86191637/227226436-81a29685-36fe-4198-b168-30277a207bf7.png">

# Data Cleaning
##### Dataset basic information with Pandas
<img width="300" alt="info_pandas" src="https://user-images.githubusercontent.com/86191637/227229830-e2bf9f54-5f81-4f4b-bf7b-cb5a0e5e798e.png">

##### Information about the datatypes of the features with PySpark
<img width="300" alt="info_pyspark" src="https://user-images.githubusercontent.com/86191637/227229854-da1be59c-aaf7-407e-805c-d3a0b360a2da.png">

### Change of the data-types

##### With Pandas
<img width="700" alt="type_change_pandas" src="https://user-images.githubusercontent.com/86191637/227231802-24a882d4-9e3c-458d-9fa2-f1e92e923f1a.png">

##### With PySpark
<img width="700" alt="type_change_pyspark" src="https://user-images.githubusercontent.com/86191637/227231808-c7384785-102b-484e-8ebd-9fa957a03851.png">


### Duplicates

##### With Pandas
<img width="1064" alt="identify_duplicates_pandas" src="https://user-images.githubusercontent.com/86191637/227232679-ce3c9bdb-085a-432a-a4b4-2d8391994038.png">

##### With PySpark
<img width="994" alt="dublicates_pyspark" src="https://user-images.githubusercontent.com/86191637/227232662-6fb18d91-7370-4f71-ad2f-3793b4d7e7f6.png">


### Drop Nan values

##### With Pandas
Find the Nan values:

<img width="700" alt="find_na_pandas" src="https://user-images.githubusercontent.com/86191637/227233299-d0879747-3b43-45a2-a23a-96e64fb51e10.png">

Drop the Nan values:

<img width="300" alt="drop_na_pandas" src="https://user-images.githubusercontent.com/86191637/227233281-808556e8-2432-4649-a8f7-33ce82a1c0bf.png">



##### With PySpark
<img width="500" alt="drop_na_pyspark" src="https://user-images.githubusercontent.com/86191637/227233285-ae7ec0a1-d61f-4ada-b097-7c8e8b93aedc.png">



# Data Preprocessing
Firstly, I drop the off the feature costumerID.




# Correlations
The correlation with the target column in supervised learning is very essential. In a huge dataset, it can help us to choose specific columns and spend less computation time for the calculation. Below, I show the concept to find the correlation between all features and put it into a dataframe. I show here the PySpark script:

<img width="850" alt="road_to_correlation_matrix" src="https://user-images.githubusercontent.com/86191637/227242561-5067e08b-faff-44c9-8f61-f3541129c4cd.png">

One can visualize dataframe with a matrix, by showing only the lower the diagonal elements.

<img width="700" alt="correlation_matrix_PySpark" src="https://user-images.githubusercontent.com/86191637/227242529-1a75fbcc-b573-4072-9a57-b1d609f37339.png">

How can I find the correlation of all features with the target?

<img width="800" alt="how_to_find_the_correlation" src="https://user-images.githubusercontent.com/86191637/227242559-cd6ded92-0cd9-457f-9646-e23d292bd061.png">

And I can visualize for all 

###### Correlation between columns with
<p float="left">
  <img width="333" alt="MonthlyCharges" src="https://user-images.githubusercontent.com/86191637/227235877-03ce56f1-3ff9-4db5-bb2d-54a813a2c8dd.png">
  <img width="333" alt="TotalCharges" src="https://user-images.githubusercontent.com/86191637/227235888-f4869039-6356-49d4-a582-4124c241c7a1.png">
  <img width="333" alt="Churn" src="https://user-images.githubusercontent.com/86191637/227235772-9c9f6402-cd8f-4524-9e28-0149ad7d95c6.png">
</p>



