Apache Spark is a very powerful tool used for Big data. It has been used by companies and universities in order to manipulate big data and build a wareghouse. The role of this small tutorial/article is to show similarities/differences between the ML library of PySpark vs Scikit-Learn, a conventional python library for machine learning, from the documentation to the results. 

There are two jupyter notebooks, one for pyspark and another one for scikit-learn. I follow the same steps in both cases. These steps are: 

- Data Cleaning
- Data Preprocessing
- Two Regression instances
- A Classification instance

In the scikit-learn notebook there is an EDA part.


**Working Scenario**:
A data scientist of a telecommunications company has been asked to predict the monthly & total cost for the services that a client has. The essential, however, would be to forecast the client who is going to resigns the contract.


This dataset can be found in kaggle(https://www.kaggle.com/datasets/blastchar/telco-customer-churn). IBM has also a similar dataset (https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=samples-telco-customer-churn).

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

##### With PySpark


<p float="left">
  <img width="413" alt="info_pandas" src="https://user-images.githubusercontent.com/86191637/227229830-e2bf9f54-5f81-4f4b-bf7b-cb5a0e5e798e.png">
  <img width="446" alt="info_pyspark" src="https://user-images.githubusercontent.com/86191637/227229854-da1be59c-aaf7-407e-805c-d3a0b360a2da.png">
</p>



