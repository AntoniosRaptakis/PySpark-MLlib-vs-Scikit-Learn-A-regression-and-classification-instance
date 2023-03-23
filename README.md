Apache Spark is a very powerful tool used for Big data. It has been used by companies and universities. The role of this small tutorial is to show similarities/differences between the ML library of PySpark vs Pandas and Scikit-Learn, a conventional python library for machine learning, from the documentation to the results. 

There are two jupyter notebooks, one for pyspark and another one for scikit-learn. I follow the same steps in both cases: 

- Data Cleaning
- Data Preprocessing
- Correlations with the targets
- Features selection
- The Regression instances
- The Classification instance

In the scikit-learn notebook there is an EDA part.


**Working Scenario**:
A data scientist of a telecommunications company has been asked to predict the monthly & total cost for the services that a client has. The essential, however, would be to forecast the client who is going to resigns the contract.


This dataset can be found in kaggle(https://www.kaggle.com/datasets/blastchar/telco-customer-churn). IBM has also a similar dataset (https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=samples-telco-customer-churn).


- Python version: 3.8.8
- Spark verion: 3.2.1
- Anaconda verion: 1.7.2


Used libraries:
- Numpy
- Pandas
- Seaborn
- Matplotlib
- Scikit-learn
- Plotly
- Pyspark


### Read the dataset

##### With Pandas
<img width="600" alt="Dataset_pandas" src="https://user-images.githubusercontent.com/86191637/227224110-3059f4e8-ea2a-4be0-8254-ab15f809a9d6.png"/>

##### With PySpark
<img width="600" alt="Dataset_PySpark" src="https://user-images.githubusercontent.com/86191637/227226436-81a29685-36fe-4198-b168-30277a207bf7.png">


The features and their meaning:

- **customerID**: identity of the customer
- **gender**: what is the gender of the customer (Female/Male)
- **SeniorCitizen**: Is the customer a senior citizen? (0/1)
- **Partner**: Does the customer have a partner? (Yes/No)
- **Dependents**: Has the customer dependents? (Yes/No)
- **tenure**: Number of months the customer has stayed with the company
- **PhoneService**: Does the customer have a phone service? (Yes/ No)
- **MultipleLines**: Does the customer have multiple lines or not (Yes, No, No phone service)
- **InternetService**: Does the customer have multiple lines or not (Fiber, DSL, No)
- **OnlineSecurity**: Has the customer purchased online security option in the contract? (Yes/No/No internet service)
- **OnlineBackup**: Has the customer purchased the online back-up option in the contract? (Yes/No/No internet service)
- **DeviceProtection**: Has the customer purchased the device protection option in the contract? (Yes/No/No internet service)
- **TechSupport**: Has the customer purchased the technical support option in the contract (Yes/No/No internet service)
- **StreamingTV**: Has the customer purchased streaming TV option in the contract? (Yes/No/No internet service)
- **StreamingMovies**: Has the customer purchased streaming movies? (Yes/No/No internet service)
- **Contract**: What type of contract does the customer have? (Month-to-Month/One year/Two year)
- **PaperlessBilling**: Is the bill paperless? (Yes/No)
- **PaymentMethod**: what is the payment method? (Electronic check/Mailed check/Bank transfer/Credit card)
- **MonthlyCharges**: the monthly charges of the customer
- **TotalCharges**: the total charges of the customer
- **Churn**: Is the customer going to churn? (Yes/No)


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
<img width="750" alt="dublicates_pyspark" src="https://user-images.githubusercontent.com/86191637/227232662-6fb18d91-7370-4f71-ad2f-3793b4d7e7f6.png">


### Drop Nan values

##### With Pandas
Find the Nan values:

<img width="700" alt="find_na_pandas" src="https://user-images.githubusercontent.com/86191637/227233299-d0879747-3b43-45a2-a23a-96e64fb51e10.png">

Drop the Nan values:

<img width="300" alt="drop_na_pandas" src="https://user-images.githubusercontent.com/86191637/227233281-808556e8-2432-4649-a8f7-33ce82a1c0bf.png">



##### With PySpark
<img width="700" alt="drop_na_pyspark" src="https://user-images.githubusercontent.com/86191637/227233285-ae7ec0a1-d61f-4ada-b097-7c8e8b93aedc.png">


# Exploratory Data Analysis (EDA)
This part has been only implemented in the scikit-learn notebook.


<img width="700" alt="PhoneService_OnlineBackup_OnlineSecurity" src="https://user-images.githubusercontent.com/86191637/227262473-cf57ff90-6b20-4b3e-9094-70ddc44092d0.png">


<img width="700" alt="StreamingMovies_PhoneService_InternetService" src="https://user-images.githubusercontent.com/86191637/227262481-e652dfc1-539c-4edf-a44c-7148a77130d3.png">


<img width="700" alt="Partner_Dependents_InternetService" src="https://user-images.githubusercontent.com/86191637/227262464-4073f763-e870-4d79-a0fc-e18567777cd1.png">



<img width="700" alt="Contract_Churn" src="https://user-images.githubusercontent.com/86191637/227262444-a11d41e4-9105-4dce-92ed-bf9498b4f920.png">





# Data Preprocessing
Firstly, I drop the off the feature costumerID.


<img width="700" alt="preprocessing_scikit-learn" src="https://user-images.githubusercontent.com/86191637/227270156-bd1f527e-b037-4729-bf22-3d682d2c5a84.png">


<img width="700" alt="data_preprocessing_pyspark_1" src="https://user-images.githubusercontent.com/86191637/227267611-dd315960-afeb-46a0-bacb-e123617cae13.png">

<img width="700" alt="data_preprocessing_pyspark_2" src="https://user-images.githubusercontent.com/86191637/227267616-2d70e828-6cfd-4e5c-852e-3d7c63f4c07d.png">









# Correlations
The correlation with the target column in supervised learning is very essential. In a huge dataset, it can help us to choose specific columns and spend less computation time for the calculation. Below, I show the concept to find the correlation between all features and put it into a dataframe. I show here the PySpark script:

<img width="750" alt="road_to_correlation_matrix" src="https://user-images.githubusercontent.com/86191637/227242561-5067e08b-faff-44c9-8f61-f3541129c4cd.png">

Having the fataframe with all of the correlations between the features, I can visualize it with a matrix, by showing only the lower the diagonal elements:

<img width="700" alt="correlation_matrix_PySpark" src="https://user-images.githubusercontent.com/86191637/227242529-1a75fbcc-b573-4072-9a57-b1d609f37339.png">

I am mainly interested to see the correlation of specific features with the target. For example, I need the correlation with the MonthlyCharges. There are some features that I won't need for the regression later, so I can drop them. Then, I select the specific column and I visualize the results. Below, I show the PySpark script.

<img width="720" alt="how_to_find_the_correlation" src="https://user-images.githubusercontent.com/86191637/227242559-cd6ded92-0cd9-457f-9646-e23d292bd061.png">


The figure below shows the correlation between the specific target columns with the selected features each time.

##### For the regressions:

<p float="left">
  <img width="400" alt="MonthlyCharges" src="https://user-images.githubusercontent.com/86191637/227235877-03ce56f1-3ff9-4db5-bb2d-54a813a2c8dd.png">
  <img width="400" alt="TotalCharges" src="https://user-images.githubusercontent.com/86191637/227235888-f4869039-6356-49d4-a582-4124c241c7a1.png">
</p>


##### For the classification:

<img width="400" alt="Churn" src="https://user-images.githubusercontent.com/86191637/227235772-9c9f6402-cd8f-4524-9e28-0149ad7d95c6.png">


# Features selection



<img width="750" alt="new_datasets_scikit-learn" src="https://user-images.githubusercontent.com/86191637/227267646-ddf1617d-983b-4634-901d-8fc05b8edc12.png">

<img width="750" alt="MonthlyCharges_features_selection_pyspark" src="https://user-images.githubusercontent.com/86191637/227267634-b401c57c-2254-47ba-a53b-c89ada30cc9e.png">

<img width="750" alt="TotalCharges_features_selection_pyspark" src="https://user-images.githubusercontent.com/86191637/227267659-c3a1da14-1ad3-4168-81b6-8f1e2a992764.png">

<img width="750" alt="Churn_features_selection_pyspark" src="https://user-images.githubusercontent.com/86191637/227267596-79b997b0-24bd-4ac2-a956-6bf1455bac5e.png">

<img width="200" alt="how_pyspark_creates_the_dataset" src="https://user-images.githubusercontent.com/86191637/227267632-6156f461-23c5-4d47-ad56-1da4e6cba30c.png">



