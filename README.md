## Data Description

This dataset is part of the Give Me Some Credit Kaggle competition. The data contains loan applicant information collected by a US credit bureau. Each row represents a single loan application and the information gathered on the applicant at the time of the application.

## Variable dictionary
Below is a list of the columns included in the dataset and their meaning.

| Variable Name | Description | Type |
|---------------|-------------|------|
|SeriousDlqin2yrs|Person experienced 90 days past due delinquency or worse.|Y/N|
|RevolvingUtilizationOfUnse curedLines|Total balance on credit cards and personal lines of credit except real estate and installment debt (e.g. car loans) divided by the sum of credit limits.|Percentage|
|age|Age of borrower in years|Integer|
|NumberOfTime30-59Days PastDueNotWorse|Number of times borrower has been 30-59 days past due but no worse in the last 2 years.|Integer|
|DebtRatio|Monthly debt payments, alimony, and living costs divided by monthly gross income.|Percentage|
|MonthlyIncome|Monthly income.|Dollars|
|NumberOfOpenCreditLines AndLoans|Number of open loans (e.g. car loan, mortgage) and lines of credit (e.g. credit cards).|Integer|
|NumberOfTimes90DaysLate|Number of times borrower has been 90 days or more past due.|Integer|
|NumberRealEstateLoansO rLines|Number of mortgage and real estate loans including home equity lines of credit.|Integer|
|NumberOfTime60-89Days PastDueNotWorse|Number of times borrower has been 60-89 days past due but no worse in the last 2 years.|Integer|
|NumberOfDependents|Number of dependents in family excluding applicant (spouse, children, etc...).|Integer|

## Data Pipeline Solution hint:
1. The solution is built to be run in an Airflow scheduler. the `loan-application-core-loader.py` file in the 'dags' folder needs to be placed in the `airflow/dags` directory of the instance.
2. All the necessary Pypi packages are mentioned in the `requirements.txt`. Same needs to be installed in the AIrflow instance before the dag deployment.
3. Need to set the AWS/Snowflake connectivity details in the Airflow variable as below,
   - `SF_USERNAME`: Snowflake DB username
   - `SF_PASSWORD`: Password for the username
   - `SF_ACCOUNT`: Snowflake account name
   - `AWS_ACCESS_KEY`: Access key for AWS connectivity for S3 access
   - `AWS_SECRET_KEY`: Secret key for the access key
4. Execute the DDLs (`database/object_definition.ddl`) in the Snowflake database where the data is intended to be landed.
5. Create the `/inbox/loan-data` and `/archive/loan-data` directory in the Airflow local folder.
6. Create the bucket named `TEST001` in AWS S3 datalake.
7. Under the above bucket create the path, `loan-data/dump`
