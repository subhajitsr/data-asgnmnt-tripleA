create database TESTDB;
use database TESTDB;
create schema CORE;
create schema SEMANTIC;

-- Create the S3 stage
CREATE OR REPLACE STAGE stg_s3_loan_application
  URL = 's3://loan-test001/loan-data/dump/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
  );

-- Create the core table
create or replace TABLE TESTDB.CORE.tbl_loan_application (
	serious_dlq_in_2yrs VARCHAR(50),
    revolving_util_of_unsecured_lines VARCHAR(20),
    age VARCHAR(30),
    num_of_time_30_59_days_past_due_not_worse VARCHAR(50),
    debt_ratio VARCHAR(30),
    monthly_income VARCHAR(20),
    num_of_open_cred_ln_n_loans VARCHAR(50),
    num_of_times_90days_late VARCHAR(50),
    num_real_estate_loans_or_lines VARCHAR(50),
    num_of_time_60_89_day_past_due_nt_worse VARCHAR(50),
    number_of_dependents VARCHAR(50),
    _file_name varchar(500),
    _load_ts timestamp_ntz(0)
);

-- Create core view
create or replace view TESTDB.CORE.vw_loan_application(
serious_dlq_in_2yrs,
revolving_util_of_unsecured_lines,
age,
num_of_time_30_59_days_past_due_not_worse,
debt_ratio,
monthly_income,
num_of_open_cred_ln_n_loans,
num_of_times_90days_late,
num_real_estate_loans_or_lines,
num_of_time_60_89_day_past_due_nt_worse,
number_of_dependents,
_file_name,
_load_ts
) as
SELECT
cast(serious_dlq_in_2yrs as integer) as serious_dlq_in_2yrs,
to_number(revolving_util_of_unsecured_lines,18,10) as revolving_util_of_unsecured_lines ,
cast(age as integer) as age,
cast(num_of_time_30_59_days_past_due_not_worse as integer) as num_of_time_30_59_days_past_due_not_worse,
cast(debt_ratio as number(18,10)) as debt_ratio ,
case when trim(monthly_income)='NA' then null else cast(monthly_income as decimal(15,0)) end as monthly_income,
case when trim(num_of_open_cred_ln_n_loans)='NA' then null else cast(num_of_open_cred_ln_n_loans as integer) end as num_of_open_cred_ln_n_loans,
cast(num_of_times_90days_late as integer) as num_of_times_90days_late,
cast(num_real_estate_loans_or_lines as integer) as num_real_estate_loans_or_lines,
cast(num_of_time_60_89_day_past_due_nt_worse as integer) as num_of_time_60_89_day_past_due_nt_worse,
case when trim(number_of_dependents)='NA' then null else cast(number_of_dependents as integer) end as number_of_dependents,
_file_name,
_load_ts
FROM TESTDB.CORE.tbl_loan_application
;

-- Create the Semantic table
create or replace TABLE TESTDB.SEMANTIC.tbl_loan_application (
	serious_dlq_in_2yrs VARCHAR(50),
    revolving_util_of_unsecured_lines VARCHAR(20),
    age VARCHAR(30),
    num_of_time_30_59_days_past_due_not_worse VARCHAR(50),
    debt_ratio VARCHAR(30),
    monthly_income VARCHAR(20),
    num_of_open_cred_ln_n_loans VARCHAR(50),
    num_of_times_90days_late VARCHAR(50),
    num_real_estate_loans_or_lines VARCHAR(50),
    num_of_time_60_89_day_past_due_nt_worse VARCHAR(50),
    number_of_dependents VARCHAR(50),
    _file_name varchar(500),
    _load_ts timestamp_ntz(0)
);

-- Create Semantic view
create or replace view TESTDB.SEMANTIC.vw_loan_application(
serious_dlq_in_2yrs,
revolving_util_of_unsecured_lines,
age,
num_of_time_30_59_days_past_due_not_worse,
debt_ratio,
monthly_income,
num_of_open_cred_ln_n_loans,
num_of_times_90days_late,
num_real_estate_loans_or_lines,
num_of_time_60_89_day_past_due_nt_worse,
number_of_dependents,
_file_name,
_load_ts
) as
select
serious_dlq_in_2yrs,
revolving_util_of_unsecured_lines,
age,
num_of_time_30_59_days_past_due_not_worse,
debt_ratio,
monthly_income,
num_of_open_cred_ln_n_loans,
num_of_times_90days_late,
num_real_estate_loans_or_lines,
num_of_time_60_89_day_past_due_nt_worse,
number_of_dependents,
_file_name,
_load_ts
from TESTDB.SEMANTIC.tbl_loan_application;



-- Create Analysis view with Measure and dimensions
create or replace view TESTDB.SEMANTIC.loan_application_agg(
age_group,
num_of_dependents_group,
monthly_income_group,
serious_dlq_in_2yrs,
avg_debt_ratio,
sum_num_of_open_cred_ln_n_loans,
sum_num_real_estate_loans_or_lines,
avg_revolving_util_of_unsecured_lines,
sum_30_59_days_past_due_not_worse,
sum_60_89_day_past_due_nt_worse,
sum_90days_late
) as
select
case when age<20 then 'teenagers'
    when age >=20 and age<30 then '20s'
    when age >=30 and age<40 then '30s'
    when age >=40 and age<50 then '40s'
    when age >=50 and age<60 then '50s'
    when age >=60 and age<70 then '60s'
    when age >=70 then 'Senior Citizen'
    else 'Unknown' end as age_group,
case when coalesce(number_of_dependents,0) = 0 then 'No Dependents'
    when coalesce(number_of_dependents,0) = 1 then 'One'
    when coalesce(number_of_dependents,0) = 2 then 'Two'
    when coalesce(number_of_dependents,0) between 3 and 4 then 'Three to Four'
    when coalesce(number_of_dependents,0) > 4 then 'More than Four' end
    as num_of_dependents_group,
case when monthly_income < 1000 then '<1k'
     when monthly_income >= 1000 and monthly_income<3000 then '1k-3k'
     when monthly_income >= 3000 and monthly_income<5000 then '3k-5k'
     when monthly_income >= 5000 and monthly_income<7000 then '5k-7k'
     when monthly_income >= 7000 and monthly_income<10000 then '7k-10k'
     when monthly_income >= 10000 and monthly_income<15000 then '10k-15k'
     when monthly_income >= 15000 and monthly_income<20000 then '15k-20k'
     when monthly_income >= 20000 then '>20k'
     else 'Unknown' end as monthly_income_group,
serious_dlq_in_2yrs,
avg(coalesce(debt_ratio,0)) as avg_debt_ratio,
sum(coalesce(num_of_open_cred_ln_n_loans,0)) as sum_num_of_open_cred_ln_n_loans,
sum(coalesce(num_real_estate_loans_or_lines,0)) as sum_num_real_estate_loans_or_lines,
avg(coalesce(revolving_util_of_unsecured_lines,0)) as avg_revolving_util_of_unsecured_lines,
sum(coalesce(num_of_time_30_59_days_past_due_not_worse,0)) as sum_30_59_days_past_due_not_worse,
sum(coalesce(num_of_time_60_89_day_past_due_nt_worse,0)) as sum_60_89_day_past_due_nt_worse,
sum(coalesce(num_of_times_90days_late,0)) as sum_90days_late
from TESTDB.SEMANTIC.vw_loan_application
group by age_group,monthly_income_group,number_of_dependents,serious_dlq_in_2yrs
;