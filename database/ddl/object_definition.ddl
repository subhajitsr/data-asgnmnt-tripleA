-- Create the S3 stage
CREATE OR REPLACE STAGE stg_s3_loan_application
  URL = 's3://TEST001/loan-data/dump'
  CREDENTIALS = (
    AWS_KEY_ID = 'AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
  );

-- Create the core table
create or replace TABLE TESTDB.CORE.tbl_loan_application (
	serious_dlq_in_2yrs VARCHAR(5),
    revolving_util_of_unsecured_lines VARCHAR(20),
    age VARCHAR(1),
    num_of_time_30_59_days_past_due_not_worse VARCHAR(5),
    debt_ratio VARCHAR(30),
    monthly_income VARCHAR(20),
    num_of_open_cred_ln_n_loans VARCHAR(5),
    num_of_times_90days_late VARCHAR(5),
    num_real_estate_loans_or_lines VARCHAR(5),
    num_of_time_60_89_day_past_due_nt_worse VARCHAR(5),
    number_of_dependents VARCHAR(5),
    _file_name varchar(100),
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
cast(revolving_util_of_unsecured_lines as number(10,18)) as revolving_util_of_unsecured_lines ,
cast(age as integer) as age,
cast(num_of_time_30_59_days_past_due_not_worse as integer) as num_of_time_30_59_days_past_due_not_worse,
cast(debt_ratio as number(10,18)) as debt_ratio ,
case when trim(monthly_income)=='NA' then null else cast(monthly_income as number(15,0)) end as monthly_income,
cast(num_of_open_cred_ln_n_loans as integer) as num_of_open_cred_ln_n_loans,
cast(num_of_times_90days_late as integer) as num_of_times_90days_late,
cast(num_real_estate_loans_or_lines as integer) as num_real_estate_loans_or_lines,
cast(num_of_time_60_89_day_past_due_nt_worse as integer) as num_of_time_60_89_day_past_due_nt_worse,
case when trim(number_of_dependents)=='NA' then null else cast(number_of_dependents as integer) end as number_of_dependents,
_file_name,
_load_ts
FROM TESTDB.CORE.tbl_loan_application
;