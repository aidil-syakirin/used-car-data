
-- create a role for read, write and update operation of airflow operator
CREATE USER [sp-airflow-sql] FROM EXTERNAL PROVIDER;

-- assign the role to the user
ALTER ROLE used_car_data_rw ADD MEMBER [sp-airflow-sql];