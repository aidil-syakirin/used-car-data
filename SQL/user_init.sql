-- set the default scheam for the user for this project
ALTER USER spark_kirin WITH DEFAULT_SCHEMA = used_car_data;

-- need to use admin role for these commands
-- create a role for the schema permissions

CREATE ROLE used_car_data_rw;

-- grant read, write, update and delete permissions on the schema to the created role

GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::used_car_data TO used_car_data_rw;

-- azure sql doesnot support grant create table at schema level. need to grant alter permission for the role
GRANT ALTER ON SCHEMA::used_car_data TO used_car_data_rw;

-- grant the refrences and create table at database level
GRANT REFERENCES ON SCHEMA::used_car_data TO your_user_or_role;
GRANT CREATE TABLE TO your_user_or_role;


-- assign the role to the user
ALTER ROLE used_car_data_rw ADD MEMBER spark_kirin;


-- example of other necessary commands
-- revoke permssions by dropping the role from the USER

-- ALTER ROLE used_car_data_rw DROP MEMBER spark_kirin;

-- grant read-access to a user (for dashboard app or other reader)
-- GRANT SELECT ON SCHEMA::used_car_data TO read_only_user;

-- verify if the user has the role
-- SELECT 
--     m.name AS role_name, 
--     p.name AS user_name 
-- FROM sys.database_role_members rm
-- JOIN sys.database_principals m ON rm.role_principal_id = m.principal_id
-- JOIN sys.database_principals p ON rm.member_principal_id = p.principal_id
-- WHERE p.name = 'spark_kirin';
