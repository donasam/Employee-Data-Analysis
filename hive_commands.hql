use donaalab;

drop table if exists d_Departments;
CREATE EXTERNAL TABLE d_Departments STORED AS AVRO LOCATION '/user/anabig114242/hive/warehouse/d_Departments' TBLPROPERTIES ('avro.schema.url'='/user/anabig114242/capstone1_dona/d_Departments.avsc');

drop table if exists d_Employees;
CREATE EXTERNAL TABLE d_Employees STORED AS AVRO LOCATION '/user/anabig114242/hive/warehouse/d_Employees' TBLPROPERTIES ('avro.schema.url'='/user/anabig114242/capstone1_dona/d_Employees.avsc');

drop table if exists d_Department_Employees;
CREATE EXTERNAL TABLE d_Department_Employees STORED AS AVRO LOCATION '/user/anabig114242/hive/warehouse/d_Department_Employees' TBLPROPERTIES ('avro.schema.url'='/user/anabig114242/capstone1_dona/d_Department_Employees.avsc');

drop table if exists d_Department_Managers;
CREATE EXTERNAL TABLE d_Department_Managers STORED AS AVRO LOCATION '/user/anabig114242/hive/warehouse/d_Department_Managers' TBLPROPERTIES ('avro.schema.url'='/user/anabig114242/capstone1_dona/d_Department_Managers.avsc');

drop table if exists d_Salaries;
CREATE EXTERNAL TABLE d_Salaries STORED AS AVRO LOCATION '/user/anabig114242/hive/warehouse/d_Salaries' TBLPROPERTIES ('avro.schema.url'='/user/anabig114242/capstone1_dona/d_Salaries.avsc');

drop table if exists d_Titles;
CREATE EXTERNAL TABLE  d_Titles STORED AS AVRO LOCATION '/user/anabig114242/hive/warehouse/ d_Titles' TBLPROPERTIES ('avro.schema.url'='/user/anabig114242/capstone1_dona/ d_Titles.avsc');

create view employee_date as
select emp_no,first_name,last_name,sex,no_of_projects,last_performance_rating,left1,
cast(to_date(from_unixtime(unix_timestamp(hire_date,'dd/mm/yyyy'))) as date) hire_date,
cast(to_date(from_unixtime(unix_timestamp(birth_date,'dd/mm/yyyy'))) as date) birth_date,
cast(to_date(from_unixtime(unix_timestamp(hire_date,'dd/mm/yyyy'))) as date) last_date
from d_employees;
