mysql -u anabig114246 -pBigdata123

USE anabig114242;

create table d_Titles(
title_id varchar(10),
title varchar(30) NOT NULL);

create table d_Employees(
emp_no int NOT NULL,
emp_title_id varchar(10) NOT NULL,
birth_date varchar(10) NOT NULL,
first_name varchar(20) NOT NULL,
last_name varchar(20) NOT NULL,
sex varchar(5) NOT NULL,
hire_date varchar(10) NOT NULL,
no_of_projects int NOT NULL,
last_performance_rating varchar(10) NOT NULL,
left1 int NOT NULL,
last_date varchar(10));

create table d_Salaries(
emp_no int NOT NULL,
salary bigint NOT NULL);

create table d_Departments(
dept_no varchar(20) NOT NULL ,
dept_name varchar(30) NOT NULL);

CREATE TABLE d_Department_Employees(
emp_no int NOT NULL,
dept_no varchar(20) NOT NULL); 

create table d_Department_Managers(
dept_no varchar(20) NOT NULL,
emp_no int NOT NULL);


load data local infile '/home/anabig114242/dept_emp.csv' into table d_Department_Employees
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114242/dept_manager.csv' into table d_Department_Managers
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114242/departments.csv' into table d_Departments
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114242/employees.csv' into table d_Employees
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114242/salaries.csv' into table d_Salaries
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114242/titles .csv' into table d_Titles
fields terminated by ','
ignore 1 rows;
