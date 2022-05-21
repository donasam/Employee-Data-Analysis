# Data Engineering Capstone Project-1

## Documentation 

### Business Objective
The objective of the project is to design a data model for one of the big corporations' given employees data from the 1980s and 1995s, with all the tables to hold data, import the CSVs into a SQL database, transfer SQL database to HDFS/Hive,and perform analysis using Hive/Impala/Spark/SparkML using the data and create data and ML pipelines.

### Data Description
The database of employees from the 1980s and 1995s are provided as six CSV files as given below:

```
a. Titles (titles.csv):
- title_id – Unique id of type of employee (designation id)
- title – Designation 

b. Employees (employees.csv):
- emp_no – Employee Id 
- emp_titles_id – designation id 
- birth_date – Date of Birth 
- first_name – First Name 
- last_name – Last Name 
- sex – Gender 
- hire_date – Employee Hire date 
- no_of_projects – Number of projects worked on 
- Last_performance_rating – Last year performance rating 
- left – Employee left the organization 
- Last_date - Last date of employment (Exit Date) 

c. Salaries (salaries.csv):
- emp_no – Employee id 
- Salary – Employee’s Salary 

d. Departments (departments.csv)
- dept_no - Unique id for each department 
- dept_name – Department Name 

e. Department Managers (dept_manager.csv)
- dept_no - Unique id for each department 
- emp_no – Employee number (head of the department) 

f. Department Employees (dept_emp.csv)
- emp_no – Employee id 
- dept_no - Unique id for each department 
```

### Technology Stack
* MySQL (to create database)
* Shell Commands
* Sqoop (Transfer data from MySQL Server to HDFS/Hive)
* HDFS (to store the data)
* Hive (to create database)
* Impala (to perform the EDA)
* SparkSQL (to perform the EDA)
* SparkML (to perform model building)

### ER Diagram
![erd_dona](https://user-images.githubusercontent.com/83896298/169606649-d4b5bdfa-b139-4757-bd9d-3a069b31be0b.png)

### Pipeline Creation

```
Run the following Commands in the given order

1)MySQL Data Loading 
mysql -u anabig114239 -pBigdata123
show databases;
use anabig114242;

Create tables for analysis 
a. Upload the tables to ftp (https://npbdh.cloudloka.com/ftp)
b. Run the below command to create tables.
source sqltables.sql

2) Data transfer from SQL to HDFS using Sqoop
Run the following command to transfer the data from SQL to HDFS 
sh sqoop_HDFS.sh

3) Creating tables in Hive
Run the following command in Hive
hive -f hive_commands.sql

4) Analysis using Impala
Run the following command in Impala
impala-shell -f command_impala.sql > output.txt

The output of the above command will be saved in the output.txt

5) Upload data to PySpark and perform data analysis and ML modeling using Spark SQL and Spark ML respectively.
```
### Code Elaboration 

### Step 1 : MySQL
```
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
```

### Step 2 : Data transfer from SQL to HDFS using Sqoop
```
sqoop list-databases --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306  --username anabig114242 --password Bigdata123
sqoop list-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114242 --username anabig114246 --password Bigdata123

sqoop import-all-tables "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114242 --username anabig114242 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir /user/anabig114242/hive/warehouse --m 1 --driver com.mysql.jdbc.Driver

hdfs dfs -ls /user/anabig114242/hive/warehouse   

hadoop fs -mkdir /user/anabig114242/capstone1_dona
hadoop fs -put /home/anabig114242/d_Departments.avsc /user/anabig114242/capstone1_dona/d_Departments.avsc
hadoop fs -put /home/anabig114242/d_Employees.avsc /user/anabig114242/capstone1_dona/d_Employees.avsc
hadoop fs -put /home/anabig114242/d_Department_Employees.avsc /user/anabig114242/capstone1_dona/d_Department_Employees.avsc
hadoop fs -put /home/anabig114242/d_Department_Managers.avsc /user/anabig114242/capstone1_dona/d_Department_Managers.avsc
hadoop fs -put /home/anabig114242/d_Salaries.avsc /user/anabig114242/capstone1_dona/d_Salaries.avsc
hadoop fs -put /home/anabig114242/d_Titles.avsc /user/anabig114242/capstone1_dona/d_Titles.avsc
```

### Step 3 : Table Creation in Hive
```
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
```

### Step 4 : Analysis using Impala
```
use donaalab;

show tables;
select * from d_Department_Employees limit 10;                                                                                                                         
select * from d_Department_Managers limit 10;                                                                                                                            
select * from d_Departments limit 10;                                                                                                                         
select * from d_Employees limit 10;                                                                                                                                    
select * from d_Salaries limit 10;                                                                                                                               
select * from d_Titles limit 10; 

select E.emp_no, E.last_name,E.first_name, E.sex, S.salary FROM d_employees E JOIN d_salaries S ON E.emp_no = S.emp_no;

select first_name, last_name, hire_date from employee_date where year(hire_date)=1986 order by hire_date;

SELECT D.dept_no, D.dept_name,DM.emp_no, E.first_name, E.last_name FROM d_departments D JOIN d_department_managers DM ON D.dept_no =DM.dept_no JOIN d_employees E ON DM.emp_no = E.emp_no;

select DE.emp_no, E.last_name, E.first_name, D.dept_name from d_Department_Employees DE join d_Employees E on DE.emp_no = E.emp_no join d_Departments D on DE.dept_no = D.dept_no;

select first_name, last_name, sex from d_Employees where first_name = 'Hercules' and  last_name Like 'B%';

select D.dept_name,E.last_name,E.first_name,D.dept_no from d_Department_Employees DE join d_Employees E on DE.emp_no = E.emp_no join d_Departments D on DE.dept_no =D.dept_no where D.dept_name = '"Sales"';

select DE.emp_no, E.first_name,E.last_name,DE.dept_name from d_Department_Employees DE join d_Employees E on DE.emp_no = E.emp_no join d_Departments D on DE.dept_no = D.dept_no where D.dept_name = '”Sales”' or D.dept_name = '”Development”';

select last_name,count(last_name) as Frequency from d_Employees group by last_name order by count(last_name) desc;

SELECT title, avg(salary) as avg_Salary from d_Titles T JOIN d_Employees E on T.title_id = E.emp_title_id JOIN d_Salaries S on E.emp_no =  S.emp_no GROUP BY title;

select D.dept_name, count(E.emp_no) as No_of_Employees,sum(S.salary) as Total_Salary from d_Departments D join d_Department_Employees DE on D.dept_no= DE.dept_no join d_Employees E on DE.emp_no=E.emp_no join d_Salaries S on S.emp_no=E.emp_no group by dept_name;

select concat(first_name," ",last_name) as emp_name,emp_no,sex,hire_date,last_performance_rating from d_employees;

create view employeesnew as(select concat(first_name," ",last_name) as emp_name,emp_no,sex,hire_date,last_performance_rating from d_employees);

select DE.emp_no, EC.emp_name,D.dept_name from d_department_employees DE join d_employees E on DE.emp_no = E.emp_no join d_departments D on DE.dept_no = D.dept_no join employeesnew EC on E.emp_no=EC.emp_no;

select D.dept_name,count(DE.emp_no) as no_of_employees from d_department_employees DE join d_departments D on D.dept_no = DE.dept_no group by D.dept_name;

select EC.emp_no, EC.emp_name,S.salary from employeesnew EC join d_salaries S on S.emp_no = EC.emp_no;

select D.dept_name,E.sex,count(E.sex) as count_of_male_female from d_employees E join d_department_employees DE on E.emp_no = DE.emp_no join d_departments D on D.dept_no = DE.dept_no group by D.dept_name,E.sex;

```
### Outputs and Visualization

**1. A list showing employee number, last name, first name, sex, and salary for each employee**

select E.emp_no, E.last_name,E.first_name, E.sex, S.salary FROM d_employees E JOIN d_salaries S ON E.emp_no = S.emp_no;

![1_O](https://user-images.githubusercontent.com/83896298/169620327-9336eb4d-01fe-4d39-85b8-b9096dbb5ef9.png)

**2. A list showing first name, last name, and hire date for employees who were hired in 1986.**

select first_name, last_name, hire_date from employee_date where year(hire_date)=1986 order by hire_date;

![2](https://user-images.githubusercontent.com/83896298/169620522-631e6a7d-3b59-4752-8fe6-571dc55ce7dd.png)

**3. A list showing the manager of each department with the following information: department number, department name, 
the manager's employee number, last name, first name.**

SELECT D.dept_no, D.dept_name,DM.emp_no, E.first_name, E.last_name FROM d_departments D JOIN d_department_managers DM ON D.dept_no =DM.dept_no JOIN d_employees E ON DM.emp_no = E.emp_no;

![3_O](https://user-images.githubusercontent.com/83896298/169620560-61247a22-d0c0-4a8a-90df-30526fe17366.png)

**4. A list showing the department of each employee with the following information: employee number, last name, first 
name, and department name.**

select DE.emp_no, E.last_name, E.first_name, D.dept_name from d_Department_Employees DE join d_Employees E on DE.emp_no = E.emp_no join d_Departments D on DE.dept_no = D.dept_no;

![4_O](https://user-images.githubusercontent.com/83896298/169620603-8194af96-f286-41d3-b781-cd94045c5c40.png)

**5. A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B.“**

select first_name, last_name, sex from d_Employees where first_name = 'Hercules' and  last_name Like 'B%';

![5_O](https://user-images.githubusercontent.com/83896298/169620653-35aee62e-255c-4471-9075-58b1af318fe0.png)

**6. A list showing all employees in the Sales department, including their employee number, last name, first name, and 
department name.**

select D.dept_name,E.last_name,E.first_name,D.dept_no from d_Department_Employees DE join d_Employees E on DE.emp_no = E.emp_no join d_Departments D on DE.dept_no =D.dept_no where D.dept_name = '"Sales"';

![6_OUT](https://user-images.githubusercontent.com/83896298/169620706-70d80395-a75e-4bf8-b0af-1a6718f91198.png)

**7. A list showing all employees in the Sales and Development departments, including their employee number, last name, 
first name, and department name**

select DE.emp_no, E.first_name,E.last_name,DE.dept_name from d_Department_Employees DE join d_Employees E on DE.emp_no = E.emp_no join d_Departments D on DE.dept_no = D.dept_no where D.dept_name = '”Sales”' or D.dept_name = '”Development”';

![image](https://user-images.githubusercontent.com/83896298/169622426-1ef140bd-0318-45a2-8436-73e63231fca9.png)

**8. A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each 
last name**

select last_name,count(last_name) as Frequency from d_Employees group by last_name order by count(last_name) desc;

![8_O](https://user-images.githubusercontent.com/83896298/169620804-0a76e0b2-31a4-43be-bbe1-8ae37a39e704.png)

**9. Histogram to show the salary distribution among the employees**

select D.dept_name, count(E.emp_no) as No_of_Employees,sum(S.salary) as Total_Salary from d_Departments D join d_Department_Employees DE on D.dept_no= DE.dept_no join d_Employees E on DE.emp_no=E.emp_no join d_Salaries S on S.emp_no=E.emp_no group by dept_name;

![10](https://user-images.githubusercontent.com/83896298/169621314-987217a9-90f2-4251-8c08-90593e68f74d.png)

**10. Bar graph to show the Average salary per title (designation)**

SELECT title, avg(salary) as avg_Salary from d_Titles T JOIN d_Employees E on T.title_id = E.emp_title_id JOIN d_Salaries S on E.emp_no =  S.emp_no GROUP BY title;

![Screenshot 2022-05-21 040945](https://user-images.githubusercontent.com/83896298/169621193-8091e0be-9622-41b1-a253-516133c27dbb.png)

**12.Own Analysis(based on the data understanding)**

**Displaying the concatenated version of first name and last name of employees along with employee number,sex,hire date and last performance rating**

select concat(first_name," ",last_name) as emp_name,emp_no,sex,hire_date,last_performance_rating from d_employees;

![12_1](https://user-images.githubusercontent.com/83896298/169621425-aa181bd6-31ab-4eaf-80ef-f14716f93053.png)

**Listing the employee number,employee name and department name of each employee**

create view employeesnew as(select concat(first_name," ",last_name) as emp_name,emp_no,sex,hire_date,last_performance_rating from d_employees);

select DE.emp_no, EC.emp_name,D.dept_name from d_department_employees DE join d_employees E on DE.emp_no = E.emp_no join d_departments D on DE.dept_no = D.dept_no join employeesnew EC on E.emp_no=EC.emp_no;

![12_2](https://user-images.githubusercontent.com/83896298/169621478-7178d07d-6e5a-4c00-91bc-150dc6090e1f.png)

**Listing total number of employees in each department**

select D.dept_name,count(DE.emp_no) as no_of_employees from d_department_employees DE join d_departments D on D.dept_no = DE.dept_no group by D.dept_name;

![12_3](https://user-images.githubusercontent.com/83896298/169621519-04fbcb57-a7a0-41c1-9972-31120c377e02.png)

**Displaying the employee name,employee number and salary of each employee**

select EC.emp_no, EC.emp_name,S.salary from employeesnew EC join d_salaries S on S.emp_no = EC.emp_no;

![12_4](https://user-images.githubusercontent.com/83896298/169621551-62d42a32-4b13-41ac-9fc5-6572db38c1a7.png)

**Listing the number of male and female employees in each department**

select D.dept_name,E.sex,count(E.sex) as count_of_male_female from d_employees E join d_department_employees DE on E.emp_no = DE.emp_no join d_departments D on D.dept_no = DE.dept_no group by D.dept_name,E.sex;

![12_5](https://user-images.githubusercontent.com/83896298/169621570-56a7910a-93d9-4bf2-ba17-1ba8f1457709.png)


### Step 5 : Upload data to PySpark and perform data analysis and ML modeling using Spark SQL and Spark ML respectively.

### EDA in PySpark

**Histogram to show the salary distribution among the employees**

![SAL_DIST](https://user-images.githubusercontent.com/83896298/169647379-7b8a8e47-acb2-4995-aa3d-42d026c26acf.png)

**Bar graph to show the Average salary per title (designation)**

![AVG_SAL](https://user-images.githubusercontent.com/83896298/169647416-ae99acec-415d-4b76-83c7-237c64e99cad.png)

### ML Model Results

For modelling the data,Logistic Regression and Decision Tree classifiers are used and a moderately good accuracy is obtained.

**- Logistic Regression**

![lr acc](https://user-images.githubusercontent.com/83896298/169647552-32fd40a2-1815-4813-a3a8-b496a68c4a8d.png)

**- Decision Tree**

![dt aa](https://user-images.githubusercontent.com/83896298/169647575-038455a0-9252-4a32-b92a-9a0bb8b58eb3.png)

Here, the target variable is 'left1',which is used to make predictions on whether employees will leave the organization or not.

Accuracy comes out to be 0.9013. It obtains 90 % values that are correctly predicted by both the models.

> Here is the link to my python notebook conatining EDA and ML model.

./ https://github.com/donasam/Employee-Data-Analysis/blob/main/capstoneproject1_dona.ipynb ./


 



