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

