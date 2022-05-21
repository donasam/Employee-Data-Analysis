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


