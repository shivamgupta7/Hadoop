import os 
import sys

importcommandsqoop="sqoop import --connect jdbc:mysql://localhost/employees --username hadoopuser --password Gupta@007 --table employees --m 1 --target-dir /user/hadoop/data"

os.system(importcommandsqoop)