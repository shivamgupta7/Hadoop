import os
import sys

exportcommandsqoop="sqoop export -m1 --connect jdbc:mysql://localhost/employees --username hadoopuser --password Gupta@007 --table employees1 --export-dir /user/hadoop/data/part-m-00000"

os.system(exportcommandsqoop)