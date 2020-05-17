import os
import sys

codegencommandsqoop="sqoop codegen --connect jdbc:mysql://localhost/employees --username hadoopuser --password Gupta@007 --table employees1 -outdir /home/shivam_gupta/Documents/Hadoop/PyHive/Sqoop/"

os.system(codegencommandsqoop)