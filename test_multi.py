import os
import re

temp1 = os.listdir(os.getcwd() + "/data")

print(temp1)

temp = list(filter(lambda  val: ".biom" in val, temp1 ))

print(temp)

""" for val in temp1:
    print(val)
    print(".biom" in val) """