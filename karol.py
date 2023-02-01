import os
print(os.stat('test.txt').st_size)
open('test.txt', 'w').close()
print(os.stat('test.txt').st_size)
# with open ('test.txt', 'r') as file:
#     print(file)