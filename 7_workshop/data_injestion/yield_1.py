   
# A Simple Python program to demonstrate working
# of yield
 
# A generator function that yields 1 for the first time,
# 2 second time and 3 third time
 
 
def simpleGeneratorFun():
    print('第一次')
    yield 1
    print('第二次')
    yield 2
    print('第三次')
    yield 3
 
 
# Driver code to check above generator function
for value in simpleGeneratorFun():
    print(value)