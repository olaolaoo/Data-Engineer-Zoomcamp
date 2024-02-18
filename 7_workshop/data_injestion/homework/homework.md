## Workshop-Data Injestion

## Question 1.  **What is the sum of the outputs of the generator for limit = 5?**

> **Answer: 8.38**

Add a variable with `sum`  to the `for iterator`：

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)

sum=0

for sqrt_value in generator:
    sum+=sqrt_value
    print(f"sqrt_value is {sqrt_value}")
    print(f"total is {sum}")
```

Outcome：

```python
sqrt_value is 1.0
total is 1.0
sqrt_value is 1.4142135623730951
total is 2.414213562373095
sqrt_value is 1.7320508075688772
total is 4.146264369941973
sqrt_value is 2.0
total is 6.146264369941973
sqrt_value is 2.23606797749979
total is 8.382332347441762
```

## Question .**What is the 13th number yielded**

> **Answer:3.6**

Add a variable with name `number`  to for iterator：

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

number=1

for sqrt_value in generator:
    print(f"the {number} sqrt_value is {sqrt_value}")
    number+=1
    if number>13:
      break
```

Outcome：

```python
the 1 sqrt_value is 1.0
the 2 sqrt_value is 1.4142135623730951
the 3 sqrt_value is 1.7320508075688772
the 4 sqrt_value is 2.0
the 5 sqrt_value is 2.23606797749979
the 6 sqrt_value is 2.449489742783178
the 7 sqrt_value is 2.6457513110645907
the 8 sqrt_value is 2.8284271247461903
the 9 sqrt_value is 3.0
the 10 sqrt_value is 3.1622776601683795
the 11 sqrt_value is 3.3166247903554
the 12 sqrt_value is 3.4641016151377544
the 13 sqrt_value is 3.605551275463989
```

## Question 3. calculate the sum of all ages of people

> **Answer:353**

Here is my jupyter notebook including all the code [load_to_DuckDB.ipynb](./load_to_DuckDB.ipynb)

## Question 4. 

> **Answer: 266**

Here is my jupyter notebook including all the code [primary_load_to_DuckDB.ipynb](./primary_load_to_DuckDB.ipynb)

## Submitting the solutions 

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/workshop1

Due date: Feb. 18, 2024, 11 p.m.
