# base Docker image that we will build on

FROM python:3.9.1

# set up our image by installing prerequisites; pandas in this case

RUN pip install pandas

# setr up the working directory inside the containe

WORKDIR /app

# copy the script to the container. 1st name is source file, 2nd is destination

COPY pipeline.py pipeline.py

# define what to do first when the container runs

# in this example, we will just run the script

# 执行python
#ENTRYPOINT ["python", "pipeline.py"]

# 进入bash命令行模式   
ENTRYPOINT ["bash"]

