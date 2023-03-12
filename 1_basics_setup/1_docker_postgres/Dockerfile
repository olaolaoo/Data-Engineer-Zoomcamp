# base Docker image that we will build on

FROM python:3.9.1

# set up our image by installing prerequisites; pandas in this case

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

# setr up the working directory inside the containe

WORKDIR /app

# copy the script to the container. 1st name is source file, 2nd is destination

COPY ingest_data_parquet.py ingest_data_parquet.py

# define what to do first when the container runs

# in this example, we will just run the script

# 执行python
ENTRYPOINT ["python", "ingest_data_parquet.py"]

# 进入bash命令行模式   
#ENTRYPOINT ["bash"]

