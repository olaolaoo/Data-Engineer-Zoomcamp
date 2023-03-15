# base Docker image that we will build on
# use image：prefecthq/prefect，并指定了版本号为 2.7.7-python3.9
FROM prefecthq/prefect:2.7.7-python3.9

# 该命令将本地的 docker-requirements.txt 文件复制到 Docker 镜像中，这里的目标路径是 Docker 镜像的根目录。
COPY docker-requirements.txt .

# 安装 docker-requirements.txt 文件中指定的 Python 依赖包。
#其中，--trusted-host pypi.python.org 选项指定了信任的 PyPI 主机，--no-cache-dir 选项指定了不使用缓存目录。
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN apt-get update && apt-get install -y wget

# 该命令将本地的 flows 目录复制到 Docker 镜像中，这里的目标路径是 /opt/prefect/flows
COPY flows /opt/prefect/flows

# 该命令在 Docker 镜像中创建一个目录 /opt/prefect/data/yellow，其中 -p 选项表示如果目录不存在，则会自动创建父目录。
RUN mkdir -p /opt/prefect/data/yellow