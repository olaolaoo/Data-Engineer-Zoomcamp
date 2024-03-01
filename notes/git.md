# clone 仓库到本地

http：` git clone https://github.com/olaolaoo/Data-Engineer-Zoomcamp.git `

ssh：` git clone git@github.com:olaolaoo/Data-Engineer-Zoomcamp.git `

# 上传

1. 进入仓库目录git add .
2. git commit  -m "logs"
3. git push origin main



git add .gitignore 

git commit -m "Add .gitignore file" 

git push



git rm --cached '**/.DS_Store'

git commit -m "Remove .DS_Store  from tracking"

git push

# submodule

git submodule add https://github.com/mage-ai/mage-zoomcamp.git mage-zoomcamp 2_workflow-orchestration/mage-zoomcamp

git submodule init
git submodule update

# gitignore

在.gitignore文件中，星号`*`和两个星号`**`有不同的作用：

1. **单个星号 `*`**：在.gitignore中，单个星号`*`用于匹配零个或多个字符。例如，`*.txt`表示匹配所有以`.txt`结尾的文件。
2. **两个星号 `**`**：在.gitignore中，两个星号`**`用于匹配任意数量的目录层级。例如，`**/.txt`表示匹配任意目录下的所有`.txt`结尾文件，包括子目录中的文件。

总的来说，单个星号`*`主要用于匹配当前目录下的文件，而两个星号`**`则可以匹配任意目录下的文件。

.txt: 忽略**当前目录**下名为.txt的文件

*.txt: 忽略**当前目录**下后缀是.txt的文件

**/.txt: 忽略任意目录下后缀是.txt的文件



如果先上传了`.csv`文件到存储库（repo）中，然后将`.csv`文件添加到`.gitignore`中，并重新提交并拉取存储库，之前上传的`.csv`文件不会自动从存储库中删除。`.gitignore`文件只会影响将来的提交，而不会影响已经提交的历史记录。
