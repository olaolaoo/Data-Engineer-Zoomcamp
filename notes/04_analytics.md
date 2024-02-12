> Previous:[03_data-warehouse](03_data_warehouse.md)
>
> [See the content tree](index.md)
>
> Next:[05_batch_processing](05_batch_processing.md)

# 1.Introduction to Analytics Engineering

## 1.1.What is Analytics Engineering?

> 会数据分析的程序员，填补以下两类职能空缺
>
> 数据开发：会编程，不会分析
>
> 数据分析师和科学家：会分析，不太会编程

As the _data domain_ has developed over time, new tools have been introduced that have changed the dynamics of working with data:

1. Massively parallel processing (MPP) databases
   * Lower the cost of storage 
   * BigQuery, Snowflake, Redshift...
1. Data-pipelines-as-a-service
   * Simplify the ETL process
   * Fivetran, Stitch...
1. SQL-first / Version control systems
   * Looker...
1. Self service analytics
   * Mode...
1. Data governance

The introduction of all of these tools changed the way the data teams work as well as the way that the stakeholders consume the data, creating a gap in the roles of the data team. Traditionally:

* The ***data engineer数据工程师*** prepares and maintains the infrastructure the data team needs.
* The ***data analyst*** uses data to answer questions and solve problems (they are in charge of _today_).
* The ***data scientist*** predicts the future based on past patterns and covers the what-ifs rather than the day-to-day (they are in charge of _tomorrow_).

The ***analytics engineer分析工程师*** is the role that tries to fill the gap: it introduces the good software engineering practices to the efforts of data analysts and data scientists. The analytics engineer may be exposed to the following tools:

1. Data Loading (Stitch...)
1. Data Storing (Data Warehouses)
1. **Data Modeling (dbt, Dataform...)**
1. **Data Presentation (BI tools like Looker, Mode, Tableau...)**

This lesson focuses on the last 2 parts: Data Modeling and Data Presentation.

## 1.2.Data Modeling Concepts

**数据建模**

* **ETL vs ELT** ![](./images/04_01.png)

在本课程中我们将介绍 ELT 过程中的 _transform_ 

* **Dimensional Modeling维度建模**

  > 维度建模（Dimensional Modeling）是一种设计数据仓库和数据集市的方法，旨在提供有效的数据存储和查询性能，以支持数据分析和报告需求。维度建模通常采用星型或雪花型模式，其中包含了维度（Dimensions）和事实（Facts）

[Ralph Kimball's Dimensional Modeling](https://www.wikiwand.com/en/Dimensional_modeling#:~:text=Dimensional%20modeling%20(DM)%20is%20part,use%20in%20data%20warehouse%20design.) is an approach to Data Warehouse design which focuses on 2 main points:

* Deliver data which is understandable to the business users.
* Deliver fast query performance.

Other goals such as reducing redundant data (prioritized by other approaches such as [3NF](https://www.wikiwand.com/en/Third_normal_form#:~:text=Third%20normal%20form%20(3NF)%20is,integrity%2C%20and%20simplify%20data%20management.) by [Bill Inmon](https://www.wikiwand.com/en/Bill_Inmon)) are secondary to these goals. Dimensional Modeling also differs from other approaches to Data Warehouse design such as [Data Vaults](https://www.wikiwand.com/en/Data_vault_modeling).

Dimensional Modeling is based around 2 important concepts:

![](./images/04_02.png)

* ***Fact Table***:
  * _Facts_ = _Measures_
  * Typically numeric values which can be aggregated, such as measurements or metrics.
    * Examples: sales, orders, etc.
  * Corresponds to a [_business process_ ](https://www.wikiwand.com/en/Business_process).
  * Can be thought of as _"verbs"_.
* ***Dimension Table***:
  * _Dimension_ = _Context_
  * Groups of hierarchies and descriptors that define the facts.
    * Example: customer, product, etc.
  * Corresponds to a _business entity_.
  * Can be thought of as _"nouns"_.
* Dimensional Modeling is built on a [***star schema***](https://www.wikiwand.com/en/Star_schema) with fact tables surrounded by dimension tables.星型模式是维度建模中常用的一种设计模式，其中包含一个中心的事实表，周围围绕着多个维度表，形成了类似星星的结构。在星型模式中，每个维度表与事实表之间都有直接的一对多关系，使得查询和分析变得简单和直观

A good way to understand the _architecture_ of Dimensional Modeling is by drawing an analogy between dimensional modeling and a restaurant餐馆:

![](./images/04_03.png)

* Stage Area:
  * Contains the raw data.
  * Not meant to be exposed to everyone.
  * Similar to the food storage area in a restaurant.
* Processing area:
  * From raw data to data models.
  * Focuses in efficiency and ensuring standards.
  * Similar to the kitchen in a restaurant.
* Presentation area:
  * Final presentation of the data.
  * Exposure to business stakeholder.
  * Similar to the dining room in a restaurant.

# 2.dbt

## 2.1.What is dbt?

**数据建模工具**

![](./images/04_04.png)

旨在简化和加速数据分析和报告的开发过程。它提供了一种声明式的、可重复使用的方法来定义、构建和管理数据转换和建模过程

***dbt*** stands for ***data build tool***. It's a _transformation_ tool: it allows us to transform process _raw_ data in our Data Warehouse to _transformed_ data which can be later used by Business Intelligence tools and any other data consumers.

dbt also allows us to introduce good software engineering practices by defining a _deployment workflow允许我们通过定义一个“部署工作流”来引入良好的软件工程实践_:

1. Develop models

1. Test and document models

   dbt提供了内置的测试和文档功能，允许用户编写和运行数据质量测试、业务规则验证以及自动生成数据文档。这有助于确保数据质量和一致性，并提供了对数据模型和转换过程的全面了解

1. Deploy models with _version control_ and _CI/CD_.

   dbt与版本控制系统集成，并支持代码审查和部署工作流。用户可以使用常见的版本控制工具（如Git）来管理dbt项目，并将数据模型和转换过程部署到不同的环境中（如开发、测试、生产）

## 2.2.How does dbt work?

> dbt的工作原理是通过定义一个**建模层**，该层位于我们的数据仓库之上。建模层将把数据仓库中的_表格_转换为**模型**，然后我们将这些模型转换为_派生模型_，最后可以将其存储到数据仓库中以保持持久性
>
> 一个模型是一个sql文档

![](./images/04_05.png)

dbt works by defining a ***modeling layer*** that sits on top of our Data Warehouse. The modeling layer will turn _tables_ into ***models*** which we will then transform into _derived models_, which can be then stored into the Data Warehouse for persistence.

* A ***model*** :

  * a .sql file with a `SELECT` statement; 

  * no DDL or DML is used. 

  * dbt will compile(编译) the file and run it in our Data Warehouse.

## 2.3.How to use dbt?

> 为了与BigQuery集成，我们将使用dbt Cloud IDE，因此不需要本地安装dbt core。如果要在本地进行开发而不是使用Cloud IDE，则需要dbt Core。使用本地Postgres数据库可以使用dbt Core，它可以在本地安装并连接到Postgres，通过CLI运行模型

![](./images/04_06.png)

* For integration with BigQuery we will use the dbt Cloud IDE, so a local installation of dbt core isn't required. For developing locally rather than using the Cloud IDE, dbt Core is required. Using dbt with a local Postgres database can be done with dbt Core, which can be installed locally and connected to Postgres and run models through the CLI.

* dbt has 2 main components: _dbt Core_ and _dbt Cloud_:

  * ***dbt Core***: open-source project that allows the data transformation.
    * Builds and runs a dbt project (.sql and .yaml files).
    * Includes SQL compilation logic, macros and database adapters.
    * Includes a CLI interface to run dbt commands locally.
    * Open-source and free to use.

  * ***dbt Cloud***: SaaS application to develop and manage dbt projects.
    * Web-based IDE to develop, run and test a dbt project.
    * Jobs orchestration.
    * Logging and alerting.
    * Intregrated documentation.
    * Free for individuals (one developer seat).


## 2.4.Setting up dbt?

Before we begin, go to BigQuery and create 2 new empty datasets for your project: a _development_ dataset and a _production_ dataset. Name them any way you'd like.

* **dbt Cloud**

In order to use dbt Cloud you will need to create a user account. Got to the [dbt homepage](https://www.getdbt.com/) and sign up.

During the sign up process you will be asked to create a starter project and connect to a database. We will connect dbt to BigQuery using [BigQuery OAuth](https://docs.getdbt.com/docs/dbt-cloud/cloud-configuring-dbt-cloud/cloud-setting-up-bigquery-oauth). More detailed instructions on how to generate the credentials and connect both services are available [in this link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md). When asked, connnect the project to your _development_ dataset.

Make sure that you set up a GitHub repo for your project. In _Account settings_ > _Projects_ you can select your project and change its settings, such as _Name_ or _dbt Project Subdirectoy_, which can be convenient if your repo is previously populated and would like to keep the dbt project in a single subfolder.

In the IDE windows, press the green _Initilize_ button to create the project files. Inside `dbt_project.yml`, change the project name both in the `name` field as well as right below the `models:` block. You may comment or delete the `example` block at the end.



