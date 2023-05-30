# Intro to SQL
_Learn SQL for working with databases, using Google BigQuery._
https://www.kaggle.com/learn/intro-to-sql



## Lessons

1.  Getting Started With SQL and BigQuery
    Learn the workflow for handling big datasets with BigQuery and SQL

2.  Select, From & Where
    The foundational compontents for all SQL queries

3.  Group By, Having & Count
    Get more interesting insights directly from your SQL queries

4.  Order By
    Order your results to focus on the most important data for your use case.

5.  As & With
    Organize your query for better readability. This becomes especially important for complex queries.

6.  Joining Data
    Combine data sources. Critical for almost all real-world data problems.
    


# Getting Started With SQL and BigQuery
Learn the workflow for handling big datasets with BigQuery and SQL.


## Introduction
Structured Query Language, or SQL, is the programming language used with databases, and it is an important skill for any data scientist. In this course, you'll build your SQL skills using BigQuery, a web service that lets you apply SQL to huge datasets.
In this lesson, you'll learn the basics of accessing and examining BigQuery datasets. After you have a handle on these basics, we'll come back to build your SQL skills.


## Your first BigQuery commands
To use BigQuery, we'll import the Python package below:
```
from google.cloud import bigquery
```
The first step in the workflow is to create a Client object. As you'll soon see, this Client object will play a central role in retrieving information from BigQuery datasets.
```
# Create a "Client" object
client = bigquery.Client()
```
Using Kaggle's public dataset BigQuery integration.
We'll work with a dataset of posts on Hacker News, a website focusing on computer science and cybersecurity news.
In BigQuery, each dataset is contained in a corresponding project. In this case, our hacker_news dataset is contained in the bigquery-public-data project. To access the dataset,
We begin by constructing a reference to the dataset with the dataset() method.
Next, we use the get_dataset() method, along with the reference we just constructed, to fetch the dataset.
```
# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)
```
Every dataset is just a collection of tables. You can think of a dataset as a spreadsheet file containing multiple tables, all composed of rows and columns.
We use the list_tables() method to list the tables in the dataset.
```
# List all the tables in the "hacker_news" dataset
tables = list(client.list_tables(dataset))

# Print names of all tables in the dataset (there are four!)
for table in tables:
  print(table.table_id)
```
> comments
> full
> full_201510
> stories

Similar to how we fetched a dataset, we can fetch a table. In the code cell below, we fetch the full table in the hacker_news dataset.
```
# Construct a reference to the "full" table
table_ref = dataset_ref.table("full")

# API request - fetch the table
table = client.get_table(table_ref)
```
In the next section, you'll explore the contents of this table in more detail. For now, take the time to use the image below to consolidate what you've learned so far.
!(https://storage.googleapis.com/kaggle-media/learn/images/biYqbUB.png)


## Table schema
The structure of a table is called its schema. We need to understand a table's schema to effectively pull out the data we want.
In this example, we'll investigate the full table that we fetched above.
```
# Print information on all the columns in the "full" table in the "hacker_news" dataset
table.schema
```
> [SchemaField('title', 'STRING', 'NULLABLE', 'Story title', (), None),
>  SchemaField('url', 'STRING', 'NULLABLE', 'Story url', (), None),
>  SchemaField('text', 'STRING', 'NULLABLE', 'Story or comment text', (), None),
>  SchemaField('dead', 'BOOLEAN', 'NULLABLE', 'Is dead?', (), None),
>  SchemaField('by', 'STRING', 'NULLABLE', "The username of the item's author.", (), None),
>  SchemaField('score', 'INTEGER', 'NULLABLE', 'Story score', (), None),
>  SchemaField('time', 'INTEGER', 'NULLABLE', 'Unix time', (), None),
>  SchemaField('timestamp', 'TIMESTAMP', 'NULLABLE', 'Timestamp for the unix time', (), None),
>  SchemaField('type', 'STRING', 'NULLABLE', 'Type of details (comment, comment_ranking, poll, story, job, pollopt)', (), None),
>  SchemaField('id', 'INTEGER', 'NULLABLE', "The item's unique id.", (), None),
>  SchemaField('parent', 'INTEGER', 'NULLABLE', 'Parent comment ID', (), None),
>  SchemaField('descendants', 'INTEGER', 'NULLABLE', 'Number of story or poll descendants', (), None),
>  SchemaField('ranking', 'INTEGER', 'NULLABLE', 'Comment ranking', (), None),
>  SchemaField('deleted', 'BOOLEAN', 'NULLABLE', 'Is deleted?', (), None)]

Each SchemaField tells us about a specific column (which we also refer to as a field). In order, the information is:

- The name of the column
- The field type (or datatype) in the column
- The mode of the column ('NULLABLE' means that a column allows NULL values, and is the default)
- A description of the data in that column

The first field has the SchemaField:
> SchemaField('by', 'string', 'NULLABLE', "The username of the item's author.",())

This tells us:
- the field (or column) is called by,
- the data in this field is strings,
- NULL values are allowed, and
- it contains the usernames corresponding to each item's author.

We can use the list_rows() method to check just the first five lines of of the full table to make sure this is right. (Sometimes databases have outdated descriptions, so it's good to check.) This returns a BigQuery RowIterator object that can quickly be converted to a pandas DataFrame with the to_dataframe() method.
```
# Preview the first five lines of the "full" table
client.list_rows(table, max_results=5).to_dataframe()
```
> /opt/conda/lib/python3.7/site-packages/ipykernel_launcher.py:2: UserWarning: Cannot use bqstorage_client if max_results is set, reverting to fetching data with the tabledata.list endpoint.

> title	url	text	dead	by	score	time	timestamp	type	id	parent	descendants	ranking	deleted
> 0	None	None	I would rather just have wired earbuds, period...	None	zeveb	NaN	1591717736	2020-06-09 15:48:56+00:00	comment	23467666	23456782	NaN	NaN	None
> 1	None	None	DNS?	None	nly	NaN	1572810465	2019-11-03 19:47:45+00:00	comment	21436112	21435130	NaN	NaN	None
> 2	None	None	These benchmarks seem pretty good. Filterable...	None	mrkeen	NaN	1591717727	2020-06-09 15:48:47+00:00	comment	23467665	23467426	NaN	NaN	None
> 3	None	None	Oh really?<p>* Excel alone uses 86.1MB of priv...	None	oceanswave	NaN	1462987532	2016-05-11 17:25:32+00:00	comment	11677248	11676886	NaN	NaN	None
> 4	None	None	These systems are useless. Of the many flaws:...	None	nyxxie	NaN	1572810473	2019-11-03 19:47:53+00:00	comment	21436113	21435025	NaN	NaN	None

The list_rows() method will also let us look at just the information in a specific column. If we want to see the first five entries in the by column, for example, we can do that!
    
```
# Preview the first five entries in the "by" column of the "full" table
client.list_rows(table, selected_fields=table.schema[:1], max_results=5).to_dataframe()
```
> /opt/conda/lib/python3.7/site-packages/ipykernel_launcher.py:2: UserWarning: Cannot use bqstorage_client if max_results is set, reverting to fetching data with the tabledata.list endpoint.

> |   | title | 
> | 0 |	None |
> | 1 | None |
> | 2 |	None |
> | 3 | None |
> | 4 | None |
   
    
## Disclaimer
Before we go into the coding exercise, a quick disclaimer for those who already know some SQL:

Each Kaggle user can scan 5TB every 30 days for free. Once you hit that limit, you'll have to wait for it to reset.

The commands you've seen so far won't demand a meaningful fraction of that limit. But some BiqQuery datasets are huge. So, if you already know SQL, wait to run SELECT queries until you've seen how to use your allotment effectively. If you are like most people reading this, you don't know how to write these queries yet, so you don't need to worry about this disclaimer. 
    

## Your turn
Practice the commands you've seen to explore the structure of a dataset with crimes in the city of Chicago.
    
    
    
    
    
# Select, From & Where
The foundational compontents for all SQL queries
    
## Introduction
Now that you know how to access and examine a dataset, you're ready to write your first SQL query! As you'll soon see, SQL queries will help you sort through a massive dataset, to retrieve only the information that you need.

We'll begin by using the keywords SELECT, FROM, and WHERE to get data from specific columns based on conditions you specify.

For clarity, we'll work with a small imaginary dataset pet_records which contains just one table, called pets.

https://storage.googleapis.com/kaggle-media/learn/images/fI5Pvvp.png

## SELECT ... FROM
The most basic SQL query selects a single column from a single table. To do this,

specify the column you want after the word SELECT, and then
specify the table after the word FROM.
For instance, to select the Name column (from the pets table in the pet_records database in the bigquery-public-data project), our query would appear as follows:

https://storage.googleapis.com/kaggle-media/learn/images/c3GxYRt.png

Note that when writing an SQL query, the argument we pass to FROM is not in single or double quotation marks (' or "). It is in backticks (`).

## WHERE ...
BigQuery datasets are large, so you'll usually want to return only the rows meeting specific conditions. You can do this using the WHERE clause.

The query below returns the entries from the Name column that are in rows where the Animal column has the text 'Cat'.

https://storage.googleapis.com/kaggle-media/learn/images/HJOT8Kb.png

Example: What are all the U.S. cities in the OpenAQ dataset?
Now that you've got the basics down, let's work through an example with a real dataset. We'll use an OpenAQ dataset about air quality.

First, we'll set up everything we need to run queries and take a quick peek at what tables are in our database. (Since you learned how to do this in the previous tutorial, we have hidden the code. But if you'd like to take a peek, you need only click on the "Code" button below.)
    
```
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "openaq" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the "openaq" dataset
tables = list(client.list_tables(dataset))

# Print names of all tables in the dataset (there's only one!)
for table in tables:  
    print(table.table_id)
```
    
Using Kaggle's public dataset BigQuery integration.
global_air_quality
The dataset contains only one table, called global_air_quality. We'll fetch the table and take a peek at the first few rows to see what sort of data it contains. (Again, we have hidden the code. To take a peek, click on the "Code" button below.)
    
```
# Construct a reference to the "global_air_quality" table
table_ref = dataset_ref.table("global_air_quality")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "global_air_quality" table
client.list_rows(table, max_results=5).to_dataframe() 
```
    
> /opt/conda/lib/python3.7/site-packages/ipykernel_launcher.py:8: UserWarning: Cannot use bqstorage_client if max_results is set, reverting to fetching data with the tabledata.list endpoint.
    
 	location	city	country	pollutant	value	timestamp	unit	source_name	latitude	longitude	averaged_over_in_hours	location_geom
0	Borówiec, ul. Drapałka	Borówiec	PL	bc	0.85217	2022-04-28 07:00:00+00:00	µg/m³	GIOS	1.0	52.276794	17.074114	POINT(52.276794 1)
1	Kraków, ul. Bulwarowa	Kraków	PL	bc	0.91284	2022-04-27 23:00:00+00:00	µg/m³	GIOS	1.0	50.069308	20.053492	POINT(50.069308 1)
2	Płock, ul. Reja	Płock	PL	bc	1.41000	2022-03-30 04:00:00+00:00	µg/m³	GIOS	1.0	52.550938	19.709791	POINT(52.550938 1)
3	Elbląg, ul. Bażyńskiego	Elbląg	PL	bc	0.33607	2022-05-03 13:00:00+00:00	µg/m³	GIOS	1.0	54.167847	19.410942	POINT(54.167847 1)
4	Piastów, ul. Pułaskiego	Piastów	PL	bc	0.51000	2022-05-11 05:00:00+00:00	µg/m³	GIOS	1.0	52.191728	20.837489	POINT(52.191728 1)    
    
Everything looks good! So, let's put together a query. Say we want to select all the values from the city column that are in rows where the country column is 'US' (for "United States").
    
```
# Query to select all the items from the "city" column where the "country" column is 'US'
query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
```
    
Take the time now to ensure that this query lines up with what you learned above.

## Submitting the query to the dataset
We're ready to use this query to get information from the OpenAQ dataset. As in the previous tutorial, the first step is to create a Client object.
    
```
# Create a "Client" object
client = bigquery.Client()
```
    
Using Kaggle's public dataset BigQuery integration.
We begin by setting up the query with the query() method. We run the method with the default parameters, but this method also allows us to specify more complicated settings that you can read about in the documentation. We'll revisit this later.
    
```
# Set up the query
query_job = client.query(query) 
```
    
Next, we run the query and convert the results to a pandas DataFrame.
    
```
# API request - run the query, and return a pandas DataFrame
us_cities = query_job.to_dataframe() 
```
> /opt/conda/lib/python3.7/site-packages/google/cloud/bigquery/client.py:440: UserWarning: Cannot create BigQuery Storage client, the dependency google-cloud-bigquery-storage is not installed.
> "Cannot create BigQuery Storage client, the dependency "
  
Now we've got a pandas DataFrame called us_cities, which we can use like any other DataFrame.
    
```
# What five cities have the most measurements?
us_cities.city.value_counts().head() 
```
Phoenix-Mesa-Scottsdale                     39414
Los Angeles-Long Beach-Santa Ana            27479
Riverside-San Bernardino-Ontario            26887
New York-Northern New Jersey-Long Island    25417
San Francisco-Oakland-Fremont               22710
Name: city, dtype: int64
    

## More queries
If you want multiple columns, you can select them with a comma between the names:
```
query = """
        SELECT city, country
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """ 
```
    
You can select all columns with a * like this:
```
query = """
        SELECT *
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """ 
```
    
## Q&A: Notes on formatting
The formatting of the SQL query might feel unfamiliar. If you have any questions, you can ask in the comments section at the bottom of this page. Here are answers to two common questions:

Question: What's up with the triple quotation marks (""")?
Answer: These tell Python that everything inside them is a single string, even though we have line breaks in it. The line breaks aren't necessary, but they make it easier to read your query.

Question: Do you need to capitalize SELECT and FROM?
Answer: No, SQL doesn't care about capitalization. However, it's customary to capitalize your SQL commands, and it makes your queries a bit easier to read.

    
## Working with big datasets
BigQuery datasets can be huge. We allow you to do a lot of computation for free, but everyone has some limit.
Each Kaggle user can scan 5TB every 30 days for free. Once you hit that limit, you'll have to wait for it to reset.
The biggest dataset currently on Kaggle is 3TB, so you can go through your 30-day limit in a couple queries if you aren't careful.
Don't worry though: we'll teach you how to avoid scanning too much data at once, so that you don't run over your limit.
To begin,you can estimate the size of any query before running it. Here is an example using the (very large!) Hacker News dataset. To see how much data a query will scan, we create a QueryJobConfig object and set the dry_run parameter to True.
    
```
# Query to get the score column from every row where the type column has value "job"
query = """
        SELECT score, title
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = "job" 
        """

# Create a QueryJobConfig object to estimate size of query without running it
dry_run_config = bigquery.QueryJobConfig(dry_run=True)

# API request - dry run query to estimate costs
dry_run_query_job = client.query(query, job_config=dry_run_config)

print("This query will process {} bytes.".format(dry_run_query_job.total_bytes_processed)) 
```
> This query will process 553320240 bytes.    

You can also specify a parameter when running the query to limit how much data you are willing to scan. Here's an example with a low limit.
    
```
# Only run the query if it's less than 1 MB
ONE_MB = 1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_MB)

# Set up the query (will only run if it's less than 1 MB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
safe_query_job.to_dataframe() 
```
    > ERROR
    
In this case, the query was cancelled, because the limit of 1 MB was exceeded. However, we can increase the limit to run the query successfully!
```
# Only run the query if it's less than 1 GB
ONE_GB = 1000*1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_GB)

# Set up the query (will only run if it's less than 1 GB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
job_post_scores = safe_query_job.to_dataframe()

# Print average score for job posts
job_post_scores.score.mean() 
```
> 1.7267060367454068

    
## Your turn!
Writing SELECT statements is the key to using SQL. So try your new skills!
    https://www.kaggle.com/kernels/fork/681989
