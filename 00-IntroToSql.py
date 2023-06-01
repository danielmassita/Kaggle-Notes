# |----- Theory 01 -----|

from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the "hacker_news" dataset
tables = list(client.list_tables(dataset))

# Print names of all tables in the dataset (there are four!)
for table in tables:
	print(table.table_id)
"""
comments
full
full_201510
stories
"""

# Construct a reference to the "full" table
table_ref = dataset.ref_table("full")

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns in the "full" table in the "hacker_news" dataset
table.schema

"""[SchemaField('title', 'STRING', 'NULLABLE', 'Story title', (), None),
 SchemaField('url', 'STRING', 'NULLABLE', 'Story url', (), None),
 SchemaField('text', 'STRING', 'NULLABLE', 'Story or comment text', (), None),
 SchemaField('dead', 'BOOLEAN', 'NULLABLE', 'Is dead?', (), None),
 SchemaField('by', 'STRING', 'NULLABLE', "The username of the item's author.", (), None),
 SchemaField('score', 'INTEGER', 'NULLABLE', 'Story score', (), None),
 SchemaField('time', 'INTEGER', 'NULLABLE', 'Unix time', (), None),
 SchemaField('timestamp', 'TIMESTAMP', 'NULLABLE', 'Timestamp for the unix time', (), None),
 SchemaField('type', 'STRING', 'NULLABLE', 'Type of details (comment, comment_ranking, poll, story, job, pollopt)', (), None),
 SchemaField('id', 'INTEGER', 'NULLABLE', "The item's unique id.", (), None),
 SchemaField('parent', 'INTEGER', 'NULLABLE', 'Parent comment ID', (), None),
 SchemaField('descendants', 'INTEGER', 'NULLABLE', 'Number of story or poll descendants', (), None),
 SchemaField('ranking', 'INTEGER', 'NULLABLE', 'Comment ranking', (), None),
 SchemaField('deleted', 'BOOLEAN', 'NULLABLE', 'Is deleted?', (), None)]"""

# Preview the first five lines of the "full" table
client.list_rows(table, max_result=5).to_dataframe()

"""
	title	url	text	dead	by	score	time	timestamp	type	id	parent	descendants	ranking	deleted
0	None	None	I would rather just have wired earbuds, period...	None	zeveb	NaN	1591717736	2020-06-09 15:48:56+00:00	comment	23467666	23456782	NaN	NaN	None
1	None	None	DNS?	None	nly	NaN	1572810465	2019-11-03 19:47:45+00:00	comment	21436112	21435130	NaN	NaN	None
2	None	None	These benchmarks seem pretty good. Filterable...	None	mrkeen	NaN	1591717727	2020-06-09 15:48:47+00:00	comment	23467665	23467426	NaN	NaN	None
3	None	None	Oh really?<p>* Excel alone uses 86.1MB of priv...	None	oceanswave	NaN	1462987532	2016-05-11 17:25:32+00:00	comment	11677248	11676886	NaN	NaN	None
4	None	None	These systems are useless. Of the many flaws:...	None	nyxxie	NaN	1572810473	2019-11-03 19:47:53+00:00	comment	21436113	21435025	NaN	NaN	None
"""

# Preview the first five entries in the "by" column of the "full" table
client.list_rows(table, selected_fields=table.schema[:1], max_results=5).to_dataframe()

"""
	title
0	None
1	None
2	None
3	None
4	None
"""

# |----- Exercise 01 -----| 

# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex1 import *
print("Setup Complete")

"Using Kaggle's public dataset BigQuery integration.
Setup Complete"

# From Google Cloud import BigQuery
from google.cloud import bigquery

# Create a "Client" object
cliente = bigquery.Client()

# Construct a reference to the "chicago_crimes" dataset
dataset_ref = client.dataset("chicago_crime", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Q1. How many tables are in the Chicago Crime dataset? 
tables = list(client.list_tables(dataset))
for table in tables:
	print(table.table_id)
num_tables = 1
q_1.check()

# Q2. How many columns in the 'crime' table have 'TIMESTAMP' data type?
# Construct a reference to the "crime" table
table_ref = dataset_ref("crime")

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns in the "crime" table in the "chicago_crime" dataset
table.schema
num_timestamp_fields = 2
q_2.check()

# Q3. If you wanted to create a map with a dot at the location of each crime, what are the names of the two fields you likely need to pull out of the 'crime' table to plot the crimes on a map?
fields_for_plotting = ['latitude', 'longitude']
q_3.check()

# Preview the first five lines of the 'crime' table
client.list_rows(table, max_results=5).to_dataframe()

"""
	unique_key	case_number	date	block	iucr	primary_type	description	location_description	arrest	domestic	...	ward	community_area	fbi_code	x_coordinate	y_coordinate	year	updated_on	latitude	longitude	location
0	26345	JE395709	2021-10-02 06:44:00+00:00	001XX E 107TH ST	0110	HOMICIDE	FIRST DEGREE MURDER	HOUSE	False	False	...	9	49	01A	1178985.0	1834047.0	2021	2022-09-18 04:45:51+00:00	41.699915	-87.620245	(41.699915337, -87.620245268)
1	6063232	HP161447	2008-02-06 01:00:00+00:00	105XX S MICHIGAN AVE	0281	CRIM SEXUAL ASSAULT	NON-AGGRAVATED	VEHICLE NON-COMMERCIAL	False	False	...	9	49	02	1178853.0	1835179.0	2008	2018-02-10 03:50:01+00:00	41.703025	-87.620694	(41.7030247, -87.620694297)
2	8308596	HT527097	2011-10-04 01:25:00+00:00	106XX S PERRY AVE	0313	ROBBERY	ARMED: OTHER DANGEROUS WEAPON	STREET	False	False	...	34	49	03	1177490.0	1834194.0	2011	2016-02-04 06:33:39+00:00	41.700353	-87.625715	(41.700352563, -87.62571489)
3	12135953	JD332831	2020-08-14 11:04:00+00:00	103XX S HARVARD AVE	0313	ROBBERY	ARMED - OTHER DANGEROUS WEAPON	STREET	False	False	...	34	49	03	1175628.0	1836454.0	2020	2020-08-21 03:53:36+00:00	41.706596	-87.632465	(41.706596109, -87.632465439)
4	3400725	HK457739	2004-06-27 03:28:00+00:00	007XX E 104TH PL	0313	ROBBERY	ARMED: OTHER DANGEROUS WEAPON	ALLEY	False	False	...	9	50	03	1182778.0	1835817.0	2004	2018-02-28 03:56:25+00:00	41.704685	-87.606302	(41.704685428, -87.60630227)
5 rows × 22 columns
"""

# Preview the first five entries in the "location" column of the "crime" table
client.list_rows(table, selected_fields=table.schema[21:], max_results=5).to_dataframe()

"""
   unique_key case_number                      date                 block  \
0       26345    JE395709 2021-10-02 06:44:00+00:00      001XX E 107TH ST   
1     6063232    HP161447 2008-02-06 01:00:00+00:00  105XX S MICHIGAN AVE   
2     8308596    HT527097 2011-10-04 01:25:00+00:00     106XX S PERRY AVE   
3    12135953    JD332831 2020-08-14 11:04:00+00:00   103XX S HARVARD AVE   
4     3400725    HK457739 2004-06-27 03:28:00+00:00      007XX E 104TH PL   

   iucr         primary_type                     description  \
0  0110             HOMICIDE             FIRST DEGREE MURDER   
1  0281  CRIM SEXUAL ASSAULT                  NON-AGGRAVATED   
2  0313              ROBBERY   ARMED: OTHER DANGEROUS WEAPON   
3  0313              ROBBERY  ARMED - OTHER DANGEROUS WEAPON   
4  0313              ROBBERY   ARMED: OTHER DANGEROUS WEAPON   

     location_description  arrest  domestic  ...  ward  community_area  \
0                   HOUSE   False     False  ...     9              49   
1  VEHICLE NON-COMMERCIAL   False     False  ...     9              49   
2                  STREET   False     False  ...    34              49   
3                  STREET   False     False  ...    34              49   
4                   ALLEY   False     False  ...     9              50   

   fbi_code  x_coordinate y_coordinate  year                updated_on  \
0       01A     1178985.0    1834047.0  2021 2022-09-18 04:45:51+00:00   
1        02     1178853.0    1835179.0  2008 2018-02-10 03:50:01+00:00   
2        03     1177490.0    1834194.0  2011 2016-02-04 06:33:39+00:00   
3        03     1175628.0    1836454.0  2020 2020-08-21 03:53:36+00:00   
4        03     1182778.0    1835817.0  2004 2018-02-28 03:56:25+00:00   

    latitude  longitude                       location  
0  41.699915 -87.620245  (41.699915337, -87.620245268)  
1  41.703025 -87.620694    (41.7030247, -87.620694297)  
2  41.700353 -87.625715   (41.700352563, -87.62571489)  
3  41.706596 -87.632465  (41.706596109, -87.632465439)  
4  41.704685 -87.606302   (41.704685428, -87.60630227)  

[5 rows x 22 columns]

location
0	(41.699915337, -87.620245268)
1	(41.7030247, -87.620694297)
2	(41.700352563, -87.62571489)
3	(41.706596109, -87.632465439)
4	(41.704685428, -87.60630227)
"""

# |----- Theory 02 -----|

from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "openaq" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the "openaq" dataset
tables = list(client.list_tables(dataset))

# Print name of all tables in the dataset (there's only one)
for table in tables:
	print(table.table_id)
#global_air_quality

# Construct a reference to the "global_air_quality" table
table_ref = dataset_ref.table("global_air_quality")

# API request - fetch the table
table = client.get_table(table_ref)
client.list_rows(table, max_results=5).to_dataframe()

"""
	location	city	country	pollutant	value	timestamp	unit	source_name	latitude	longitude	averaged_over_in_hours	location_geom
0	Borówiec, ul. Drapałka	Borówiec	PL	bc	0.85217	2022-04-28 07:00:00+00:00	µg/m³	GIOS	1.0	52.276794	17.074114	POINT(52.276794 1)
1	Kraków, ul. Bulwarowa	Kraków	PL	bc	0.91284	2022-04-27 23:00:00+00:00	µg/m³	GIOS	1.0	50.069308	20.053492	POINT(50.069308 1)
2	Płock, ul. Reja	Płock	PL	bc	1.41000	2022-03-30 04:00:00+00:00	µg/m³	GIOS	1.0	52.550938	19.709791	POINT(52.550938 1)
3	Elbląg, ul. Bażyńskiego	Elbląg	PL	bc	0.33607	2022-05-03 13:00:00+00:00	µg/m³	GIOS	1.0	54.167847	19.410942	POINT(54.167847 1)
4	Piastów, ul. Pułaskiego	Piastów	PL	bc	0.51000	2022-05-11 05:00:00+00:00	µg/m³	GIOS	1.0	52.191728	20.837489	POINT(52.191728 1)

"""

# Query to select all the items from the "city" column where the "country" column is 'US'
query = """
	SELECT city
	FROM `bigquery-public-data.openaq.global_air_quality`
	WHERE country = 'US'
"""

# Create a "Client" object
client = bigquery.Client()

# Set up the query
query_job = client.query(query)

# API request - run the query, and return a panda DataFrame
us_cities = query_job.to_dataframe()

# What five cities have the most measurements?
us_cities.city.value_counts().head()

"""
Phoenix-Mesa-Scottsdale                     39414
Los Angeles-Long Beach-Santa Ana            27479
Riverside-San Bernardino-Ontario            26887
New York-Northern New Jersey-Long Island    25417
San Francisco-Oakland-Fremont               22710
Name: city, dtype: int64
"""

# If you want multiple columns, you can select them with a comma between the names:
query = """
	SELECT city, country
	FROM `bigquery-public-data.openaq.global_air_quality`
	WHERE country = 'US' 
"""

# You can select all columns with a * like this:
query = """
	SELECT *
	FROM `bigquery-public-data.openaq.global_air_quality` 
	WHERE country = 'US'
"""

# Query to get the score column from every row where the type column has value "job"
query = """
	SELECT score, title
	FROM `bigquery-public-data.openaq.global_air_quality`
	WHERE type = "job"
"""

# Create a QueryJobConfig object to estimate size of query without running it...
dry_run_config = bigquery.QueryJobConfig(dry_run=True)

# API request - dry run query to estimate costs
dry_run_query_job = client.query(query, job_config=dry_run_config)

print("This query will process{} bytes.".format(dry_run_query_job.total_bytes_processed))
	# This query will process 553320240 bytes.

# Only run the query if it's less than 1 Mb
ONE_MB = 1000*1000
safe_config = bigquery.QueryJobConfig()maximum_bytes_billed=ONE_MB

# Set up the query (will only run if it's less than 1 Mb)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
safe_query_job.to_dataframe()

# In this case, the query was cancelled, because the limit of 1 MB was exceeded. However, we can increase the limit to run the query successfully!

# Onyle run the query if it's less than 1 Gb
ONE_GB = 1000*1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_GB)

# Set up the query (will only run if it's less than 1 Gb)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame 
job_post_scores = safe_query_job.to_dataframe()

# Print average score for job posts
job_post_scores.score.mean()
#1.7267060367454068

# |----- Exercise 02 -----|

# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex2 import *
print("Setup Complete")

# From Google Cloud import BigQuery
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "openaq" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-dataset")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "global_air_quality" table
table_ref = dataset_ref.table("global_air_quality")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "global_air_quality" table 
client.list_rows(table, max_results=5).to_dataframe()

"""
	location	city	country	pollutant	value	timestamp	unit	source_name	latitude	longitude	averaged_over_in_hours	location_geom
0	Borówiec, ul. Drapałka	Borówiec	PL	bc	0.85217	2022-04-28 07:00:00+00:00	µg/m³	GIOS	1.0	52.276794	17.074114	POINT(52.276794 1)
1	Kraków, ul. Bulwarowa	Kraków	PL	bc	0.91284	2022-04-27 23:00:00+00:00	µg/m³	GIOS	1.0	50.069308	20.053492	POINT(50.069308 1)
2	Płock, ul. Reja	Płock	PL	bc	1.41000	2022-03-30 04:00:00+00:00	µg/m³	GIOS	1.0	52.550938	19.709791	POINT(52.550938 1)
3	Elbląg, ul. Bażyńskiego	Elbląg	PL	bc	0.33607	2022-05-03 13:00:00+00:00	µg/m³	GIOS	1.0	54.167847	19.410942	POINT(54.167847 1)
4	Piastów, ul. Pułaskiego	Piastów	PL	bc	0.51000	2022-05-11 05:00:00+00:00	µg/m³	GIOS	1.0	52.191728	20.837489	POINT(52.191728 1)
"""

# Q1. Which countries have reported pollution levels in units of "ppm"? In the code cell below, set first_query to an SQL query that pulls the appropriate entries from the country column.

# Query to select countries with units of "ppm"
first_query = """ 
	SELECT country
	FROM `bigquery-public-data.openaq.global_air_quality`
	WHERE unit = "ppm"
"""

# Or to get each country just once, you could use
first_query = """
	SELECT DISTINCT country
	FROM `bigquery-public-data.openaq.global_air_quality`
	WHERE unit = "ppm"
"""

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
first_query_job = client.query(first_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
first_results = first_query_job.to_dataframe()

# View top few rows of results
print(first_results.head())

# Check your answer
q_1.check()

"""
  country
0      AR
1      IL
2      TW
3      CO
4      EC
Correct
"""

# Q2. Which pollution levels were reported to be exactly 0?
#Set zero_pollution_query to select all columns of the rows where the value column is 0.
#Set zero_pollution_results to a pandas DataFrame containing the query results.

# Query to select all columns where pollution levels are exactly 0
zero_pollution_query = """
	SELECT *
	FROM `bigquery-public-data.openaq.global_air_quality` 
	WHERE value = 0
"""

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(zero_pollution_query, job_config=safe_config)

# API request - run the query and return a pandas DataFrame
zero_pollution_results = query_job.to_dataframe() 

print(zero_pollution_results.head())

# Check your answer
q_2.check()

"""
                       location      city country pollutant  value  \
0     Zielonka, Bory Tucholskie  Zielonka      PL        bc    0.0   
1    Toruń, ul. Przy Kaszowniku     Toruń      PL        bc    0.0   
2           Kielce, ul. Targowa    Kielce      PL        bc    0.0   
3     Zielonka, Bory Tucholskie  Zielonka      PL        bc    0.0   
4  Koszalin, ul. Armii Krajowej  Koszalin      PL        bc    0.0   

                  timestamp   unit source_name  latitude  longitude  \
0 2022-04-29 14:00:00+00:00  µg/m³        GIOS       1.0  53.662136   
1 2022-04-19 04:00:00+00:00  µg/m³        GIOS       1.0  53.017628   
2 2022-05-07 17:00:00+00:00  µg/m³        GIOS       1.0  50.878998   
3 2022-05-19 14:00:00+00:00  µg/m³        GIOS       1.0  53.662136   
4 2022-05-12 20:00:00+00:00  µg/m³        GIOS       1.0  54.193986   

   averaged_over_in_hours       location_geom  
0               17.933986  POINT(53.662136 1)  
1               18.612808  POINT(53.017628 1)  
2               20.633692  POINT(50.878998 1)  
3               17.933986  POINT(53.662136 1)  
4               16.172544  POINT(54.193986 1)  
Correct
"""

# |----- Theory 03 -----|

from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "comments" table
table_ref = dataset_ref.table("comments")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "comments" table
client.list_rows(table, max_results=5).to_dataframe()

"""
	id	by	author	time	time_ts	text	parent	deleted	dead	ranking
0	9734136	None	None	1434565400	2015-06-17 18:23:20+00:00	None	9733698	True	None	0
1	4921158	None	None	1355496966	2012-12-14 14:56:06+00:00	None	4921100	True	None	0
2	7500568	None	None	1396261158	2014-03-31 10:19:18+00:00	None	7499385	True	None	0
3	8909635	None	None	1421627275	2015-01-19 00:27:55+00:00	None	8901135	True	None	0
4	9256463	None	None	1427204705	2015-03-24 13:45:05+00:00	None	9256346	True	None	0
"""

# Query to select comments that received more than 10 replies
query_popular = """
                SELECT parent, COUNT(id)
                FROM `bigquery-public-data.hacker_news.comments`
                GROUP BY parent
                HAVING COUNT(id) > 10
                """

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_popular, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
popular_comments = query_job.to_dataframe()

# Print the first five rows of the DataFrame
popular_comments.head()

"""
	parent	f0_
0	5463210	55
1	8232019	39
2	4821689	45
3	5865935	38
4	8296879	53
"""

# Improved version of earlier query, now with aliasing & improved readability
query_improved = """
                 SELECT parent, COUNT(1) AS NumPosts
                 FROM `bigquery-public-data.hacker_news.comments`
                 GROUP BY parent
                 HAVING COUNT(1) > 10
                 """

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_improved, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
improved_df = query_job.to_dataframe()

# Print the first five rows of the DataFrame
improved_df.head()

"""
	parent	NumPosts
0	8799572	139
1	6738893	38
2	7740226	38
3	5482178	62
4	4467603	70
"""

query_good = """
             SELECT parent, COUNT(id)
             FROM `bigquery-public-data.hacker_news.comments`
             GROUP BY parent
             """

# And this query won't work, because the author column isn't passed to an aggregate function or a GROUP BY clause:

query_bad = """
            SELECT author, parent, COUNT(id)
            FROM `bigquery-public-data.hacker_news.comments`
            GROUP BY parent
            """

# If make this error, you'll get the error message SELECT list expression references column (column's name) which is neither grouped nor aggregated at.


# |----- Exercise 03 -----|

# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex3 import *
print("Setup Complete")

# from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "comments" table
table_ref = dataset_ref.table("comments")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "comments" table
client.list_rows(table, max_results=5).to_dataframe()

"""
	id	by	author	time	time_ts	text	parent	deleted	dead	ranking
0	9734136	None	None	1434565400	2015-06-17 18:23:20+00:00	None	9733698	True	None	0
1	4921158	None	None	1355496966	2012-12-14 14:56:06+00:00	None	4921100	True	None	0
2	7500568	None	None	1396261158	2014-03-31 10:19:18+00:00	None	7499385	True	None	0
3	8909635	None	None	1421627275	2015-01-19 00:27:55+00:00	None	8901135	True	None	0
4	9256463	None	None	1427204705	2015-03-24 13:45:05+00:00	None	9256346	True	None	0
"""

# Q1. Hacker News would like to send awards to everyone who has written more than 10,000 posts. Write a query that returns all authors with more than 10,000 posts as well as their post counts. Call the column with post counts NumPosts.
# Query to select prolific commenters and post counts
prolific_commenters_query = """
	SELECT author, COUNT(1) AS NumPosts
	FROM `bigquery-public-data.hacker_news.comments` 
	GROUP BY author
	HAVING COUNT() > 10000
"""

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(prolific_commenters_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
prolific_commenters = query.job.to_dataframe()

# View top few rows of results
print(prolific_commenters.head())

q_1.check()

"""
       author  NumPosts
0        None    227736
1     tptacek     33839
2   anigbrowl     11395
3  stcredzero     10092
4       DanBC     12902
Correct
"""

# Q2. How many comments have been deleted? (If a comment was deleted, the `deleted` column in the comments table will have the value `True`.)
# Query to determine how many posts were deleted
deleted_posts_query = """
	SELECT COUNT(1) AS num_deleted_posts
	FROM `bigquery-public-data.hacker_news.comments` 
	WHERE deleted = True
"""

# Set up the query
query_job = client.query(deleted_posts_query)

# API request - run the query, and return a pandas DataFrame
deleted_posts = query_job.to_dataframe()

# View results
print(deleted_posts)

"""
   num_deleted_posts
0             227736
"""

num_deleted_posts = 227736 # Put your answer here

# Check your answer
q_2.check()
"""
Correct
"""


# |----- Theory 04 -----|

from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "nhtsa_traffic_fatalities" dataset
dataset_ref = client.dataset("nhtsa_traffic_fatalities", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "accident_2015" table
table_ref = dataset_ref.table("accident_2015")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "accident_2015" table
client.list_rows(table, max_results=5).to_dataframe()

"""
	state_number	state_name	consecutive_number	number_of_vehicle_forms_submitted_all	number_of_motor_vehicles_in_transport_mvit	number_of_parked_working_vehicles	number_of_forms_submitted_for_persons_not_in_motor_vehicles	number_of_persons_not_in_motor_vehicles_in_transport_mvit	number_of_persons_in_motor_vehicles_in_transport_mvit	number_of_forms_submitted_for_persons_in_motor_vehicles	...	minute_of_ems_arrival_at_hospital	related_factors_crash_level_1	related_factors_crash_level_1_name	related_factors_crash_level_2	related_factors_crash_level_2_name	related_factors_crash_level_3	related_factors_crash_level_3_name	number_of_fatalities	number_of_drunk_drivers	timestamp_of_crash
0	30	Montana	300019	5	5	0	0	0	7	7	...	45	0	None	0	None	0	None	1	0	2015-03-28 14:58:00+00:00
1	39	Ohio	390099	7	7	0	0	0	15	15	...	24	27	Backup Due to Prior Crash	0	None	0	None	1	0	2015-02-14 11:19:00+00:00
2	49	Utah	490123	16	16	0	0	0	28	28	...	99	0	None	0	None	0	None	1	0	2015-04-14 12:24:00+00:00
3	48	Texas	481184	6	5	1	0	5	5	10	...	99	0	None	0	None	0	None	1	0	2015-05-27 16:40:00+00:00
4	41	Oregon	410333	11	11	0	0	0	14	14	...	99	0	None	0	None	0	None	1	0	2015-11-17 18:17:00+00:00
5 rows × 70 columns
"""

# Query to find out the number of accidents for each day of the week
query = """
	SELECT COUNT(consecutive_numbers) AS num_accidents,
		EXTRACT(DAYOFWEEK FROM timestamp_of_crash) AS day_of_week
	FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
	GROUP BY day_of_week
	ORDER BY num_accidents DESC
"""
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = biquery.QueryJobConfig(maximum_bytes_billed=10**9)
query_job = client.query(query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
accidents_by_day = query_job.to_dataframe()

# Print the DataFrame
accidents_by_day

""" 
	num_accidents	day_of_week
0	5659	7
1	5298	1
2	4916	6
3	4460	5
4	4182	4
5	4038	2
6	3985	3
# To map the numbers returned for the day_of_week column to the actual day, you might consult the BigQuery documentation on the DAYOFWEEK function. It says that it returns "an integer between 1 (Sunday) and 7 (Saturday), inclusively". So, in 2015, most fatal motor accidents in the US occured on Sunday and Saturday, while the fewest happened on Tuesday.
"""

# |----- Exercise 04 -----|










