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

# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex4 import *
print("Setup Complete")

# The World Bank has made tons of interesting education data available through BigQuery. Run the following cell to see the first few rows of the international_education table from the world_bank_intl_education dataset.
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "world_bank_intl_education" dataset
dataset_ref = client.dataset("world_bank_intl_education", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "international_education" table
table_ref = dataset_ref.table("international_education")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "international_education" table
client.list_rows(table, max_results=5).to_dataframe()

"""
	country_name	country_code	indicator_name	indicator_code	value	year
0	Chad	TCD	Enrolment in lower secondary education, both s...	UIS.E.2	321921.0	2012
1	Chad	TCD	Enrolment in upper secondary education, both s...	UIS.E.3	68809.0	2006
2	Chad	TCD	Enrolment in upper secondary education, both s...	UIS.E.3	30551.0	1999
3	Chad	TCD	Enrolment in upper secondary education, both s...	UIS.E.3	79784.0	2007
4	Chad	TCD	Repeaters in primary education, all grades, bo...	UIS.R.1	282699.0	2006
"""

# One interesting indicator code is SE.XPD.TOTL.GD.ZS, which corresponds to "Government expenditure on education as % of GDP (%)".
# Which countries spend the largest fraction of GDP on education?
# To answer this question, consider only the rows in the dataset corresponding to indicator code SE.XPD.TOTL.GD.ZS, and write a query that returns the average value in the value column for each country in the dataset between the years 2010-2017 (including 2010 and 2017 in the average).
# Requirements:
#	Your results should have the country name rather than the country code. You will have one row for each country.
#	The aggregate function for average is AVG(). Use the name avg_ed_spending_pct for the column created by this aggregation.
#	Order the results so the countries that spend the largest fraction of GDP on education show up first.

# Your code goes here
country_spend_pct_query = """
	SELECT country_name, AVG(value) AS avg_ed_spending_pct
	FROM `bigquery-public-data.world_bank_intl_education.international_education`
	WHERE indicator_code = 'SE.XPD.TOTL.GD.ZS' and year >= 2010 and year <= 2017
	GROUP BY country_name
	ORDER BY avg_ed_spending_pct DESC
"""

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
country_spend_pct_query_job = client.query(country_spend_pct_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
country_spending_results = country_spend_pct_query_job.to_dataframe()

# View top few rows of results
print(country_spending_results.head())

# Check your answer
q_1.check()

"""
            country_name  avg_ed_spending_pct
0                   Cuba            12.837270
1  Micronesia, Fed. Sts.            12.467750
2        Solomon Islands            10.001080
3                Moldova             8.372153
4                Namibia             8.349610
"""

# There are 1000s of codes in the dataset, so it would be time consuming to review them all. But many codes are available for only a few countries. When browsing the options for different codes, you might restrict yourself to codes that are reported by many countries.
# Write a query below that selects the indicator code and indicator name for all codes with at least 175 rows in the year 2016.
# Requirements:
#	You should have one row for each indicator code.
#	The columns in your results should be called indicator_code, indicator_name, and num_rows.
#	Only select codes with 175 or more rows in the raw database (exactly 175 rows would be included).
#	To get both the indicator_code and indicator_name in your resulting DataFrame, you need to include both in your SELECT statement (in addition to a COUNT() aggregation). This requires you to include both in your GROUP BY clause.
#	Order from results most frequent to least frequent.

# Your code goes here
code_count_query = """
	SELECT indicator_code, indicator_name, COUNT(1) AS num_rows
	FROM `bigquery-public-data.world_bank_intl_education.international_education`
	WHERE year = 2016
	GROUP BY indicator_name, indicator_code
	HAVING COUNT(1) >= 175
	ORDER BY COUNT(1) DESC
"""

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
code_count_query_job = client.query(code_count_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
code_count_results = code_count_query_job.to_dataframe()

# View top few rows of results
print(code_count_results.head())

# Check your answer
q_2.check()

"""
      indicator_code                       indicator_name  num_rows
0        SP.POP.TOTL                    Population, total       232
1        SP.POP.GROW         Population growth (annual %)       232
2     IT.NET.USER.P2      Internet users (per 100 people)       223
3  SP.POP.TOTL.FE.ZS      Population, female (% of total)       213
4        SH.DYN.MORT  Mortality rate, under-5 (per 1,000)       213
"""


# |----- Theory 05 -----|

# We're going to use a CTE to find out how many Bitcoin transactions were made each day for the entire timespan of a bitcoin transaction dataset.
# We'll investigate the transactions table. Here is a view of the first few rows.
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "crypto_bitcoin" dataset
dataset_ref = client.dataset("crypto_bitcoin", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "transactions" table
table_ref = dataset_ref.table("transactions")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "transactions" table
client.list_rows(table, max_results=5).to_dataframe()

"""
	hash	size	virtual_size	version	lock_time	block_hash	block_number	block_timestamp	block_timestamp_month	input_count	output_count	input_value	output_value	is_coinbase	fee	inputs	outputs
0	a16f3ce4dd5deb92d98ef5cf8afeaf0775ebca408f708b...	275	275	1	0	00000000dc55860c8a29c58d45209318fa9e9dc2c1833a...	181	2009-01-12 06:02:13+00:00	2009-01-01	1	2	4000000000.000000000	4000000000.000000000	False	0E-9	[{'index': 0, 'spent_transaction_hash': 'f4184...	[{'index': 0, 'script_asm': '04b5abd412d4341b4...
1	591e91f809d716912ca1d4a9295e70c3e78bab077683f7...	275	275	1	0	0000000054487811fc4ff7a95be738aa5ad9320c394c48...	182	2009-01-12 06:12:16+00:00	2009-01-01	1	2	3000000000.000000000	3000000000.000000000	False	0E-9	[{'index': 0, 'spent_transaction_hash': 'a16f3...	[{'index': 0, 'script_asm': '0401518fa1d1e1e3e...
2	12b5633bad1f9c167d523ad1aa1947b2732a865bf5414e...	276	276	1	0	00000000f46e513f038baf6f2d9a95b2a28d8a6c985bcf...	183	2009-01-12 06:34:22+00:00	2009-01-01	1	2	2900000000.000000000	2900000000.000000000	False	0E-9	[{'index': 0, 'spent_transaction_hash': '591e9...	[{'index': 0, 'script_asm': '04baa9d3665315562...
3	828ef3b079f9c23829c56fe86e85b4a69d9e06e5b54ea5...	276	276	1	0	00000000fb5b44edc7a1aa105075564a179d65506e2bd2...	248	2009-01-12 20:04:20+00:00	2009-01-01	1	2	2800000000.000000000	2800000000.000000000	False	0E-9	[{'index': 0, 'spent_transaction_hash': '12b56...	[{'index': 0, 'script_asm': '04bed827d37474bef...
4	35288d269cee1941eaebb2ea85e32b42cdb2b04284a56d...	277	277	1	0	00000000689051c09ff2cd091cc4c22c10b965eb8db3ad...	545	2009-01-15 05:48:32+00:00	2009-01-01	1	2	2500000000.000000000	2500000000.000000000	False	0E-9	[{'index': 0, 'spent_transaction_hash': 'd71fd...	[{'index': 0, 'script_asm': '044a656f065871a35...
"""

# Query to select the number of transactions per date, sorted by date
query_with_CTE = """
	WITH time AS
	(
		SELECT DATE(block_timestamp) AS trans_date
		FROM `bigquery-public-data.crypto_bitcoin.transactions`
	)
	SELECT COUNT(1) AS transactions,
		trans_date
	FROM time
	GROUP BY trans_date
	ORDER BY trans_date
"""
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
query_job = client.query(query_with_CTE, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
transactions_by_date = query_job.to_dataframe()

# Print the first five rows
transactions_by_date.head()

"""
	transactions	trans_date
0	1	2009-01-03
1	14	2009-01-09
2	61	2009-01-10
3	93	2009-01-11
4	101	2009-01-12
"""

# Since they're returned sorted, we can easily plot the raw results to show us the number of Bitcoin transactions per day over the whole timespan of this dataset.
transactions_by_date.set_index('trans_date').plot()
<AxesSubplot:xlabel='trans_date'>
	
# As you can see, common table expressions (CTEs) let you shift a lot of your data cleaning into SQL. That's an especially good thing in the case of BigQuery, because it is vastly faster than doing the work in Pandas.


# |----- Exercise 05 -----|

# Get most recent checking code
!pip install -U -t /kaggle/working/ git+https://github.com/Kaggle/learntools.git
# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex5 import *
print("Setup Complete")

"""
Collecting git+https://github.com/Kaggle/learntools.git
  Cloning https://github.com/Kaggle/learntools.git to /tmp/pip-req-build-9o9byrr4
  Running command git clone --filter=blob:none --quiet https://github.com/Kaggle/learntools.git /tmp/pip-req-build-9o9byrr4
  Resolved https://github.com/Kaggle/learntools.git to commit 69bc6daec79619690e758841dc2df35708d226c8
  Preparing metadata (setup.py) ... done
Building wheels for collected packages: learntools
  Building wheel for learntools (setup.py) ... done
  Created wheel for learntools: filename=learntools-0.3.4-py3-none-any.whl size=268981 sha256=45020ef010f98729325fd97f7839de418e49fc608f8fe48b42a23d38e276d8c4
  Stored in directory: /tmp/pip-ephem-wheel-cache-fhk5yk9g/wheels/2f/6c/3c/aa9f50cfb5a862157cb4c7a5b34881828cf45404698255dced
Successfully built learntools
Installing collected packages: learntools
Successfully installed learntools-0.3.4
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv class="ansi-yellow-fg">
Setup Complete
"""

# You'll work with a dataset about taxi trips in the city of Chicago. Run the cell below to fetch the chicago_taxi_trips dataset.
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "chicago_taxi_trips" dataset
dataset_ref = client.dataset("chicago_taxi_trips", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Your code here to find the table name
tables = list(client.list_tables(dataset))
for table in tables:
    print(table.table_id) 
"""
taxi_trips
"""
num_tables = 1

# Write the table name as a string below
table_name = "taxi_trips"

# Check your answer
q_1.check()

# Use the next code cell to peek at the top few rows of the data. Inspect the data and see if any issues with data quality are immediately obvious.

# Your code here
# Construct a reference to the "taxi_trips" table
table_ref = dataset_ref.table("taxi_trips")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "taxi_trips" table
client.list_rows(table, max_results=5).to_dataframe()

"""
	unique_key	taxi_id	trip_start_timestamp	trip_end_timestamp	trip_seconds	trip_miles	pickup_census_tract	dropoff_census_tract	pickup_community_area	dropoff_community_area	...	extras	trip_total	payment_type	company	pickup_latitude	pickup_longitude	pickup_location	dropoff_latitude	dropoff_longitude	dropoff_location
0	976397e9c120d15bb6b222b5425071c7e1286f34	f9aee16b2de2973e98d52db66fb4a63a8dfd859ac95f9a...	2019-04-02 11:45:00+00:00	2019-04-02 11:45:00+00:00	581	1.2	NaN	NaN	NaN	NaN	...	0.0	7.00	Cash	Blue Diamond	NaN	NaN	None	NaN	NaN	None
1	7b325318bd14a46641ab10ad5421d98ea7dc4987	298b6be4872d03f31ce998d0c8f78262bd07ce93a1622b...	2016-12-19 13:15:00+00:00	2016-12-19 13:30:00+00:00	624	4.3	NaN	NaN	NaN	NaN	...	0.0	11.40	Cash	303 Taxi	NaN	NaN	None	NaN	NaN	None
2	b2d2eb79755bd3dd86b04c82c688abaeea8fefaa	fe9ca948ad83900c022e7cb67a5b0fe142b4ae7a17c01a...	2017-02-09 07:45:00+00:00	2017-02-09 08:00:00+00:00	322	2.4	NaN	NaN	NaN	NaN	...	0.0	7.00	Cash	303 Taxi	NaN	NaN	None	NaN	NaN	None
3	0de2c16744d3efa6a35bbc3722a0e733f9694bb4	be050281eb37a24f3bc9c0ed8f13541d2a9fabef8a2319...	2016-10-15 10:15:00+00:00	2016-10-15 10:15:00+00:00	13	0.0	NaN	NaN	NaN	NaN	...	0.0	7.01	Credit Card	303 Taxi	NaN	NaN	None	NaN	NaN	None
4	8b348926c0a2aea502b66adc78fbf751720b5900	412cc78b4c891db2d615b68da0ec1292ca399d6d8e51dc...	2016-11-28 17:15:00+00:00	2016-11-28 17:15:00+00:00	612	2.7	NaN	NaN	NaN	NaN	...	0.0	11.00	Credit Card	303 Taxi	NaN	NaN	None	NaN	NaN	None
5 rows × 23 columns
"""
# Some location fields have values of None or NaN. That is a problem if we want to use those fields.

# Q3. Determine when this data is from
# If the data is sufficiently old, we might be careful before assuming the data is still relevant to traffic patterns today. Write a query that counts the number of trips in each year.
# Your results should have two columns:
#	year - the year of the trips
#	num_trips - the number of trips in that year

# Your code goes here
rides_per_year_query = """
                       SELECT EXTRACT(YEAR FROM trip_start_timestamp) AS year, 
                              COUNT(1) AS num_trips
                       FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                       GROUP BY year
                       ORDER BY year
                       """

# Set up the query (cancel the query if it would use too much of your quota)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)

# Your code goes here
rides_per_year_query_job = client.query(rides_per_year_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
rides_per_year_result = rides_per_year_query_job.to_dataframe()

# View results
print(rides_per_year_result)

# Check your answer
q_3.check()

"""
     year  num_trips
0     NaN          1
1  2013.0       5906
2  2014.0    2552428
3  2015.0    7661200
4  2016.0    5476726
5  2017.0     472579
6  2018.0      26800
7  2019.0       9273
8  2022.0       5330
"""

# Q4. Dive slightly deeper
# You'd like to take a closer look at rides from 2016. Copy the query you used above in rides_per_year_query into the cell below for rides_per_month_query. Then modify it in two ways:
# Use a WHERE clause to limit the query to data from 2016.
# Modify the query to extract the month rather than the year.

# Your code goes here
rides_per_month_query = """
	SELECT EXTRACT(MONTH FROM trip_start_timestamp) AS month, COUNT(1) AS num_trips
	FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
	WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2016
	GROUP BY month
	ORDER BY month
""" 

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
# Your code goes here
rides_per_month_query_job = client.query(rides_per_month_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
# Your code goes here
rides_per_month_result = rides_per_month_query_job.to_dataframe()

# View results
print(rides_per_month_result)

# Check your answer
q_4.check()

"""
    month  num_trips
0       1     496237
1       2     509295
2       3     564636
3       4     559003
4       5     559789
5       6     259785
6       7      29063
7       8     715312
8       9      70144
9      10      44572
10     11     760689
11     12     908201
"""

# Q5. It's time to step up the sophistication of your queries. Write a query that shows, for each hour of the day in the dataset, the corresponding number of trips and average speed.
# Your results should have three columns:
#	hour_of_day - sort by this column, which holds the result of extracting the hour from trip_start_timestamp.
#	num_trips - the count of the total number of trips in each hour of the day (e.g. how many trips were started between 6AM and 7AM, independent of which day it occurred on).
#	avg_mph - the average speed, measured in miles per hour, for trips that started in that hour of the day. Average speed in miles per hour is calculated as 3600 * SUM(trip_miles) / SUM(trip_seconds). (The value 3600 is used to convert from seconds to hours.)
# Restrict your query to data meeting the following criteria:
#	a trip_start_timestamp > 2016-01-01 and < 2016-04-01
#	trip_seconds > 0 and trip_miles > 0
# You will use a common table expression (CTE) to select just the relevant rides. Because this dataset is very big, this CTE should select only the columns you'll need to create the final output (though you won't actually create those in the CTE -- instead you'll create those in the later SELECT statement below the CTE).

# Your code goes here
speeds_query = """
               WITH RelevantRides AS
               (
                   SELECT EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day, 
                          trip_miles, 
                          trip_seconds
                   FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                   WHERE trip_start_timestamp > '2016-01-01' AND 
                         trip_start_timestamp < '2016-04-01' AND 
                         trip_seconds > 0 AND 
                         trip_miles > 0
               )
               SELECT hour_of_day, 
                      COUNT(1) AS num_trips, 
                      3600 * SUM(trip_miles) / SUM(trip_seconds) AS avg_mph
               FROM RelevantRides
               GROUP BY hour_of_day
               ORDER BY hour_of_day
               """

# Set up the query (cancel the query if it would use too much of 
# your quota)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
speeds_query_job = client.query(speeds_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
speeds_result = speeds_query_job.to_dataframe()

# View results
print(speeds_result)

# Check your answer
q_5.check()


# |----- Theory 06 -----|
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "github_repos" dataset
dataset_ref = client.dataset("github_repos", project="bigquery-public-data")
# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "licenses" table
licenses_ref = dataset_ref.table("licenses")
# API request - fetch the table
licenses_table = client.get_table(licenses_ref)

# Preview the first five lines of the "licenses" table
client.list_rows(licenses_table, max_results=5).to_dataframe()

"""
	repo_name	license
0	autarch/Dist-Zilla-Plugin-Test-TidyAll	artistic-2.0
1	thundergnat/Prime-Factor	artistic-2.0
2	kusha-b-k/Turabian_Engin_Fan	artistic-2.0
3	onlinepremiumoutlet/onlinepremiumoutlet.github.io	artistic-2.0
4	huangyuanlove/LiaoBa_Service	artistic-2.0
"""

# The second table is the sample_files table, which provides, among other information, the GitHub repo that each file belongs to (in the repo_name column). The first several rows of this table are printed below.

# Construct a reference to the "sample_files" table
files_ref = dataset_ref.table("sample_files")
# API request - fetch the table
files_table = client.get_table(files_ref)

# Preview the first five lines of the "sample_files" table
client.list_rows(files_table, max_results=5).to_dataframe()

"""
	repo_name	ref	path	mode	id	symlink_target
0	EOL/eol	refs/heads/master	generate/vendor/railties	40960	0338c33fb3fda57db9e812ac7de969317cad4959	/usr/share/rails-ruby1.8/railties
1	np/ling	refs/heads/master	tests/success/merger_seq_inferred.t/merger_seq...	40960	dd4bb3d5ecabe5044d3fa5a36e0a9bf7ca878209	../../../fixtures/all/merger_seq_inferred.ll
2	np/ling	refs/heads/master	fixtures/sequence/lettype.ll	40960	8fdf536def2633116d65b92b3b9257bcf06e3e45	../all/lettype.ll
3	np/ling	refs/heads/master	fixtures/failure/wrong_order_seq3.ll	40960	c2509ae1196c4bb79d7e60a3d679488ca4a753e9	../all/wrong_order_seq3.ll
4	np/ling	refs/heads/master	issues/sequence/keep.t	40960	5721de3488fb32745dfc11ec482e5dd0331fecaf	../keep.t
"""

# Next, we write a query that uses information in both tables to determine how many files are released in each license.

# Query to determine the number of files per license, sorted by number of files
query = """
	SELECT L.license, COUNT(1) AS number_of_files
	FROM `bigquery-public-data.github_repos.sample_files` AS sf
	INNER JOIN `bigquery-public-data.github_repos.licenses` AS L
		ON sf.repo_name = L.repo_name
	GROUP BY L.license
	ORDER BY number_of_files DESC
	"""

# Set up the query (cancel the query if it would use too much of your quota, with the limit set to 10 GB)
safe_config = biquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
file_count_by_license = query_job.to_dataframe()

# Print the DataFrame
file_count_by_license

"""
	license	number_of_files
0	mit	20560894
1	gpl-2.0	16608922
2	apache-2.0	7201141
3	gpl-3.0	5107676
4	bsd-3-clause	3465437
5	agpl-3.0	1372100
6	lgpl-2.1	799664
7	bsd-2-clause	692357
8	lgpl-3.0	582277
9	mpl-2.0	457000
10	cc0-1.0	449149
11	epl-1.0	322255
12	unlicense	208602
13	artistic-2.0	147391
14	isc	118332
"""

# |----- Exercise 06 -----|

# Stack Overflow is a widely beloved question and answer site for technical questions. You'll probably use it yourself as you keep using SQL (or any programming language).
# Their data is publicly available. What cool things do you think it would be useful for?
# Here's one idea: You could set up a service that identifies the Stack Overflow users who have demonstrated expertise with a specific technology by answering related questions about it, so someone could hire those experts for in-depth help.
# In this exercise, you'll write the SQL queries that might serve as the foundation for this type of service.
# As usual, run the following cell to set up our feedback system before moving on.

# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex6 import *
print("Setup Complete")
"""
Using Kaggle's public dataset BigQuery integration.
Setup Complete
"""

# Run the next cell to fetch the stackoverflow dataset.
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)
"""
Using Kaggle's public dataset BigQuery integration.
"""

# Q1. Explore the data
# Before writing queries or JOIN clauses, you'll want to see what tables are available.

# Get a list of available tables
tables = list(client.list_tables(dataset))
list_of_tables = [table.table_id for table in tables]

# Print your answer
print(list_of_tables)

# Check your answer
q_1.check()

"""
['badges', 'comments', 'post_history', 'post_links', 'posts_answers', 'posts_moderator_nomination', 'posts_orphaned_tag_wiki', 'posts_privilege_wiki', 'posts_questions', 'posts_tag_wiki', 'posts_tag_wiki_excerpt', 'posts_wiki_placeholder', 'stackoverflow_posts', 'tags', 'users', 'votes']
Correct
"""

# Q2. Review relevant tables
# If you are interested in people who answer questions on a given topic, the posts_answers table is a natural place to look. Run the following cell, and look at the output.

# Construct a reference to the "posts_answers" table
answers_table_ref = dataset_ref.table("posts_answers")

# API request - fetch the table
answers_table = client.get_table(answers_table_ref)

# Preview the first five lines of the "posts_answers" table
client.list_rows(answers_table, max_results=5).to_dataframe()

"""
id	title	body	accepted_answer_id	answer_count	comment_count	community_owned_date	creation_date	favorite_count	last_activity_date	last_edit_date	last_editor_display_name	last_editor_user_id	owner_display_name	owner_user_id	parent_id	post_type_id	score	tags	view_count
0	18	None	<p>For a table like this:</p>\n\n<pre><code>CR...	None	None	2	NaT	2008-08-01 05:12:44.193000+00:00	None	2016-06-02 05:56:26.060000+00:00	2016-06-02 05:56:26.060000+00:00	Jeff Atwood	126039	phpguy	NaN	17	2	59	None	None
1	165	None	<p>You can use a <a href="http://sharpdevelop....	None	None	0	NaT	2008-08-01 18:04:25.023000+00:00	None	2019-04-06 14:03:51.080000+00:00	2019-04-06 14:03:51.080000+00:00	None	1721793	user2189331	NaN	145	2	10	None	None
2	1028	None	<p>The VB code looks something like this:</p>\...	None	None	0	NaT	2008-08-04 04:58:40.300000+00:00	None	2013-02-07 13:22:14.680000+00:00	2013-02-07 13:22:14.680000+00:00	None	395659	user2189331	NaN	947	2	8	None	None
3	1073	None	<p>My first choice would be a dedicated heap t...	None	None	0	NaT	2008-08-04 07:51:02.997000+00:00	None	2015-09-01 17:32:32.120000+00:00	2015-09-01 17:32:32.120000+00:00	None	45459	user2189331	NaN	1069	2	29	None	None
4	1260	None	<p>I found the answer. all you have to do is a...	None	None	0	NaT	2008-08-04 14:06:02.863000+00:00	None	2016-12-20 08:38:48.867000+00:00	2016-12-20 08:38:48.867000+00:00	None	1221571	Jin	NaN	1229	2	1	None	None
"""

# It isn't clear yet how to find users who answered questions on any given topic. But posts_answers has a parent_id column. If you are familiar with the Stack Overflow site, you might figure out that the parent_id is the question each post is answering.
# Look at posts_questions using the cell below.

# Construct a reference to the "posts_questions" table
questions_table_ref = dataset_ref.table("posts_questions")

# API request - fetch the table
questions_table = client.get_table(questions_table_ref)

# Preview the first five lines of the "posts_questions" table
client.list_rows(questions_table, max_results=5).to_dataframe()

"""
id	title	body	accepted_answer_id	answer_count	comment_count	community_owned_date	creation_date	favorite_count	last_activity_date	last_edit_date	last_editor_display_name	last_editor_user_id	owner_display_name	owner_user_id	parent_id	post_type_id	score	tags	view_count
0	320268	Html.ActionLink doesn’t render # properly	<p>When using Html.ActionLink passing a string...	NaN	0	0	NaT	2008-11-26 10:42:37.477000+00:00	0	2009-02-06 20:13:54.370000+00:00	NaT	None	NaN	Paulo	NaN	None	1	0	asp.net-mvc	390
1	324003	Primitive recursion	<p>how will i define the function 'simplify' ...	NaN	0	0	NaT	2008-11-27 15:12:37.497000+00:00	0	2012-09-25 19:54:40.597000+00:00	2012-09-25 19:54:40.597000+00:00	Marcin	1288.0	None	41000.0	None	1	0	haskell|lambda|functional-programming|lambda-c...	497
2	390605	While vs. Do While	<p>I've seen both the blocks of code in use se...	390608.0	0	0	NaT	2008-12-24 01:49:54.230000+00:00	2	2008-12-24 03:08:55.897000+00:00	NaT	None	NaN	Unkwntech	115.0	None	1	0	language-agnostic|loops	11262
3	413246	Protect ASP.NET Source code	<p>Im currently doing some research in how to ...	NaN	0	0	NaT	2009-01-05 14:23:51.040000+00:00	0	2009-03-24 21:30:22.370000+00:00	2009-01-05 14:42:28.257000+00:00	Tom Anderson	13502.0	Velnias	NaN	None	1	0	asp.net|deployment|obfuscation	4823
4	454921	Difference between "int[] myArray" and "int my...	<blockquote>\n <p><strong>Possible Duplicate:...	454928.0	0	0	NaT	2009-01-18 10:22:52.177000+00:00	0	2009-01-18 10:30:50.930000+00:00	2017-05-23 11:49:26.567000+00:00	None	-1.0	Evan Fosmark	49701.0	None	1	0	java|arrays	798
"""

# Are there any fields that identify what topic or technology each question is about? If so, how could you find the IDs of users who answered questions about a specific topic?
# Think about it, and then check the solution by running the code in the next cell.

# Check your answer (Run this code cell to receive credit!)
q_2.solution()

"""
Solution: 
- posts_questions has a column called tags which lists the topics/technologies each question is about.
- posts_answers has a column called parent_id which identifies the ID of the question each answer is responding to. posts_answers also has an owner_user_id column which specifies the ID of the user who answered the question.

You can join these two tables to:
	determine the tags for each answer, and then
	select the owner_user_id of the answers on the desired tag.
This is exactly what you will do over the next few questions.
"""

# Q3. Selecting the right questions
# A lot of this data is text.
# We'll explore one last technique in this course which you can apply to this text.
# A WHERE clause can limit your results to rows with certain text using the LIKE feature. For example, to select just the third row of the pets table from the tutorial, we could use the query in the picture below.

#query = """
#	SELECT *
#	FROM `bigquery-public-data.pet_records.pets`
#	WHERE Name LIKE 'Ripley'
#	"""

"""
ID	NAME	ANIMAL
1	Dr. Harris Bonkers	Rabbit
2	Moon	Dog
3	Ripley	Cat
4	Tom	Cat
"""

# You can also use % as a "wildcard" for any number of characters. So you can also get the third row with:
#query = """
#        SELECT * 
#        FROM `bigquery-public-data.pet_records.pets` 
#        WHERE Name LIKE '%ipl%'
#        """

# Try this yourself. Write a query that selects the id, title and owner_user_id columns from the posts_questions table.
# Restrict the results to rows that contain the word "bigquery" in the tags column.
# Include rows where there is other text in addition to the word "bigquery" (e.g., if a row has a tag "bigquery-sql", your results should include that too).

# Your code here
questions_query = """
                  SELECT id, title, owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_questions`
                  WHERE tags LIKE '%bigquery%'
                  """

# Set up the query (cancel the query if it would use too much of your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
questions_query_job = client.query(questions_query, job_config=safe_config) # Your code goes here

# API request - run the query, and return a pandas DataFrame
questions_results = questions_query_job.to_dataframe() # Your code goes here

# Preview results
print(questions_results.head())

# Check your answer
q_3.check()

"""
         id                                              title  owner_user_id
0  64345717  Loop by array and union looped result in BigQuery     13304769.0
1  64610766  BigQuery Transfer jobs from S3 stuck pending o...     14549617.0
2  64383871  How to get sum of values in days intervals usi...     12472644.0
3  64251948                BigQuery get row above empty column      4572124.0
4  64323398  SQL: Remove part of string that is in another ...      6089137.0
Correct
"""

# Q4. Your first join
# Now that you have a query to select questions on any given topic (in this case, you chose "bigquery"), you can find the answers to those questions with a JOIN.
# Write a query that returns the id, body and owner_user_id columns from the posts_answers table for answers to "bigquery"-related questions.
#	You should have one row in your results for each answer to a question that has "bigquery" in the tags.
#	Remember you can get the tags for a question from the tags column in the posts_questions table.
# Hint: Do an INNER JOIN between bigquery-public-data.stackoverflow.posts_questions and bigquery-public-data.stackoverflow.posts_answers.
# Give post_questions an alias of q, and use a as an alias for posts_answers. The ON part of your join is q.id = a.parent_id.

# Your code here
answers_query = """
	SELECT a.id, a.body, a.owner_user_id
	FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
	INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
		ON q.id = a.parent_id
	WHERE q.tags LIKE '%bigquery%'
"""

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=27*10**9)
answers_query_job = client.query(answer_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
answers_results = answers_query_job.to_dataframe()

# Preview results
print(answers_results.head())

# Check your answer
q_4.check()

"""
         id                                               body  owner_user_id
0  57032546  <p>Another solution - same logic, just a diffe...      1391685.0
1  57032790  <p>Recommendation : </p>\n\n<pre><code>select\...     10369599.0
2  57036857  <p>Basically, this is the answer</p>\n\n<pre><...     11506172.0
3  57042242  <p>first, I hope that year+week &amp; year+day...      2929192.0
4  57049923  <p>I think the solutions mentioned by others s...      1391685.0
Correct
"""

# Q5. Answer the question
# You have the merge you need. But you want a list of users who have answered many questions... which requires more work beyond your previous result.
# Write a new query that has a single row for each user who answered at least one question with a tag that includes the string "bigquery". Your results should have two columns:
#	user_id - contains the owner_user_id column from the posts_answers table
#	number_of_answers - contains the number of answers the user has written to "bigquery"-related questions
# Hint: Start with SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
bigquery_experts_query = """
                         SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                         FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                         INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                             ON q.id = a.parent_Id
                         WHERE q.tags LIKE '%bigquery%'
                         GROUP BY a.owner_user_id
                         """

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
bigquery_experts_query_job = client.query(bigquery_experts_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
bigquery_experts_results = bigquery_experts_query_job.to_dataframe()

# Preview results
print(bigquery_experts_results.head())

# Check your answer
q_5.check()

"""
      user_id  number_of_answers
0   4650934.0                  1
1   2157547.0                  1
2   3011380.0                  1
3   9447598.0                  8
4  15745884.0                 24
Correct
"""

# Q6. Building a more generally useful service¶
# How could you convert what you've done to a general function a website could call on the backend to get experts on any topic?
# Think about it and then check the solution below.

def expert_finder(topic, client):
    '''
    Returns a DataFrame with the user IDs who have written Stack Overflow answers on a topic.

    Inputs:
        topic: A string with the topic of interest
        client: A Client object that specifies the connection to the Stack Overflow dataset

    Outputs:
        results: A DataFrame with columns for user_id and number_of_answers. Follows similar logic to bigquery_experts_results shown above.
    '''
    my_query = """
               SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
               FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
               INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                   ON q.id = a.parent_Id
               WHERE q.tags like '%{topic}%'
               GROUP BY a.owner_user_id
               """

    # Set up the query (a real service would have good error handling for 
    # queries that scan too much data)
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)      
    my_query_job = client.query(my_query, job_config=safe_config)

    # API request - run the query, and return a pandas DataFrame
    results = my_query_job.to_dataframe()

    return results





# |----- Theory 07 -----|

"""
JOINs and UNIONs
Combine information from multiple tables.
JOINs and UNIONs

Table of Contents
Introduction
JOINs
UNIONs
Example
Your turn
"""
# Example
# We'll work with the Hacker News dataset. We begin by reviewing the first several rows of the comments table. (The corresponding code is hidden, but you can un-hide it by clicking on the "Code" button below.)
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

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()

"""
	id	by	author	time	time_ts	text	parent	deleted	dead	ranking
0	9734136	None	None	1434565400	2015-06-17 18:23:20+00:00	None	9733698	True	None	0
1	4921158	None	None	1355496966	2012-12-14 14:56:06+00:00	None	4921100	True	None	0
2	7500568	None	None	1396261158	2014-03-31 10:19:18+00:00	None	7499385	True	None	0
3	8909635	None	None	1421627275	2015-01-19 00:27:55+00:00	None	8901135	True	None	0
4	9256463	None	None	1427204705	2015-03-24 13:45:05+00:00	None	9256346	True	None	0
"""

# You'll also work with the stories table.

# Construct a reference to the "stories" table
table_ref = dataset_ref.table("stories")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()
"""
	id	by	score	time	time_ts	title	url	text	deleted	dead	descendants	author
0	6988445	cflick	0	1388454902	2013-12-31 01:55:02+00:00	Appshare	http://chadflick.ws/appshare.html	Did facebook or angrybirds pay you? We will!	None	True	NaN	cflick
1	7047571	Rd2	1	1389562985	2014-01-12 21:43:05+00:00	Java in startups		Hello, hacker news!<p>Have any of you used jav...	None	True	NaN	Rd2
2	9157712	mo0	1	1425657937	2015-03-06 16:05:37+00:00	Show HN: Discover what songs were used in YouT...	http://www.mooma.sh/	The user can paste a media url(currently only ...	None	True	NaN	mo0
3	8127403	ad11	1	1407052667	2014-08-03 07:57:47+00:00	My poker project, what do you think?		Hi guys, what do you think about my poker proj...	None	True	NaN	ad11
4	6933158	emyy	1	1387432701	2013-12-19 05:58:21+00:00	Christmas Crafts Ideas - Easy and Simple Famil...	http://www.winxdvd.com/resource/christmas-craf...	There are some free Christmas craft ideas to m...	None	True	NaN	emyy
"""

# The query below pulls information from the stories and comments tables to create a table showing all stories posted on January 1, 2012, along with the corresponding number of comments. We use a LEFT JOIN so that the results include stories that didn't receive any comments.
# Query to select all stories posted on January 1, 2012, with number of comments
join_query = """
             WITH c AS
             (
             SELECT parent, COUNT(*) as num_comments
             FROM `bigquery-public-data.hacker_news.comments` 
             GROUP BY parent
             )
             SELECT s.id as story_id, s.by, s.title, c.num_comments
             FROM `bigquery-public-data.hacker_news.stories` AS s
             LEFT JOIN c
             ON s.id = c.parent
             WHERE EXTRACT(DATE FROM s.time_ts) = '2012-01-01'
             ORDER BY c.num_comments DESC
             """

# Run the query, and return a pandas DataFrame
join_result = client.query(join_query).result().to_dataframe()
join_result.head()

"""
	story_id	by	title	num_comments
0	3412900	whoishiring	Ask HN: Who is Hiring? (January 2012)			154.0
1	3412901	whoishiring	Ask HN: Freelancer? Seeking freelancer? (Janua...	97.0
2	3412643	jemeshsu	Avoid Apress						30.0
3	3414012	ramanujam	Impress.js - a Prezi like implementation using...	27.0
4	3412891	Brajeshwar	There's no shame in code that is simply "good ...	27.0
"""

# Since the results are ordered by the num_comments column, stories without comments appear at the end of the DataFrame. (Remember that NaN stands for "not a number".)
# None of these stories received any comments
join_result.tail()
"""
	story_id	by	title	num_comments
439	3413041	ORioN63	Solar days, sidereal days, solar years and sid...	NaN
440	3412667	Tez_Dhar	How shall i Learn Hacking			NaN
441	3412783	mmaltiar	Working With Spring Data JPA			NaN
442	3412821	progga	Networking on the Network: A Guide to Professi...	NaN
443	3412930	shipcode	Project Zero Operating System – New Kernel	NaN
"""

# Next, we write a query to select all usernames corresponding to users who wrote stories or comments on January 1, 2014. We use UNION DISTINCT (instead of UNION ALL) to ensure that each user appears in the table at most once.
# Query to select all users who posted stories or comments on January 1, 2014
union_query = """
              SELECT c.by
              FROM `bigquery-public-data.hacker_news.comments` AS c
              WHERE EXTRACT(DATE FROM c.time_ts) = '2014-01-01'
              UNION DISTINCT
              SELECT s.by
              FROM `bigquery-public-data.hacker_news.stories` AS s
              WHERE EXTRACT(DATE FROM s.time_ts) = '2014-01-01'
              """

# Run the query, and return a pandas DataFrame
union_result = client.query(union_query).result().to_dataframe()
union_result.head()
"""
	by
0	learnlivegrow
1	egybreak
2	dclara
3	vram22
4	espeed
"""
# To get the number of users who posted on January 1, 2014, we need only take the length of the DataFrame.
# Number of users who posted stories or comments on January 1, 2014
len(union_result)
""" 2282 """





# |----- Exercise 07 -----|

# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql_advanced.ex1 import *
print("Setup Complete")
"""Setup Complete"""

# from google.cloud import bigquery
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "posts_questions" table
table_ref = dataset_ref.table("posts_questions")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()

"""
	id	title	body	accepted_answer_id	answer_count	comment_count	community_owned_date	creation_date	favorite_count	last_activity_date	last_edit_date	last_editor_display_name	last_editor_user_id	owner_display_name	owner_user_id	parent_id	post_type_id	score	tags	view_count
0	320268	Html.ActionLink doesn’t render # properly	<p>When using Html.ActionLink passing a string...	NaN	0	0	NaT	2008-11-26 10:42:37.477000+00:00	0	2009-02-06 20:13:54.370000+00:00	NaT	None	NaN	Paulo	NaN	None	1	0	asp.net-mvc	390
1	324003	Primitive recursion	<p>how will i define the function 'simplify' ...	NaN	0	0	NaT	2008-11-27 15:12:37.497000+00:00	0	2012-09-25 19:54:40.597000+00:00	2012-09-25 19:54:40.597000+00:00	Marcin	1288.0	None	41000.0	None	1	0	haskell|lambda|functional-programming|lambda-c...	497
2	390605	While vs. Do While	<p>I've seen both the blocks of code in use se...	390608.0	0	0	NaT	2008-12-24 01:49:54.230000+00:00	2	2008-12-24 03:08:55.897000+00:00	NaT	None	NaN	Unkwntech	115.0	None	1	0	language-agnostic|loops	11262
3	413246	Protect ASP.NET Source code	<p>Im currently doing some research in how to ...	NaN	0	0	NaT	2009-01-05 14:23:51.040000+00:00	0	2009-03-24 21:30:22.370000+00:00	2009-01-05 14:42:28.257000+00:00	Tom Anderson	13502.0	Velnias	NaN	None	1	0	asp.net|deployment|obfuscation	4823
4	454921	Difference between "int[] myArray" and "int my...	<blockquote>\n <p><strong>Possible Duplicate:...	454928.0	0	0	NaT	2009-01-18 10:22:52.177000+00:00	0	2009-01-18 10:30:50.930000+00:00	2017-05-23 11:49:26.567000+00:00	None	-1.0	Evan Fosmark	49701.0	None	1	0	java|arrays	798

"""

# We also take a look at the posts_answers table.

# Construct a reference to the "posts_answers" table
table_ref = dataset_ref.table("posts_answers")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the table
client.list_rows(table, max_results=5).to_dataframe()

"""
	id	title	body	accepted_answer_id	answer_count	comment_count	community_owned_date	creation_date	favorite_count	last_activity_date	last_edit_date	last_editor_display_name	last_editor_user_id	owner_display_name	owner_user_id	parent_id	post_type_id	score	tags	view_count
0	18	None	<p>For a table like this:</p>\n\n<pre><code>CR...	None	None	2	NaT	2008-08-01 05:12:44.193000+00:00	None	2016-06-02 05:56:26.060000+00:00	2016-06-02 05:56:26.060000+00:00	Jeff Atwood	126039	phpguy	NaN	17	2	59	None	None
1	165	None	<p>You can use a <a href="http://sharpdevelop....	None	None	0	NaT	2008-08-01 18:04:25.023000+00:00	None	2019-04-06 14:03:51.080000+00:00	2019-04-06 14:03:51.080000+00:00	None	1721793	user2189331	NaN	145	2	10	None	None
2	1028	None	<p>The VB code looks something like this:</p>\...	None	None	0	NaT	2008-08-04 04:58:40.300000+00:00	None	2013-02-07 13:22:14.680000+00:00	2013-02-07 13:22:14.680000+00:00	None	395659	user2189331	NaN	947	2	8	None	None
3	1073	None	<p>My first choice would be a dedicated heap t...	None	None	0	NaT	2008-08-04 07:51:02.997000+00:00	None	2015-09-01 17:32:32.120000+00:00	2015-09-01 17:32:32.120000+00:00	None	45459	user2189331	NaN	1069	2	29	None	None
4	1260	None	<p>I found the answer. all you have to do is a...	None	None	0	NaT	2008-08-04 14:06:02.863000+00:00	None	2016-12-20 08:38:48.867000+00:00	2016-12-20 08:38:48.867000+00:00	None	1221571	Jin	NaN	1229	2	1	None	None
"""

# You will work with both of these tables to answer the questions below.

# Exercises
# 1) How long does it take for questions to receive answers?
# You're interested in exploring the data to have a better understanding of how long it generally takes for questions to receive answers. Armed with this knowledge, you plan to use this information to better design the order in which questions are presented to Stack Overflow users.
# With this goal in mind, you write the query below, which focuses on questions asked in January 2018. It returns a table with two columns:
#	q_id - the ID of the question
#	time_to_answer - how long it took (in seconds) for the question to receive an answer
# Run the query below (without changes), and take a look at the output.

first_query = """
              SELECT q.id AS q_id,
                  MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
              FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                  INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
              ON q.id = a.parent_id
              WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
              GROUP BY q_id
              ORDER BY time_to_answer
              """

first_result = client.query(first_query).result().to_dataframe()
print("Percentage of answered questions: %s%%" % \
      (sum(first_result["time_to_answer"].notnull()) / len(first_result) * 100))
print("Number of questions:", len(first_result))
first_result.head()

"""
Percentage of answered questions: 100.0%
Number of questions: 134719
q_id	time_to_answer
0	48382183	-132444692
1	48174391	0
2	48375126	0
3	48092100	0
4	48102324	0
"""

# You're surprised at the results and strongly suspect that something is wrong with your query. In particular,

# According to the query, 100% of the questions from January 2018 received an answer. But, you know that ~80% of the questions on the site usually receive an answer.
# The total number of questions is surprisingly low. You expected to see at least 150,000 questions represented in the table.
# Given these observations, you think that the type of JOIN you have chosen has inadvertently excluded unanswered questions. Using the code cell below, can you figure out what type of JOIN to use to fix the problem so that the table includes unanswered questions?

# Note: You need only amend the type of JOIN (i.e., INNER, LEFT, RIGHT, or FULL) to answer the question successfully.

# Your code here
correct_query = """
              SELECT q.id AS q_id,
                  MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
              FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                  LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
              ON q.id = a.parent_id
              WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
              GROUP BY q_id
              ORDER BY time_to_answer
              """

# Check your answer
q_1.check()

# Run the query, and return a pandas DataFrame
correct_result = client.query(correct_query).result().to_dataframe()
print("Percentage of answered questions: %s%%" % \
      (sum(correct_result["time_to_answer"].notnull()) / len(correct_result) * 100))
print("Number of questions:", len(correct_result))

"""

q_id	time_to_answer
0	48534629	NaN
1	48483836	NaN
2	48530405	NaN
3	48446591	NaN
4	48342185	NaN
"""
# Correct
# Percentage of answered questions: 83.3368387192557%
# Number of questions: 161656



# 2) Initial questions and answers, Part 1
# You're interested in understanding the initial experiences that users typically have with the Stack Overflow website. Is it more common for users to first ask questions or provide answers? After signing up, how long does it take for users to first interact with the website? To explore this further, you draft the (partial) query in the code cell below.

# The query returns a table with three columns:

#	owner_user_id - the user ID
#	q_creation_date - the first time the user asked a question
#	a_creation_date - the first time the user contributed an answer

# You want to keep track of users who have asked questions, but have yet to provide answers. And, your table should also include users who have answered questions, but have yet to pose their own questions.
# With this in mind, please fill in the appropriate JOIN (i.e., INNER, LEFT, RIGHT, or FULL) to return the correct information.
# Note: You need only fill in the appropriate JOIN. All other parts of the query should be left as-is. (You also don't need to write any additional code to run the query, since the check() method will take care of this for you.)
# To avoid returning too much data, we'll restrict our attention to questions and answers posed in January 2019. We'll amend the timeframe in Part 2 of this question to be more realistic!

# Your code here
q_and_a_query = """
                SELECT q.owner_user_id AS owner_user_id,
                    MIN(q.creation_date) AS q_creation_date,
                    MIN(a.creation_date) AS a_creation_date
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                    FULL JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a 
                ON q.owner_user_id = a.owner_user_id 
                WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-02-01' 
                    AND a.creation_date >= '2019-01-01' AND a.creation_date < '2019-02-01'
                GROUP BY owner_user_id
                """

# Check your answer
q_2.check()

"""
	owner_user_id	q_creation_date	a_creation_date
0	4868132	2019-01-03 14:42:42.477000+00:00	2019-01-06 12:19:33.453000+00:00
1	2430915	2019-01-07 01:21:25.160000+00:00	2019-01-18 05:22:23.530000+00:00
2	10848105	2019-01-04 02:02:15.210000+00:00	2019-01-06 15:15:52.123000+00:00
3	7692562	2019-01-08 15:15:55.523000+00:00	2019-01-14 16:40:36.240000+00:00
4	5897602	2019-01-03 13:24:49.870000+00:00	2019-01-02 18:47:19.087000+00:00
Correct
"""

# Lines below will give you a hint or solution code
q_2.hint()
q_2.solution()

"""
Solution:
q_and_a_query = """
                SELECT q.owner_user_id AS owner_user_id,
                    MIN(q.creation_date) AS q_creation_date,
                    MIN(a.creation_date) AS a_creation_date
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                    FULL JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                ON q.owner_user_id = a.owner_user_id 
                WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-02-01' 
                    AND a.creation_date >= '2019-01-01' AND a.creation_date < '2019-02-01'
                GROUP BY owner_user_id
                """
"""

