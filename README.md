# Summary of the Project
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Tasks
1. Create staging tables to host JSON data from S3 buckets.
2. Create fact and dimension tables of a star schema.
2. Build an ETL pipeline for a Sparkify database hosted on Amazon Redshift.

# Staging tables
extracting data from JSON files to these tables to be inserted later into fact and dimension tables 
### staging_events
- Records: artist, auth, firstName,gender, itemInSession, lastName, length, level, location, method, page,registration, sessionId, song, status, ts, userAgent, userId
           
### staging_songs
- Records: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year
                              
                              
# Star Schema design
### Fact Table: 
songplays
- Records: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.
- Primary Key: songplay_id.
- Foreign Keys: start_time, user_id, song_id, artist_id.

### Dimension Tables:

users
* Records: first_name, last_name, gender, level.
* Primary key: user_id.

songs
* Records: title, artist_id, year, duration.
* Primary key: song_id.

artists
* Records: artist_name, artist_location, artist_latitude, artist_longitude.
* Primary key: artist_id.

time
* Records: hour, day, week, month, year, weekday.
* Primary key: start_time. 


# Data Explanation
* create_tables.py: drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
* etl.py: reads and processes files from S3 buckets to staging tables and then to fact and dimension tables.
* sql_queries.py: contains all sql queries to create, drop and inserting tables.
* dwh.cfg: configuration file to access the database and cluster on AWS Redshift.
* dwh-cluster.cfg: configuration file to create a cluster and database.
* create_cluster.ipynb: contains all steps to create a cluster on AWS and accessing it.

# How to run:
1. Create a cluster using dwh-cluster.cfg and create_cluster.ipynb files.
2. Run the following command on the terminal to create the tables 
    "python create_tables.py". 
3. Run ETL source code to extract and inserts the data from json files to tables
    "python etl.py".