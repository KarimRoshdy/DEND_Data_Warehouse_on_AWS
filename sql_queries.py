import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# create staging events data from JSON files
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                  artist text, 
                                  auth text, 
                                  firstName varchar(50), 
                                  gender varchar(10), 
                                  itemInSession varchar,
                                  lastName varchar(50), 
                                  length float, 
                                  level varchar(10), 
                                  location text,
                                  method varchar(10),
                                  page varchar(20),
                                  registration bigint, 
                                  sessionId int,
                                  song text,
                                  status smallint,
                                  ts bigint,
                                  userAgent text,
                                  userId int) """)


# create staging songs data from JSON files
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                  num_songs integer,
                                  artist_id text,
                                  artist_latitude numeric(15,2),
                                  artist_longitude numeric(15,2),
                                  artist_location text,
                                  artist_name text,
                                  song_id text,
                                  title text,
                                  duration numeric(15,2),
                                  year integer) """)



# Creating fact and dimension tables of star schema
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays( 
                            songplay_id int IDENTITY(0,1),
                            start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
                            user_id  INT NOT NULL REFERENCES users(user_id),
                            level VARCHAR(40),
                            song_id VARCHAR(100) NOT NULL REFERENCES songs(song_id),
                            artist_id VARCHAR(100) NOT NULL REFERENCES artists(artist_id),
                            session_id INT ,
                            location VARCHAR(150),
                            user_agent VARCHAR(250),
                            PRIMARY KEY (songplay_id))""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INT NOT NULL, 
                        first_name VARCHAR(50), 
                        last_name VARCHAR(50),
                        gender VARCHAR(10), 
                        level VARCHAR(50),
                        PRIMARY KEY (user_id))""")


song_table_create = ("""CREATE TABLE  IF NOT EXISTS songs(
                        song_id VARCHAR(100) NOT NULL ,
                        title VARCHAR(200),
                        artist_id VARCHAR(100),
                        year INT,
                        duration numeric(15,2),
                        PRIMARY KEY (song_id))""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                          artist_id VARCHAR(100) NOT NULL ,
                          artist_name VARCHAR(255) ,
                          artist_location VARCHAR(255),
                          artist_latitude numeric(15,2),
                          artist_longitude numeric(15,2),
                          PRIMARY KEY (artist_id)) """)


time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP NOT NULL,
                        hour INT,
                        day INT,
                        week INT, 
                        month INT, 
                        year INT,
                        weekday INT,
                        PRIMARY KEY (start_time)) """)



# Copy data from S3 buckets to staging tables

staging_events_copy = (""" copy staging_events from {}
                           credentials 'aws_iam_role={}'
                           format as json {}
                           region 'us-west-2' """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE', 'ARN'), 
                                                          config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy =  ("""copy staging_songs from {}
                          credentials 'aws_iam_role={}'
                          format as json 'auto' 
                          region 'us-west-2' truncatecolumns;""").format(config.get('S3', 'SONG_DATA'),
                                                                         config.get('IAM_ROLE', 'ARN'))


# Inserting data from staging tables to fact and dimension tables 

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, 
                                                    session_id, location, user_agent)
                             SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                                    se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location,
                                    se.userAgent
                             FROM staging_events se
                             JOIN staging_songs ss ON se.artist=ss.artist_name 
                             WHERE se.page = 'NextSong' """)



user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
                         SELECT DISTINCT(userId), firstName, lastName, gender, level
                         FROM staging_events AS se 
                         WHERE se.page = 'NextSong' """)



song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
                         SELECT DISTINCT(song_id), title, artist_id, year, duration
                         FROM   staging_songs """)



artist_table_insert = (""" INSERT INTO artists (artist_id, artist_name, artist_location, 
                                                artist_latitude, artist_longitude)
                           SELECT DISTINCT(artist_id), artist_name, artist_location,
                                           artist_latitude, artist_longitude
                           FROM   staging_songs """)



time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        
                        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                                EXTRACT(hour from start_time) as hour,
                                EXTRACT(day from start_time) as day,
                                EXTRACT(week from start_time) as week,
                                EXTRACT(month from start_time) as month,
                                EXTRACT(year from start_time) as year,
                                EXTRACT(dow from start_time) as weekday    
                        FROM staging_events """)





# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,  songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
