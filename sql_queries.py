import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events" (
    "artist" VARCHAR(200),
    "auth" VARCHAR(12),
    "firstName" VARCHAR(30),
    "gender" VARCHAR(1),
    "itemInSession" SMALLINT,
    "lastName" VARCHAR(30),
    "length" numeric(10,5),
    "level" VARCHAR(6),
    "location" VARCHAR(300), 
    "method" VARCHAR(3),
    "page" VARCHAR(20),
    "registeration" DECIMAL,
    "sessionId" BIGINT,
    "song" VARCHAR(300),
    "status" VARCHAR(4),
    "ts" TIMESTAMP,
    "userAgent" VARCHAR(300),
    "userId" INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs" INTEGER ,
    "artist_id" VARCHAR(60) NOT NULL,
    "artist_latitude" VARCHAR(50),
    "artist_longitude" VARCHAR(50),
    "artist_location" VARCHAR(200),
    "artist_name" VARCHAR(100),
    "song_id" VARCHAR(60) NOT NULL,
    "title" VARCHAR(200) NOT NULL,
    "duration" numeric(10,5) NOT NULL,
    "year" INTEGER NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays 
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR distkey, 
    artist_id VARCHAR sortkey,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""CREATE TABLE users
(
    user_id INTEGER PRIMARY KEY sortkey,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE songs 
(
    song_id VARCHAR PRIMARY KEY sortkey distkey,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration numeric(10,5)
);
""")

artist_table_create = ("""CREATE TABLE artists 
(
    artist_id VARCHAR PRIMARY KEY sortkey,
    name VARCHAR,
    location VARCHAR, 
    lattitude VARCHAR,
    longitude VARCHAR
);
""")

time_table_create = ("""CREATE TABLE times 
(
    ts TIMESTAMP PRIMARY KEY sortkey,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);
""")

# STAGING TABLES

staging_events_copy = """copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {} compupdate off TIMEFORMAT 'epochmillisecs' TRUNCATECOLUMNS region 'us-west-2';
""".format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy = """copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto' compupdate off STATUPDATE ON TRUNCATECOLUMNS region 'us-west-2';
""".format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))
           
    
# Staging tables row count

staging_events_row_count = "SELECT count (*) from staging_events"
staging_songs_copy_row_count = "SELECT count (*) from staging_songs"

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        ts,
        userId,
        level,
        song_id,
        artist_id,
        sessionId,
        location,
        userAgent
        FROM staging_songs s JOIN staging_events e
        ON s.artist_name = e.artist 
        AND s.title = e.song """)
                        

user_table_insert = ("""INSERT INTO users
    SELECT 
        userId,
        firstName,
        lastName,
        gender,
        level FROM staging_events where userId is not NULL""")
                    

song_table_insert = ("""INSERT INTO songs
    SELECT 
        song_id,
        title,
        artist_id,
        year,
        duration FROM staging_songs where song_id is not null """)
                    

artist_table_insert = ("""INSERT INTO artists
    SELECT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude FROM staging_songs where artist_id is not null """)
        

time_table_insert = ("""INSERT INTO times
    SELECT 
        e.ts,
        EXTRACT(hour from e.ts) as hour,
        EXTRACT(day from e.ts) as day,
        EXTRACT(week from e.ts) as week,
        EXTRACT(month from e.ts) as month,
        EXTRACT(year from e.ts) as year,
        EXTRACT(weekday from e.ts) as weekday FROM staging_events e """)
                    
# Analytics tables row count

songplay_table_row_count = 'SELECT count (*) FROM songplays'
user_table_row_count = 'SELECT count (*) FROM users'
song_table_row_count = 'SELECT count (*) FROM songs'
artist_table_row_count = 'SELECT count (*) FROM artists'
time_table_row_count = 'SELECT count (*) FROM times'
    
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# Operation Names
create_drop_names = ['Staging Events', 'Staging Songs', 'Analytics Song Plays', 'Analytics Users', 'Analytics Songs',
                       'Analytics Artists', 'Analytics Times']
copy_table_names = ['Staging Events', 'Staging Songs']
insert_table_names = ['Analytics Song Plays', 'Analytics Users', 'Analytics Songs', 'Analytics Artists', 'Analytics Times']

# Row Counts
staging_tables_row_count = [staging_events_row_count, staging_songs_copy_row_count]
analytics_tables_row_count = [songplay_table_row_count, user_table_row_count, song_table_row_count, artist_table_row_count, time_table_row_count]


