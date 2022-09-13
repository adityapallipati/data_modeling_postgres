import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    
    This function takes the data from the song file,
    selects the specfic rows for the song record,
    selects the specific rows for the artist table,
    and inserts them into the table created from
    create_tables.py.
    
    
    """
    
 
    # open song file
    df = pd.read_json(filepath, lines=True)

    #select specific rows, convert to list
    df_select = df[['song_id','title','artist_id', 'year', 'duration']].values.tolist()
    
    # index first of list
    df_select = df_select[0]
    
    # insert song record
    song_data = df_select
    cur.execute(song_table_insert, song_data)
    
    
    # select artist data, convert to list
    df_select_artist = df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    
    
    # select first of list
    df_select_artist = df_select_artist[0]
    
    # insert artist record
    artist_data = df_select_artist
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    This function takes the data from the log file,
    filters the by column by the NextSong,
    converts the timestamp column into datetime values, 
    and then inserts into the time table.
    
    Following this action, the function selects columns from
    the users column, copies the data from that df into another
    variable called df_select_users_copy, drops duplicates in that
    dataframe, then inserts the data into the users table.
    
    An additional conversion to dattime for the 'ts' or timestamp column
    is done to avoid any errors. then the data is inserted into the
    songplay table by retrieving data from the song table and artist table.
    
    """
    

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df.copy()
    t.ts = pd.to_datetime(t.ts)
    timestamp = t.ts
    hour = t.ts.dt.hour
    day = t.ts.dt.day
    week = t.ts.dt.week
    month = t.ts.dt.month
    year = t.ts.dt.year
    weekday = t.ts.dt.weekday_name
    
    # insert time data records
    time_data = (timestamp, hour, day, week, month, year, weekday)
    column_labels = ('timestamp','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
    # select user columns
    df_select_users = df[['userId','firstName','lastName', 'gender', 'level']]
    
    # copy df to avoid SettingwithCopyWarning
    df_select_users_copy = df_select_users.copy()
    
    # drop duplicate users
    df_select_users_copy.drop_duplicates(subset='userId')

    # load user table
    user_df = df_select_users_copy

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    # convert ts to datetime
    
    df.ts = pd.to_datetime(df.ts)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    This function allows the first two functions to take the song file
    and the log file in order to insert the proper data into the tables outlined above.
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    This is our main function which executes our all of our above functions as well as
    ensures a connection is established to througb psycopg2 library to our sparkifydb.
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()