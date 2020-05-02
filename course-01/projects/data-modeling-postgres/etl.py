import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a song file from Sparkify's dataset.
    
    Parameters:
        cur (cursor): Cursor to the database storing the processed data
        filepath (string): Path to the file to be processed
        
    Returns:
        None
        
    Result:
        The information in the song file is processed and 
        stored in the artist and song tables in the DB
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data_values = song_data_df.values
    song_data = list(song_data_values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data_df = df[['artist_id', 'artist_name','artist_location',
                         'artist_latitude', 'artist_longitude']]
    artist_data_values = artist_data_df.values
    artist_data = list(artist_data_values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes a log file from Sparkify's dataset.
    
    Parameters:
        cur (cursor): pyscopg2 cursor to the database storing the processed data
        filepath (string): Path to the file to be processed
        
    Returns:
        None
        
    Result:
        The information in the log file is processed and 
        stored in the time, user and songplay tables in the DB
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    is_next_song = df['page'] == 'NextSong'
    df = df[is_next_song]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.weekofyear
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.dayofweek
    time_df = df[['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']] 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                     row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function that processes all of the files of one kind in Sparkify's dataset (logs/songs)
    
    Parameters:
        cur (cursor): pyscopg2 ursor to the database storing the processed data
        conn (connection): pyscopg2 connection which encapsulates a database session
        filepath (string): Path to the file to be processed
        func(string): Name of the function used to process the data
    Returns:
        None
        
    Result:
        The information in all of the files of this type (logs/songs) is processed and 
        stored in their respective tables in the database
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
    Main method in charge of carrying out the ETL process
    
    Parameters:
        None
    Returns:
        None
        
    Result:
        The ETL process is complete and the data mart has been created 
    """    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()