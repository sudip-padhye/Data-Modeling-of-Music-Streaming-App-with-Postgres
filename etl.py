import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Reads and inserts data from song data log files. This function populates "songs" and "artists" database tables.
    
    Keyword arguments:
    cur -- cursor to the postgres database
    filepath -- path to the song data log files
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # filtering null IDs 
    df = df[pd.notnull(df['song_id'])]
    df = df[pd.notnull(df['artist_id'])]

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values)
    cur.executemany(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values)
    cur.executemany(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Reads and inserts data from process data log files. This function populates "time", "users" and "songplays" database tables.
    
    Keyword arguments:
    cur -- cursor to the postgres database
    filepath -- path to the song data log files
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]
    
    # filtering null IDs
    df = df[pd.notnull(df['ts'])]
    df = df[pd.notnull(df['userId'])]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"])
    
    # insert time data records
    time_data = (df["ts"], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({column_labels[0]:time_data[0], column_labels[1]:time_data[1], column_labels[2]:time_data[2], column_labels[3]:time_data[3],
        column_labels[4]:time_data[4], column_labels[5]:time_data[5], column_labels[6]:time_data[6]})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        if songid is not None and artistid is not None:
            songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Reads files from the filepath and invokes proper function to process those files.
    
    Keyword arguments:
    cur -- cursor to the postgres database
    conn -- database connection object
    filepath -- path to the log files
    func -- function name to be called to process the files
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
    """Main function which controls all activities. It creates connection to database, invokes data processing functions and later closes the database connection.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()