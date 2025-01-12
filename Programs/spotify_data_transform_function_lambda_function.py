import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

#function toextract album data
def album_data(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_link = row['track']['album']['external_urls']['spotify']
        album_elements = {'album_id' : album_id, 'album_name' : album_name, 'album_release_date' : album_release_date, 'album_total_tracks' : album_total_tracks, 'album_link' : album_link}
        album_list.append(album_elements)
    return album_list

#function to extract artist data
def artist_data(data):
    artist_list = []

    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for ar in value['artists']:
                    artist_dic = {'artist_id' : ar['id'], 'artist_name' : ar['name'], 'external_url' : ar['href']}
                    artist_list.append(artist_dic)
    return artist_list 

#function to extract song data
def song_data(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                    }
        song_list.append(song_element)
    
    return song_list



def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-project1"
    Key = "raw_data/to_processed/"

    spotify_data = []
    spotify_key = []

    #to get data related to spotify
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_key.append(file_key)
    
    #to run the differnet functions on spotify data
    for data in spotify_data:
        album_list = album_data(data)
        song_list = song_data(data)
        artist_list = artist_data(data)

        #converting the album data into a dataframe 
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset='album_id')
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])

        #converting the artist data into a dataframe 
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset='artist_id')

        #converting the song data into a dataframe 
        song_df = pd.DataFrame.from_dict(song_list)

        #to convert the song dataframe to csv file
        song_key = "transformed_data/song_data/song_transformed" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)  
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)

        #to convert the album dataframe to csv file
        album_key = "transformed_data/album_data/album_transformed" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)  
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        #to convert the artist dataframe to csv file
        artist_key = "transformed_data/artist_data/artist_transformed" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)  
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    s3_resource = boto3.resource('s3')
    for key in spotify_key:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()





