import requests
import json
import pandas as pd
import seaborn as sns
import redis
from BigData.Assignment3.config import *
import matplotlib.pyplot as plt


class SpotifyTrackAnalyzer:
    def __init__(self, id, secret):
        """Initialize Spotify Connector.

        Using the Spotify Web API.
        Must have a Spotify Developer account from:
        https://developer.spotify.com/

        Args:
            id:     the user's Spotify Web API client ID
            secret: the user's Spotify Web API secret key
        """
        self.CLIENT_ID = id
        self.CLIENT_SECRET = secret
        self.AUTH_URL = 'https://accounts.spotify.com/api/token'
        self.access_token = ''
        # base URL of all Spotify API endpoints
        self.BASE_URL = 'https://api.spotify.com/v1/'

    def get_access_token(self):
        """Get an access token from the Spotify Web API"""
        # POST
        auth_response = requests.post(self.AUTH_URL, {
            'grant_type': 'client_credentials',
            'client_id': self.CLIENT_ID,
            'client_secret': self.CLIENT_SECRET,
        })

        # convert the response to JSON
        auth_response_data = auth_response.json()

        # save the access token
        self.access_token = auth_response_data['access_token']
        print('Got Spotify Access Token')

    def get_artists_top_songs(self, artist_name):
        """ Using Spotify Web API, GET top 10 songs of a given artist in the US market.

        Args:
            artist_name:     Name of a musical artist.
        Returns:
            Python dictionary containing keys of Spotify Track ID and
            values of JSON track data
        """
        headers = {
            'Authorization': 'Bearer {token}'.format(token=self.access_token)
        }

        # actual GET request with proper header
        # Get the artist ID for use in second search
        r = requests.get(self.BASE_URL + 'search?',
                         {
                             'q': artist_name,
                             'type': 'artist'
                         },
                         headers=headers)
        r = r.json()
        artist_id = r['artists']['items'][0]['id']

        # Use Spotify API to get information about top songs
        r = requests.get(self.BASE_URL + 'artists/' + artist_id + '/top-tracks',
                         params={'market': 'US'},
                         headers=headers)
        r = r.json()

        all_ids = []
        all_track_data = []
        for t in r['tracks']:
            all_ids.append(t['id'])
            all_track_data.append(t)

        track_dict = {'track_id': all_ids,
                      'track_data': all_track_data}

        print('Found {} tracks for {}'.format(len(track_dict['track_id']), artist_name))
        return track_dict

    def json_to_dataframe(self, json_data):
        """
        Convert JSON data from Spotify Web API into Pandas DataFrame containing
        only necessary information for analysis.

        Args:
            json_data: Spotify Track Object in JSON format, from Spotify Web API
        Returns:
            A Pandas DataFrame containing information on the Spotify track

        """
        # Make a new dict with only data from Spotify API that we want
        track_dict = {'album': [],
                      'artist': [],
                      'name': [],
                      'duration_ms': [],
                      'release_date': [],
                      'explicit': [],
                      'popularity': []}

        for item in json_data:
            a = json.loads(item)
            track_dict['album'].append(a['album']['name'])
            track_dict['artist'].append(a['artists'][0]['name'])
            track_dict['name'].append(a['name'])
            track_dict['duration_ms'].append(a['duration_ms'])

            # Force all dates to be in YYYY-mm-dd format
            # if only the year is available
            release_date = a['album']['release_date']
            if len(str(release_date)) == 4:
                release_date = str(release_date) + '-01-01'
            track_dict['release_date'].append(release_date)

            track_dict['explicit'].append(a['explicit'])
            track_dict['popularity'].append(a['popularity'])

        return pd.DataFrame(track_dict)

    def generate_report(self, df):
        """ Generate analysis on artists in current Redis database

        Args:
              df:   Pandas DataFrame of Spotify Track Data,
                    as returned by SpotifyTrackAnalyzer.json_to_dataframe
        Returns:
              Aggregates number of top songs by album, timeline of artist's top songs,
              and a Violin Plot of the artists' top 10 tracks by popularity
        """
        print('\nGenerating report....\n============================================\n')
        # For each artist's top 10 songs, find which albums they are from
        print(df.groupby('artist')['album'].value_counts())

        # Convert release date to date time object
        df['release_date'] = pd.to_datetime(df['release_date'], format='%Y-%m-%d')
        # Generate figure of artist's song's popularity by release year and month
        plt.figure()
        sns.scatterplot(data=df, x='release_date', y='artist', hue='popularity').set(
            title='Popularity of Pop Artist''s Songs by Release Date',
            xlabel='Release Date', ylabel='Artist')
        plt.tight_layout()
        plt.savefig('artist_scatter_by_release_date.png')

        # What is the range of Spotify Popularity of each artist's top 10 songs
        plt.figure()
        sns.violinplot(data=df, x='artist', y='popularity', hue='artist').set(
            title='Comparing Popularity of Pop Artist''s Top 10 Songs',
            xlabel='Artist', ylabel='Popularity')
        plt.savefig('artist_popularity_violin_plot.png')

class RedisConnector:
    """ Connect to Redis Cloud, insert, and read data """

    def __init__(self, host, port, password):
        """ Connect to Redis Cloud.
        Go to Redis Cloud database Dashboard to get the host, port, and password to connect.

        Args:
            host: Endpoint for Redis database
            port: Port defined by Redis Cloud
            password: Secret Password from Redis Cloud Dashboard
        """
        print('Connecting to: ' + host + ' at port: ' + str(port))
        self.r = redis.Redis(
            host=host,
            port=port,
            password=password)

        # Clear database before adding new data
        self.r.flushall()

    def set_redis_keys(self, spotify_dict):
        """ Populate Redis Database with data from Spotify API

        Args:
            spotify_dict: Spotify Track Dictionary as returned by
                          SpotifyTrackAnalyzer.get_artists_top_songs
        """
        print('\tSetting Redis keys')
        print('\tAdding ' + str(len(spotify_dict['track_id'])) + ' tracks to Redis')
        for i in range(len(spotify_dict['track_id'])):
            self.r.json().set('spotify:track:' + str(spotify_dict['track_id'][i]),
                              '.',
                              json.dumps(spotify_dict['track_data'][i]))

    def get_redis_keys(self):
        """ Finds all keys in Redis database, iterates through keys returning all JSON data

        Returns:
            data: Array, each element track data in JSON format from Spotify Web API
        """
        print('Getting Redis keys')
        data = []
        for key in self.r.scan_iter():
            data.append(self.r.json().get(key))
        return data


def main():
    # Create Spotify and Redis Cloud Connection
    spotify = SpotifyTrackAnalyzer(CLIENT_ID, CLIENT_SECRET)
    redis = RedisConnector(host, port, password)

    # Get Spotify Web API token
    spotify.get_access_token()

    # Search Spotify Web API for top 10 tracks for each artist
    # Populate Redis Database with Spotify Track data in JSON format
    redis.set_redis_keys(spotify.get_artists_top_songs('Beyonce'))
    redis.set_redis_keys(spotify.get_artists_top_songs('Taylor Swift'))
    redis.set_redis_keys(spotify.get_artists_top_songs('Cher'))
    redis.set_redis_keys(spotify.get_artists_top_songs('Vanessa Carlton'))

    # Read from Redis database
    data = redis.get_redis_keys()

    # Convert JSON format data to a Pandas dataframe for analysis
    # Do some data cleaning on release_date format YYYY to YYYY-mm-dd
    df = spotify.json_to_dataframe(data)

    # Generate analysis on Spotify track data
    spotify.generate_report(df)


if __name__ == "__main__":
    """ Entrypoint to code"""
    main()
