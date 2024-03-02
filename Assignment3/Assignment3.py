import requests
import redis
import json
import pandas as pd
import seaborn as sns


class SpotifyAPI:
    def __init__(self, ID, SECRET):
        self.CLIENT_ID = ID
        self.CLIENT_SECRET = SECRET
        self.AUTH_URL = 'https://accounts.spotify.com/api/token'
        self.access_token = ''
        # base URL of all Spotify API endpoints
        self.BASE_URL = 'https://api.spotify.com/v1/'

    def get_access_token(self):
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

    def get_artists_top_songs(self, artist_name):
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
        # Make a new dict with only things we want
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
            track_dict['release_date'].append(a['album']['release_date'])
            track_dict['explicit'].append(a['explicit'])
            track_dict['popularity'].append(a['popularity'])

        return pd.DataFrame(track_dict)

    def generate_report(self, df):
        # For each artist's top 10 songs, find which albums they are from
        print(df.groupby('artist')['album'].value_counts())

        # What is the range of Spotify Popularity of each artist's top 10 songs
        sns.violinplot(data=df, x='artist', y='popularity', hue='artist').set(
            title='Comparing Popularity of Pop Artist''s Top 10 Songs',
            xlabel='Artist', ylabel='Popularity')

class RedisAPI:
    '''
    https://github.com/gchandra10/redis_python/blob/main/11_redisjson.py

    '''
    def __init__(self, host, port, password):
        print('Connecting to: ' + host + ' at port: ' + port)
        self.r = redis.Redis(
            host=host,
            port=port,
            password=password)

    def set_redis_keys(self, spotify_dict):
        for i in range(len(spotify_dict['track_id'])):
            print('spotify:track:' + str(spotify_dict['track_id'][i]))
            self.r.json().set('spotify:track:' + str(spotify_dict['track_id'][i]),
                              '.',
                              json.dumps(spotify_dict['track_data'][i]))

    def get_redis_keys(self):
        data = []
        for key in self.r.scan_iter():
            print(key)
            data.append(self.r.json().get(key))
        return data
