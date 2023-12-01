from collections import OrderedDict

import mysql.connector
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo as pm
from googleapiclient.discovery import build
import sqlalchemy as sa
import pymysql
import sqldf
from pandasql import sqldf, load_meat, load_births

with st.sidebar:
    selected = option_menu(None,["Youtube Channels data",
                           "Youtube Channels data harvesting",
                            "Youtube Data transformation",
                           "Youtube data analysis"]
        , default_index=0)

if selected == "Youtube Channels data":
    l1=[]
    conn = pm.MongoClient('mongodb+srv://des:Suresh@cluster0.qik9lws.mongodb.net/?retryWrites=true&w=majority')
    db = conn['youtube_project']
    col = db['Channels']
    st.header("Youtube channel data available in mongodb")
    for i in col.find({},{'_id':0}):
        l1.append(i)
    df=pd.json_normalize(l1)
    st.dataframe(df)
if selected == "Youtube Channels data harvesting":
    st.subheader('Enter Channel_id')
    cid =st.text_input('')
    st.subheader('Enter API')
    api = st.text_input('  ')
    su=st.button('Submitt')
    if su:
        def get_youtube_channeldetails(channel_id, api):
            youtube = build('youtube', 'v3', developerKey=api)
            channel_response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                                       id=channel_id).execute()
            channel_details = OrderedDict()
            channel_details = {

                'channel_name': channel_response['items'][0]['snippet']['title'],
                'channel_id': channel_response['items'][0]['id'],
                'subscriberCount': int(channel_response['items'][0]['statistics']['subscriberCount']),
                'channel_views': int(channel_response['items'][0]['statistics']['viewCount']),
                'playlistId': channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                'videoCount': int(channel_response['items'][0]['statistics']['videoCount']),
                'channel_description': channel_response['items'][0]['snippet']['description']

            }

            return dict(channel_details)


        def get_youtube_videodetails(channel_details, api):
            youtube = build('youtube', 'v3', developerKey=api)
            video_list = []
            videos_id_list = []
            playlist = channel_details['playlistId']
            video_dir = OrderedDict()
            video_list_response = youtube.playlistItems(). \
                list(part="snippet", playlistId=playlist) \
                .execute()
            for video_list_item in range(0, len(video_list_response['items'])):
                videos_id_list.append(video_list_response['items'][video_list_item]['snippet']['resourceId']['videoId'])
            try:
                nextPageToken = video_list_response['nextPageToken']
            except KeyError:
                nextPageToken = None
            while nextPageToken:
                video_list_response = youtube.playlistItems(). \
                    list(part="snippet", playlistId=playlist, pageToken=nextPageToken) \
                    .execute()
                try:
                    for video_list_item in range(0, len(video_list_response['items'])):
                        videos_id_list.append(
                            video_list_response['items'][video_list_item]['snippet']['resourceId']['videoId'])
                    nextPageToken = video_list_response['nextPageToken']
                except:
                    break



            for video in videos_id_list:
                try:
                    video_response = youtube.videos().list(part="snippet,contentDetails,statistics",
                                                           id=video).execute()
                    video_dir = {
                        'video_id': video,
                        'video_name': video_response['items'][0]['snippet']['title'],
                        'playlistId': playlist,
                        'description': video_response['items'][0]['snippet']['description'],
                        'published_date': video_response['items'][0]['snippet']['publishedAt'],
                        'view_count': int(video_response['items'][0]['statistics']['viewCount']),
                        'like_count': int(video_response['items'][0]['statistics']['likeCount']),
                        'favorite_count': int(video_response['items'][0]['statistics']['favoriteCount']),
                        'comment_count': int(video_response['items'][0]['statistics']['commentCount']),
                        'duartion': video_response['items'][0]['contentDetails']['duration'],
                        'thumbnails': video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                        'caption_status': video_response['items'][0]['contentDetails']['caption']
                    }
                    video_list.append(dict(video_dir))
                except KeyError:
                    pass


            return video_list


        def get_youtube_commentdetails(channel_details, api):
            youtube = build('youtube', 'v3', developerKey=api)
            comment_list = []
            comment_id_list = []
            comment_dir = OrderedDict()
            videos_id_list = []
            nextPageToken = None
            playlist = channel_details['playlistId']
            video_dir = OrderedDict()
            video_list_response = youtube.playlistItems().list(part="snippet", playlistId=playlist,
                                                               maxResults=50, pageToken=None).execute()
            for video_list_item in range(0, len(video_list_response['items'])):
                videos_id_list.append(video_list_response['items'][video_list_item]['snippet']['resourceId']['videoId'])
            try:
                nextPageToken = video_list_response['nextPageToken']
            except KeyError:
                nextPageToken = None
            while nextPageToken:
                video_list_response = youtube.playlistItems(). \
                    list(part="snippet", playlistId=playlist, pageToken=nextPageToken, maxResults=50) \
                    .execute()

                try:
                    for video_list_item in range(0, len(video_list_response['items'])):
                        videos_id_list.append(
                            video_list_response['items'][video_list_item]['snippet']['resourceId']['videoId'])
                    nextPageToken = video_list_response['nextPageToken']
                except:
                    break



                # print(nextPageToken)

            for video in videos_id_list:
                comment_dir = OrderedDict()
                try:
                    comment_response = youtube.commentThreads().list(part="snippet,replies", videoId=video,
                                                                     maxResults=100).execute()
                    for k in comment_response['items']:
                        comment_dir = OrderedDict()
                        comment_dir = {
                            'Comment_id': k['id'],
                            'video_id': video,
                            'Comment_text': k['snippet']['topLevelComment']['snippet']['textOriginal'],
                            'Comment_author': k['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'Comment_published': k['snippet']['topLevelComment']['snippet']['publishedAt'],
                            'Comment_parent_id': False}
                        if len(dict(comment_dir).keys()) != 0:
                            comment_list.append(dict(comment_dir))
                        replycount = k['snippet']['totalReplyCount']
                        if replycount > 0:
                            for reply in k['replies']['comments']:
                                comment_dir = OrderedDict()
                                comment_dir = {'Comment_id': reply['id'],
                                               'video_id': video,
                                               'Comment_text': reply['snippet']['textOriginal'],
                                               'Comment_author': reply['snippet']['authorDisplayName'],
                                               'Comment_published': reply['snippet']['publishedAt'],
                                               'Comment_parent_id':
                                                   k['snippet']['topLevelComment']['snippet']['authorChannelId'][
                                                       'value']
                                               }
                                if len(dict(comment_dir).keys()) != 0:
                                    comment_list.append(dict(comment_dir))
                except:
                    pass
            return comment_list


        def pymongo_check(cid):
            conn = pm.MongoClient(
                'mongodb+srv://des:Suresh@cluster0.qik9lws.mongodb.net/?retryWrites=true&w=majority')
            db = conn['youtube_project']
            col = db['Channels']
            vid_coll = db['Videos']
            co_coll = db['comments']
            x = 0
            l2 = []
            for i in col.find({'channel_details.channel_id': cid}, {'_id': 1, 'channel_details.playlistId': 1}):
                x = i['_id']
                pl = i['channel_details']['playlistId']
            if x != 0:
                col.delete_many({'channel_details.channel_id': cid})
                for i in vid_coll.find({'playlistId': pl}, {'video_id': 1}):
                    co_coll.delete_many({'video_id': i['video_id']})
                vid_coll.delete_many({'playlistId': pl})
            else:
                for i in col.find({}, {'_id': 1}):
                    l2.append(i['_id'])

                return


        # api = 'AIzaSyAkLMS4srB3lWg9UOjdjB8NNfbqVG91tp4'
        # cid = 'UCe7V53TILmFdOCYI2TjHyLA'

        id_c = pymongo_check(cid)
        channel = get_youtube_channeldetails(channel_id=cid, api=api)
        video = get_youtube_videodetails(channel_details=channel, api=api)
        comments = get_youtube_commentdetails(channel_details=channel, api=api)
        conn = pm.MongoClient('mongodb+srv://des:Suresh@cluster0.qik9lws.mongodb.net/?retryWrites=true&w=majority')
        db = conn['youtube_project']
        col = db['Channels']
        col.insert_one({ 'channel_details': channel})
        if len(video) != 0:
            col = db['Videos']
            col.insert_many(video)
        if len(comments) != 0:
            col = db['comments']
            col.insert_many(comments)

if selected == "Youtube Data transformation":
        l1=[]
        conn = pm.MongoClient('mongodb+srv://des:Suresh@cluster0.qik9lws.mongodb.net/?retryWrites=true&w=majority')
        db = conn['youtube_project']
        col = db['Channels']
        vi=db['Videos']
        st.header("Youtube channel data available in mongodb")
        for i in col.find({},{'_id':0,'channel_details.channel_name':1}):
            l1.append(i['channel_details']['channel_name'])
        l1=tuple(l1)
        option = st.selectbox('List of channels',l1)
        for i in col.find({'channel_details.channel_name': option}, {'_id': 0}):
            st.dataframe(pd.json_normalize(i))


        su1 = st.button('Transform')
        if su1:
            def transform(channel_name):
                def converttotime(x):
                    l1 = x.split('D')
                    if len(l1) == 2:

                        l1[0] = l1[0].replace('P', '')
                        l1[1] = l1[1].replace('T', '')
                        l1[1] = l1[1].replace('H', '*60*60+').replace('M', '*60+').replace('S', '')
                        if l1[1] == '':
                            return (int(l1[0]) * 24 * 60)
                        else:
                            return (int(((int(l1[0]) * 24 * 60 * 60) + eval(l1[1])) / 60))

                    else:
                        x = x.replace('P', '')
                        x = x.replace('T', '')
                        x = x.replace('H', '*60*60+').replace('M', '*60+').replace('S', '')
                        return int(eval(x) / 60)

                l1 = []
                l2 = []
                l3 = []
                pysqldf = lambda q: sqldf(q, globals())
                conn = pm.MongoClient(
                    'mongodb+srv://des:Suresh@cluster0.qik9lws.mongodb.net/?retryWrites=true&w=majority')
                db = conn['youtube_project']
                ch = db['Channels']
                for i in ch.find({'channel_details.channel_name': channel_name}, {'_id': 0, 'channel_details': 1}):
                    l1.append(i['channel_details'])
                vi = db['Videos']

                for i in vi.find({'playlistId': l1[0]['playlistId']}, {'_id': 0}):
                    l2.append(i)

                co = db['comments']
                # pprint(co)
                # pprint(l2)
                for i in l2:
                    for doc in co.find({'video_id': i['video_id']}, {'_id': 0}):
                        l3.append(doc)
                        # pprint(doc)
                ch_details = pd.json_normalize(l1)
                ch_details.rename(columns={'channel_id': 'Channel_id',
                                           'channel_name': 'Channel_name',
                                           'playlistId': 'Playlist_id',
                                           'subscriberCount': 'Channel_subscribers',
                                           'channel_views': 'Channel_views',
                                           'channel_description': 'Channel_Description',
                                           'videoCount': 'Channel_videos_Count'}, inplace=True)
                ch_details['Channel_Description'] = ch_details['Channel_Description'].apply(lambda x: x[0:4000])
                pl_details = ch_details[['Playlist_id', 'Channel_id']]
                vi_details = pd.json_normalize(l2)
                vi_details.rename(columns={'video_id': 'Video_id',
                                           'video_name': 'Video_Name',
                                           'playlistId': 'playlist_id',
                                           'description': 'Video_Description',
                                           'published_date': 'Video_published_date',
                                           'view_count': 'Video_views_count',
                                           'like_count': 'Video_like_count',
                                           'favorite_count': 'Video_favorite_count',
                                           'comment_count': 'Video_comment_count',
                                           'duartion': 'Video_duartion',
                                           'thumbnails': 'Video_thumbnail',
                                           'caption_status': 'Video_caption_status'}, inplace=True)
                vi_details['Video_published_date'] = vi_details['Video_published_date'].str.replace("T", " ",
                                                                                                    regex=True).replace(
                    "Z", " ", regex=True)
                vi_details['Video_duartion'] = vi_details['Video_duartion'].apply(
                    lambda x: x + '0S' if x[-1] != 'S' else x)
                vi_details['Video_duartion'] = vi_details['Video_duartion'].apply(converttotime)
                co_details = pd.json_normalize(l3)
                co_details.rename(columns={'Comment_id': 'Comment_id',
                                           'video_id': 'Video_id',
                                           'Comment_text': 'Comment_text',
                                           'Comment_author': 'Comment_author',
                                           'Comment_published': 'Comment_published_date',
                                           'Comment_parent_id': 'Comment_parent_id', }, inplace=True)
                # print(co_details['Comment_published_date'])
                co_details['Comment_published_date'] = co_details['Comment_published_date'].str.replace("T", " ",
                                                                                                        regex=True).replace(
                    "Z", " ", regex=True)
                connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
                engine = sa.create_engine(connection_string)
                engine.connect()

                # print(vi_details[vi_details["Video_id"]=='oNOJfOBzAxM'])
                query = "select Channel_Name from Channel where Channel_Name='" + channel_name + "'"
                ch_dbdetails = pd.read_sql(query, engine)
                if ch_dbdetails.count()['Channel_Name'] == 0:
                    ch_details.to_sql('channel', if_exists='append', con=engine, index=False)
                    pl_details.to_sql('playlist', if_exists='append', con=engine, index=False)
                    vi_details.to_sql('video', if_exists='append', con=engine, index=False)
                    co_details.to_sql('comment', if_exists='append', con=engine, index=False)
                else:

                    query2 = "select vi.Video_id from video vi " \
                             "where Vi.playlist_id=(select playlist_id from channel where Channel_Name ='" + channel_name + "' )"
                    Vi_dbdetails = pd.read_sql(query2, engine)
                    vi_l = []
                    for i in Vi_dbdetails['Video_id']:
                        vi_l.append(i)
                    vi_details1 = sqldf(
                        """select * from vi_details where Video_id not in (select Video_id from Vi_dbdetails)""")
                    delete_cmd_co = "delete from comment where video_id in ('" + ("','".join(vi_l)) + "')"
                    delete_cmd_vi = "Delete from video vi where Vi.playlist_id=(select playlist_id from channel where Channel_Name ='" + channel_name + "' )"
                    if (len(vi_l) >= 1):
                        conn = mysql.connector.Connect(host='localhost', username='root', password='Suresh@1986',
                                                       database='project')
                        cursor = conn.cursor()
                        cursor.execute(delete_cmd_co)
                        conn.commit()
                        conn.close()
                    try:
                        conn = mysql.connector.Connect(host='localhost', username='root', password='Suresh@1986',
                                                       database='project')
                        cursor = conn.cursor()
                        cursor.execute(delete_cmd_vi)
                        conn.commit()
                        conn.close()
                    except mysql.connector.errors.InternalError as ie:
                        pass
                    try:
                        conn = mysql.connector.Connect(host='localhost', username='root', password='Suresh@1986',
                                                       database='project')
                        delete_cmd_pl = "Delete from playlist where playlist_id=(select playlist_id from channel where Channel_Name ='" + channel_name + "' )"
                        cursor = conn.cursor()
                        cursor.execute(delete_cmd_pl)
                        conn.commit()
                        conn.close()
                    except mysql.connector.errors.InternalError as ie:
                        pass
                    try:
                        conn = mysql.connector.Connect(host='localhost', username='root', password='Suresh@1986',
                                                       database='project')
                        delete_cmd_ch = "delete from channel where Channel_Name ='" + channel_name + "' "
                        cursor = conn.cursor()
                        cursor.execute(delete_cmd_ch)
                        conn.commit()
                        conn.close()
                    except mysql.connector.errors.InternalError as ie:
                        pass
                    # vi_details1.to_sql('video', if_exists='append', con=engine, index=False)
                    ch_details.to_sql('channel', if_exists='append', con=engine, index=False)
                    pl_details.to_sql('playlist', if_exists='append', con=engine, index=False)
                    vi_details.to_sql('video', if_exists='append', con=engine, index=False)
                    co_details1 = sqldf(
                        """select * from co_details where Video_id not  in (select Video_id from Vi_dbdetails)""")
                    co_details.to_sql('comment', if_exists='append', con=engine, index=False)

            transform(option)

if selected == "Youtube data analysis":
    select_a=['What are the names of all the videos and their corresponding channels?',
              'Which channels have the most number of videos, and how many videos do they have?',
              'What are the top 10 most viewed videos and their respective channels?',
              'How many comments were made on each video, and what are their and corresponding video names?',
              'Which videos have the highest number of likes, and what are their corresponding channel names?',
              'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
              'What is the total number of views for each channel, and what are their corresponding channel names?',
              'What are the names of all the channels that have published videos in the year 2022?',
              'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
              'Which videos have the highest number of comments, and what are their corresponding channel names?',
              'Which videos have the highest number of views channel wise?']
    option1 = st.selectbox('List of channels', select_a)
    if option1 == "What are the names of all the videos and their corresponding channels?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "select * from  `project`.`Video_names`"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "How many comments were made on each video, and what are their and corresponding video names?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "SELECT Video_Name,Video_comment_count FROM project.video"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "What are the top 10 most viewed videos and their respective channels?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "select * from `project`.`vw_top_viewed_videos`"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "Which videos have the highest number of likes, and what are their corresponding channel names?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "select * from  `project`.`vw_top_like_video`"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "SELECT Video_Name,Video_like_count FROM project.video"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "What is the total number of views for each channel, and what are their corresponding channel names?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "SELECT Channel_Name,Channel_views FROM project.channel order by Channel_views desc;"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "What are the names of all the channels that have published videos in the year 2022?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "select * from  `project`.`vw_channels_active_in_2022`"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "Which channels have the most number of videos, and how many videos do they have?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "SELECT Channel_Name, Channel_videos_Count FROM project.channel where Channel_videos_Count=(select max(Channel_videos_Count) from project.channel)"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "select * from `project`.`vw_channels_avg_video_duration`"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "Which videos have the highest number of comments, and what are their corresponding channel names?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "select * from `project`.`vw_channels_top_Video_comment_count`"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)
    if option1 == "Which videos have the highest number of views channel wise?":
        connection_string = ('mysql+pymysql://root:Suresh%401986@localhost/project')
        engine = sa.create_engine(connection_string)
        engine.connect()
        query = "select * from `project`.`vw_channels_top_Video_view_count_foreach`"
        ou1=pd.read_sql(query, engine)
        st.dataframe(ou1)


