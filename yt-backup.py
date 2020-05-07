# yt-backup command line utility to backup youtube channels easily
# Copyright (C) 2020  w0d4
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import argparse
import glob
import json
import logging
import os
import pickle
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from random import randint
from time import sleep

import googleapiclient.discovery
import googleapiclient.errors
import requests
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from sqlalchemy import func, or_

from base import Session, engine, Base
from channel import Channel
from operation import Operation
from playlist import Playlist
from statistic import Statistic
from video import Video

api_service_name = "youtube"
api_version = "v3"
client_secrets_file = "client_secret.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]

Base.metadata.create_all(engine)
session = Session()

parser = argparse.ArgumentParser(description='yt-backup')
parser.add_argument("mode", action="store", type=str, help="Valid options: add_channel, get_playlists, get_video_infos, download_videos, run, toggle_channel_download, generate_statistics, verify_offline_videos")
parser.add_argument("--channel_id", action="store", type=str, help="Defines a channel ID to work on. Required for modes: add_channel")
parser.add_argument("--username", action="store", type=str, help="Defines a channel name to work on. Required for modes: add_channel")
parser.add_argument("--playlist_id", action="store", type=str, help="Defines a playlist ID to work on. Optional for modes: get_video_infos, download_videos")
parser.add_argument("--retry-403", action="store_true", help="If this flag ist set, yt-backup will retry to download videos which were marked with 403 error during initial download.")
parser.add_argument("--rclone_json_export_file", action="store", type=str, help="")
parser.add_argument("--statistics", action="store", type=str, help="Comma seperated list which statistics should be collected during statistics run. Supported types: archive_size,videos_monitored,videos_downloaded")
parser.add_argument("--enabled", action="store_true", help="Switch to control all modes which enables or disables things. Rquired for modes: toggle_channel_download")
parser.add_argument("--disabled", action="store_true", help="Switch to control all modes which enables or disables things. Rquired for modes: toggle_channel_download")
parser.add_argument("--ignore_429_lock", action="store_true", help="Ignore whether an IP was 429 blocked and continue downloading with it.")
parser.add_argument("--debug", action="store_true")
parser.add_argument("-V", action="version", version="%(prog)s 0.9")
args = parser.parse_args()

logger = logging.getLogger('yt-backup')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
fl = logging.FileHandler("/tmp/yt-backup.log".format())
if args.debug:
    ch.setLevel(logging.DEBUG)
    fl.setLevel(logging.DEBUG)
else:
    ch.setLevel(logging.INFO)
    fl.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fl.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fl)

with open('config.json', 'r') as f:
    config = json.load(f)

# Psave the parsed arguments for easier use
mode = args.mode
channel_id = args.channel_id
playlist_id = args.playlist_id
username = args.username
rclone_json_export_file = args.rclone_json_export_file
statistics = args.statistics
retry_403 = args.retry_403
enabled = args.enabled
disabled = args.disabled
ignore_429_lock = args.ignore_429_lock

# define video status
video_status = {"offline": 0, "online": 1, "http_403": 2, "hate_speech": 3, "unlisted": 4}


def get_current_timestamp():
    ts = time.time()
    return ts


def signal_handler():
    logger.info('Catched Ctrl+C!')
    set_status("aborted")
    if os.path.exists(config["base"]["download_lockfile"]):
        logger.debug("Removing download lockfile")
        os.remove(config["base"]["download_lockfile"])
    sys.exit(0)


def log_operation(duration, operation_type, operation_description):
    operation = Operation()
    operation.duration = duration
    operation.operation_type = operation_type
    operation.operation_description = operation_description
    operation.operation_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    session.add(operation)
    session.commit()


def set_status(new_status):
    current_status = session.query(Statistic).filter(Statistic.statistic_type == "status").scalar()
    if current_status is None:
        current_status = Statistic()
        current_status.statistic_type = "status"
    current_status.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    current_status.statistic_value = new_status
    session.add(current_status)
    session.commit()


def set_currently_downloading(video_name):
    currently_downloading = session.query(Statistic).filter(Statistic.statistic_type == "currently_downloading").scalar()
    if currently_downloading is None:
        currently_downloading = Statistic()
        currently_downloading.statistic_type = "currently_downloading"
    currently_downloading.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    currently_downloading.statistic_value = video_name
    session.add(currently_downloading)
    session.commit()


def set_http_429_state():
    http_429_state = session.query(Statistic).filter(Statistic.statistic_type == "http_429_state").scalar()
    if http_429_state is None:
        http_429_state = Statistic()
        http_429_state.statistic_type = "http_429_state"
    http_429_state.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    http_429_state.statistic_value = get_current_ytdl_ip()
    session.add(http_429_state)
    session.commit()


def clear_http_429_state():
    http_429_state = session.query(Statistic).filter(Statistic.statistic_type == "http_429_state").scalar()
    if http_429_state is None:
        http_429_state = Statistic()
        http_429_state.statistic_type = "http_429_state"
    http_429_state.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    http_429_state.statistic_value = ""
    session.add(http_429_state)
    session.commit()


def get_http_429_state():
    http_429_state = session.query(Statistic).filter(Statistic.statistic_type == "http_429_state").scalar()
    return http_429_state


def check_429_lock():
    # if the ignore_429_lock flag is set, dont't check anything, just continue
    if ignore_429_lock:
        return False
    http_429_state = get_http_429_state()
    if http_429_state is None:
        return False
    else:
        ytdl_ip_of_last_429 = http_429_state.statistic_value
        logger.debug("Calculate difference since last 429")
        if ytdl_ip_of_last_429 != get_current_ytdl_ip():
            return False
        date_of_last_429 = datetime.strptime(str(http_429_state.statistic_date), '%Y-%m-%d %H:%M:%S')
        current_time = datetime.now()
        delta = current_time - date_of_last_429
        logger.debug("Delta seconds since last 429: "+str(delta.total_seconds()))
        if delta.total_seconds() < 48*60*60:
            return True


def get_current_ytdl_ip():
    if config["youtube-dl"]["proxy"] != "":
        proxies = {"http": config["youtube-dl"]["proxy"], "https": config["youtube-dl"]["proxy"]}
        r = requests.get("https://ipinfo.io", proxies=proxies)
    else:
        r = requests.get("https://ipinfo.io")
    answer = json.loads(str(r.text))
    current_ytdl_ip = answer["ip"]
    return current_ytdl_ip


def log_statistic(statistic_type, statistic_value):
    statistic = Statistic()
    statistic.statistic_type = statistic_type
    statistic.statistic_value = statistic_value
    statistic.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    session.add(statistic)
    session.commit()


def get_playlist_ids_from_google(local_channel_id):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting playlists")
    request = youtube.channels().list(part="contentDetails", id=local_channel_id)
    response = request.execute()
    return response


def get_google_api_credentials():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def get_channel_playlists(local_channel_id, monitored=1):
    logger.debug("Getting playlist IDs")
    google_response = get_playlist_ids_from_google(local_channel_id)
    for playlist in google_response['items'][0]['contentDetails']['relatedPlaylists']:
        if playlist not in ["watchHistory", "watchLater", "favorites", "likes"]:
            local_playlist_id = str(google_response['items'][0]['contentDetails']['relatedPlaylists'][playlist])
            if session.query(Playlist).filter(Playlist.playlist_id == local_playlist_id).scalar() is not None:
                logger.debug("Playlist is already in database")
                continue
            logger.debug(str("Found playlist " + playlist + " " + google_response['items'][0]['contentDetails']['relatedPlaylists'][playlist]))
            playlist_obj = Playlist()
            playlist_obj.playlist_id = str(google_response['items'][0]['contentDetails']['relatedPlaylists'][playlist])
            playlist_obj.playlist_name = str(playlist)
            playlist_obj.channel_id = session.query(Channel).filter(Channel.channel_id == local_channel_id).scalar().id
            playlist_obj.monitored = monitored
            session.add(playlist_obj)
    session.commit()


def get_channel_name_from_google(local_channel_id):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel name")
    request = youtube.channels().list(part="brandingSettings", id=local_channel_id)
    response = request.execute()
    channel_name = str(response["items"][0]["brandingSettings"]["channel"]["title"]).replace(" ", "_")
    logger.debug("Got channel name " + channel_name + " from google.")
    return channel_name


def get_channel_id_from_google(local_username):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel id by username")
    request = youtube.channels().list(part="id", forUsername=local_username)
    response = request.execute()
    logger.debug(str(response))
    local_channel_id = str(response["items"][0]["id"])
    logger.debug("Got channel name " + local_channel_id + " from google.")
    return local_channel_id


def add_channel(local_channel_id):
    # log start time
    start_time = get_current_timestamp()

    # create channel object
    channel = Channel()

    # get Channel details
    channel.channel_id = local_channel_id
    channel.channel_name = get_channel_name_from_google(local_channel_id)

    # add channel to channel table
    if session.query(Channel).filter(Channel.channel_id == local_channel_id).scalar() is None:
        logger.info("Added Channel " + channel.channel_name + " to database.")
        session.add(channel)
        session.commit()
    else:
        logger.info("Channel is already in database")
    end_time = get_current_timestamp()
    log_operation(end_time - start_time, "add_channel", "Added channel " + channel.channel_name)


def get_video_infos_for_one_video(video_id):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel id by video_id")
    request = youtube.videos().list(part="snippet", id=video_id)
    response = request.execute()
    return response


def get_geoblock_list_for_one_video(video_id):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel id by video_id")
    request = youtube.videos().list(part="contentDetails", id=video_id)
    response = request.execute()
    geoblock_list = []
    logger.debug(str(response))
    for entry in response["items"][0]["contentDetails"]["regionRestriction"]["blocked"]:
        geoblock_list.append(str(entry))
        logger.debug("Found " + str(entry) + " in geoblock list of video " + str(geoblock_list) + ".")
    return geoblock_list


def add_video(video_id, downloaded="", resolution="", size="", duration=""):
    video = Video()
    video.video_id = video_id
    video_infos = get_video_infos_for_one_video(video_id)
    logger.debug(str(video_infos))
    if video_infos["pageInfo"]["totalResults"] == 0:
        logger.error("Video with ID " + video_id + " is not available on youtube anymore")
        return ""
    local_channel_id = str(video_infos["items"][0]["snippet"]["channelId"])
    title = str(video_infos["items"][0]["snippet"]["title"])
    description = str(video_infos["items"][0]["snippet"]["description"])
    add_channel(local_channel_id)
    get_channel_playlists(local_channel_id, 0)
    internal_channel_id = session.query(Channel.id).filter(Channel.channel_id == local_channel_id).scalar()
    video_playlist = session.query(Playlist.id).filter(Playlist.channel_id == internal_channel_id).filter(Playlist.playlist_name == "uploads")
    video.playlist = video_playlist
    video.title = title
    video.description = description
    video.downloaded = downloaded
    video.resolution = resolution
    video.size = size
    video.runtime = duration
    video.online = video_status["status"]
    video.download_required = 1
    session.add(video)
    session.commit()


def add_user(local_username):
    local_channel_id = get_channel_id_from_google(local_username)
    add_channel(local_channel_id)


def get_playlists():
    channels = session.query(Channel).all()
    for channel in channels:
        start_time = get_current_timestamp()
        logger.debug("Getting Playlists for " + str(channel.channel_name))
        get_channel_playlists(channel.channel_id)
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "get_playlists", "Got playlists for channel " + str(channel.channel_name))


def get_videos_from_playlist_from_google(local_playlist_id, next_page_token):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting videos")
    if next_page_token is None:
        request = youtube.playlistItems().list(part="snippet,contentDetails", maxResults=50, playlistId=local_playlist_id)
    else:
        request = youtube.playlistItems().list(part="snippet,contentDetails", maxResults=50, playlistId=local_playlist_id, pageToken=next_page_token)
    response = ""
    response = request.execute()
    return response


def get_video_infos():
    playlists = session.query(Playlist).all()
    for playlist in playlists:
        parsed_from_api = 0
        if playlist.monitored == 0:
            continue
        start_time = get_current_timestamp()
        videos = []
        videos_to_check_against = []
        channel_name = session.query(Channel.channel_name).filter(Channel.id == playlist.channel_id).scalar()
        playlist_videos_in_database = len(session.query(Video.video_id).filter(Video.playlist == playlist.id).filter(Video.online == video_status["online"]).all())
        logger.info("Getting all video metadata for playlist " + playlist.playlist_name + " for channel " + str(channel_name))
        results = []
        try:
            result = get_videos_from_playlist_from_google(playlist.playlist_id, None)
        except googleapiclient.errors.HttpError:
            logger.error("Playlist " + playlist.playlist_name + " in Channel " + channel_name + " is not available")
            continue
        results.append(result)
        videos_in_playlist = result["pageInfo"]["totalResults"]
        logger.debug("Videos in Playlist: " + str(videos_in_playlist))
        if playlist_videos_in_database == videos_in_playlist:
            logger.info("No new videos in playlist. We have all in database.")
            continue
        next_page_token = None
        try:
            next_page_token = str(result["nextPageToken"])
            logger.debug("Next page token: " + next_page_token)
        except KeyError:
            logger.debug("Playlist " + playlist.playlist_name + " in Channel " + channel_name + " has only one page.")
        while next_page_token is not None:
            result = get_videos_from_playlist_from_google(playlist.playlist_id, next_page_token)
            results.append(result)
            try:
                next_page_token = str(result["nextPageToken"])
            except:
                break
            logger.debug("Next page token: " + next_page_token)
        for entry in results:
            for video_raw in entry["items"]:
                # logger.debug(video_raw["contentDetails"]["videoId"] + " " + " - " + video_raw["snippet"]["title"])
                # logger.debug("Description: " + video_raw["snippet"]["description"])
                parsed_from_api = parsed_from_api + 1
                video = Video()
                video.video_id = video_raw["contentDetails"]["videoId"]
                video.title = video_raw["snippet"]["title"]
                video.description = video_raw["snippet"]["description"]
                video.playlist = playlist.id
                video.online = video_status["online"]
                video.download_required = 1
                if session.query(Video).filter(Video.video_id == video.video_id).scalar() is None:
                    videos.append(video)
                    videos_to_check_against.append(video)
                    logger.info("Added new video " + video.video_id + " to DB.")
                else:
                    videos_to_check_against.append(video)
                    # If we get a video ID back from youtube, which is already in database,
                    # we will check if it was set to offline in DB.
                    # If it's offline in DB, we will it set online again
                    # It happens, that some video IDs are missing in youtube channels details, so we have false flag offline videos in DB
                    video = session.query(Video).filter(Video.video_id == video.video_id).scalar()
                    if video.online == video_status["offline"]:
                        logger.info("Marking video " + str(video.video_id) + " as online again.")
                        video.online = video_status["online"]
                        videos.append(video)
        logger.debug("Parsed " + str(parsed_from_api) + " videos from API.")
        logger.debug(str(len(videos)))
        session.add_all(videos)
        session.commit()
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "get_video_infos", "Got video infos for playlist " + playlist.playlist_name + " of channel " + channel_name)
        check_videos_online_state(videos_to_check_against, playlist.id)


def check_videos_online_state(videos_to_check_against, local_playlist_id):
    start_time = get_current_timestamp()
    logger.debug("Getting all videos for playlist_id " + str(local_playlist_id) + " from database for video offline checking.")
    playlist_videos_in_db = session.query(Video).filter(Video.playlist == local_playlist_id).filter(Video.online == video_status["online"]).filter(Video.downloaded is not None).all()
    logger.debug(str(len(playlist_videos_in_db)) + " videos are in DB for playlist")
    videos_to_check_against_ids = []
    for video in videos_to_check_against:
        videos_to_check_against_ids.append(video.video_id)
        logger.debug("Added " + str(video.video_id) + "to check_Against list.")
    logger.debug("Will be checked against " + str(len(videos_to_check_against_ids)))
    logger.debug("Calculating the offline video ids")
    offline_video_ids = []
    for video in playlist_videos_in_db:
        if str(video.video_id) not in videos_to_check_against_ids and video.online != video_status["unlisted"]:
            offline_video_ids.append(str(video.video_id))
            logger.debug("Adding " + str(video.video_id) + " to offline video list.")
    logger.debug("Updating all video in offline video list to offline state")
    if len(offline_video_ids) == 0:
        return None
    logger.debug(str(len(offline_video_ids)) + " Videos are offline now.")
    for offline_video_id in offline_video_ids:
        logger.info("Video " + str(offline_video_id) + " is not on youtube anymore. Setting offline now.")
        video = session.query(Video).filter(Video.video_id == str(offline_video_id)).scalar()
        video.online = video_status["offline"]
        session.add(video)
    end_time = get_current_timestamp()
    log_operation(end_time - start_time, "check_online_state", "Checked online state for all videos of playlist_id " + str(local_playlist_id))
    session.commit()


def download_videos():
    # Online States: 1 = online, 2 = 403 error, 3 = blocked in countries because hate speech
    if os.path.exists(config["base"]["download_lockfile"]):
        logger.error("Download lockfile is exisiting. If this is from aborted process, please remove " + config["base"]["download_lockfile"])
        return None
    if check_429_lock():
        logger.error("The current used IP is still HTTP 429 blocked. Cannot continue.")
        return None
    Path(config["base"]["download_lockfile"]).touch()
    if os.path.exists(config["base"]["download_dir"]):
        shutil.rmtree(config["base"]["download_dir"])
    if config["youtube-dl"]["proxy"] != "":
        proxies = {"http": config["youtube-dl"]["proxy"], "https": config["youtube-dl"]["proxy"]}
        r = requests.get("https://ipinfo.io", proxies=proxies)
    else:
        r = requests.get("https://ipinfo.io")
    answer = json.loads(str(r.text))
    current_country = answer["country"]
    video_file = None
    http_429_counter = 0
    if playlist_id is None:
        if retry_403:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(or_(Video.online == video_status["online"], Video.online == video_status["hate_speech"], Video.online == video_status["http_403"])).filter(Video.download_required == 1)
        else:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(or_(Video.online == video_status["online"], Video.online == video_status["hate_speech"])).filter(Video.download_required == 1)
    else:
        playlist_internal_id = session.query(Playlist.id).filter(Playlist.playlist_id == playlist_id)
        if retry_403:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(Video.playlist == playlist_internal_id).filter(or_(Video.online == video_status["online"], Video.online == video_status["http_403"], Video.online == video_status["hate_speech"])).filter(Video.download_required == 1)
        else:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(Video.playlist == playlist_internal_id).filter(Video.online == video_status["online"], Video.online == video_status["hate_speech"]).filter(Video.download_required == 1)
    logger.info("I have " + str(len(videos_not_downloaded.all())) + " in download queue. Start downloading now.")
    for video in videos_not_downloaded:
        set_status("downloading")
        if video.copyright is None:
            pass
        else:
            if current_country + "," in video.copyright:
                logger.info("This video is geoblocked in the following countries: " + video.copyright + ". Current Country: " + current_country)
                continue
        start_time = get_current_timestamp()
        if os.path.exists(config["youtube-dl"]["download-archive"]):
            with open(config["youtube-dl"]["download-archive"]) as download_archive_file:
                if video.video_id in download_archive_file.read():
                    logger.debug("Video " + video.video_id + " found in youtube-dl archive file. Setting impossible download date to import to database.")
                    video.downloaded = "1972-01-01 23:23:23"
                    session.add(video)
                    session.commit()
                    continue
        logger.info("Video " + str(video.video_id) + " - " + video.title + " is not yet downloaded. Downloading now.")
        local_channel_id = session.query(Playlist.channel_id).filter(Playlist.id == video.playlist).scalar()
        logger.debug("Video belongs to playlist " + str(local_channel_id))
        channel_name = session.query(Channel.channel_name).filter(Channel.id == local_channel_id).scalar()
        logger.debug("Video belongs to channel " + str(channel_name))
        # Download video and get Dwonload path of mkv file as return variable
        set_currently_downloading(str(channel_name) + " - " + video.video_id + " - " + video.title)
        video_file = download_video(video.video_id, channel_name)
        if video_file == "copyright":
            logger.info("This video is geoblocked on current country " + current_country + ". Will get complete geoblock list.")
            video_geoblock_list = get_geoblock_list_for_one_video(video.video_id)
            geoblock_list = ""
            for entry in video_geoblock_list:
                geoblock_list = geoblock_list + (str(entry) + ",")
            logger.debug("Geoblock list for video " + str(video.video_id) + " is " + str(geoblock_list))
            video.copyright = geoblock_list
            session.add(video)
            session.commit()
            sleep(60)
            continue
        if video_file == "forbidden":
            continue
        if video_file == "429":
            set_status("429 paused")
            set_http_429_state()
            logger.error("Got HTTP 429 from youtube. Try restarting the proxy.")
            http_429_counter += 1
            if http_429_counter == 10:
                if os.path.exists(config["base"]["download_lockfile"]):
                    os.remove(config["base"]["download_lockfile"])
                break
            if config["youtube-dl"]["proxy"] != "":
                restart_proxy()
            sleep(10)
            continue
        if video_file == "video_forbidden":
            video.online = video_status["http_403"]
            session.add(video)
            session.commit()
            continue
        if video_file == "hate_speech":
            video.online = video_status["hate_speech"]
            session.add(video)
            session.commit()
            continue
        if video_file == "not_downloaded":
            video.downloaded = None
            session.add(video)
            session.commit()
            continue
        # get all the needed video infos
        video.downloaded = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        video.runtime = get_video_duration(video_file)
        logger.debug("Video runtime was set to " + str(video.runtime) + " seconds")
        video.resolution = get_video_resolution(video_file)
        logger.debug("Video resolution was set to " + str(video.resolution))
        video.size = os.path.getsize(video_file)
        logger.debug("Video size was set to " + str(video.size) + " bytes")
        # if it was possible to download video, we can safely assume the video is online.
        # We have to set this here, in case we successfully downloaded a video which was flagged as online=2 (HTTP 403 error on first try)
        video.online = video_status["online"]
        session.add(video)
        session.commit()
        http_429_counter = 0
        logger.info("Video " + str(video.video_id) + " is downloaded.")
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "download_videos", "Downloaded video with ID " + video.video_id)
        if video_file not in ["exists_already", "copyright"]:
            rclone_upload()
        if len(videos_not_downloaded.all()) > 1:
            sleep(randint(int(config["youtube-dl"]["min_sleep_interval"]), int(config["youtube-dl"]["max_sleep_interval"])))
    if os.path.exists(config["base"]["download_lockfile"]):
        logger.debug("Removing download lockfile")
        os.remove(config["base"]["download_lockfile"])
    if video_file != "429":
        set_status("done")
    set_currently_downloading("Nothing")
    return http_429_counter


def generate_statistics(all_stats=False):
    global statistics
    # get complete rclone size of upload dir
    if all_stats:
        statistics = "archive_size,videos_monitored,videos_downloaded"
    if "archive_size" in statistics:
        start_time = get_current_timestamp()
        rclone_size_command = config["rclone"]["binary_path"] + " size " + config["rclone"]["upload_target"] + ":" + config["rclone"]["upload_base_path"] + " --json"
        logger.debug("rclone size command is: " + rclone_size_command)
        logger.info("Getting rclone size of complete archive dir")
        output = subprocess.run(rclone_size_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = str(output.stdout.decode('utf-8'))
        size_json = json.loads(stdout)
        size = size_json["bytes"]
        log_statistic("archive_size", str(size))
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "statistics_archive_size", "Getting archive size via rclone")
    if "videos_monitored" in statistics:
        start_time = get_current_timestamp()
        number_of_videos = session.query(func.count(Video.id)).scalar()
        log_statistic("videos_monitored", str(number_of_videos))
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "statistics_videos_monitored", "Getting archive size via rclone")
    if "videos_downloaded" in statistics:
        start_time = get_current_timestamp()
        number_of_videos = session.query(func.count(Video.id)).filter(Video.downloaded is not None).scalar()
        log_statistic("videos_downloaded", str(number_of_videos))
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "statistics_videos_downloaded", "Getting archive size via rclone")


def download_video(video_id, channel_name):
    youtube_dl_command = config["youtube-dl"]["binary_path"] + " --continue " + " -4 --download-archive " + config["youtube-dl"]["download-archive"] + " --output " + config["base"]["download_dir"] + "/\"" + channel_name + "\"/\"" + config["youtube-dl"]["naming-format"] + "\"" + " --ignore-config" + " --ignore-errors --merge-output-format mkv " + " --no-overwrites" + " --restrict-filenames --format \"" + config["youtube-dl"]["video-format"] + "\" --user-agent \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36\" " + config["youtube-dl"]["additional-options"]
    if config["youtube-dl"]["proxy"] != "":
        youtube_dl_command = youtube_dl_command + " --proxy " + config["youtube-dl"]["proxy"]
    youtube_dl_command = youtube_dl_command + " https://youtu.be/" + video_id
    logger.debug("youtube-dl command is: " + str(youtube_dl_command))
    output = subprocess.run(youtube_dl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.debug(str(output.stdout))
    logger.debug(str(output.stderr))
    if output.returncode != 0:
        # Check if video was blocked due to general copyright issues
        if "who has blocked it on copyright" in str(output.stderr):
            logger.error("This video is blocked due to copyright reasons.")
            downloaded_video_file = "copyright"
            return downloaded_video_file
        # Check if video was blocked due to copyright issues in own country
        if "who has blocked it in your country on copyright grounds" in str(output.stderr):
            logger.error("This video is blocked in your country due to copyright reasons.")
            downloaded_video_file = "copyright"
            return downloaded_video_file
        if "unable to download video data: HTTP Error 403: Forbidden" in str(output.stderr):
            logger.error("This video could not be downloaded")
            return "video_forbidden"
        if "HTTP Error 403: Forbidden" in str(output.stderr):
            logger.error("Something could not be downloaded for video " + video_id)
            downloaded_video_file = "forbidden"
            return downloaded_video_file
        if "HTTP Error 429" in str(output.stderr):
            logger.error("Got HTTP 429 error. Stopping here for today.")
            downloaded_video_file = "429"
            return downloaded_video_file
        if "This video has been removed for violating YouTube's policy on hate speech" in str(output.stderr):
            logger.error("This video is blocked in your current country. Try again from different country.")
            downloaded_video_file = "hate_speech"
            return downloaded_video_file
        if "WARNING: video doesn't have subtitles" in str(output.stderr):
            logger.error("Could not download subtitles for this video. Continue anyway.")
            downloaded_video_file = glob.glob(config["base"]["download_dir"] + "/" + channel_name + "/*.mkv")[0]
            logger.debug("Video name is " + downloaded_video_file)

    if "has already been recorded in archive" in str(output.stdout):
        logger.info("The video is already in youtube-dl archive file. We assume video is already downloaded. If not, remove from archive file.")
        downloaded_video_file = "exists_already"
    else:
        try:
            logger.debug("Try finding the video name in download directory.")
            downloaded_video_file = glob.glob(config["base"]["download_dir"] + "/" + channel_name + "/*.mkv")[0]
        except:
            # In some cases, youtube-dl will not merge to mkv, even if --merge-output-format mkv is set
            # so we even have to check for mp4 files in case we couldn't find an mkv
            logger.debug("Could not find mkv file. Searching for mp4 file.")
            try:
                downloaded_video_file = glob.glob(config["base"]["download_dir"] + "/" + channel_name + "/*.mp4")[0]
            except:
                logger.error("Cannot find any downloaded file. Please check youtube-dl output by hand...")
                downloaded_video_file = "not_downloaded"
        logger.debug("Video name is " + downloaded_video_file)
    return downloaded_video_file


def get_video_duration(file):
    result = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return float(result.stdout)


def get_video_resolution(file):
    result = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0", file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    resolution = result.stdout
    resolution = resolution.decode('utf-8')
    return str(resolution).strip()


def rclone_upload():
    start_time = get_current_timestamp()
    set_status("uploading")
    rclone_upload_command = config["rclone"]["binary_path"] + " move " + config["base"]["download_dir"] + " " + config["rclone"]["upload_target"] + ":" + config["rclone"]["upload_base_path"] + " --delete-empty-src-dirs"
    logger.debug("rclone upload command is: " + rclone_upload_command)
    logger.info("Uploading files to rclone remote")
    os.system(rclone_upload_command)
    end_time = get_current_timestamp()
    log_operation(end_time - start_time, "rclone_upload", "Uploaded files to rclone remote")


def toggle_download_requirement_by_username():
    # check if one or both arguments for enabled and disabled set
    video = Video()
    if enabled and disabled:
        logger.error("You can only use --disabled OR --enabled. Not both!")
    if not enabled and not disabled:
        logger.error("You have to set either --disabled OR --enabled.")
    # check if username variable is set
    if username is None:
        logger.error("--username variable is not set")
        return None
    # Get the channel internal id based on channel name from DB
    channel_internal_id = str(session.query(Channel.id).filter(Channel.channel_name == username).scalar())
    if channel_internal_id is not None:
        logger.debug("Got channel id " + channel_internal_id + " for Username " + username)
        # Get all playlists which are connected to channel internal ID
        playlist_ids = session.query(Playlist.id).filter(Playlist.channel_id == channel_internal_id).all()
        logger.debug("Found " + str(len(playlist_ids)) + " for channel " + username)
        videos = []
        if playlist_ids is not None:
            for local_playlist_id in playlist_ids:
                result = session.query(Video).filter(Video.playlist == local_playlist_id).all()
                for entry in result:
                    videos.append(entry)
            if len(videos) > 0:
                logger.debug("Found " + str(len(videos)) + " videos for channel " + username)
                video: Video
                for video in videos:
                    if enabled:
                        video.download_required = 1
                    if disabled:
                        video.download_required = 0
                    session.add(video)
                session.commit()
                logger.info("Changed " + str(len(videos)) + " videos of channel " + username + " to download required " + str(video.download_required))
            else:
                logger.error("No videos for channel " + username + " found")
        else:
            logger.error("No playlists for channel " + username + " found")
    else:
        logger.error("No channel with name" + username + " found")


def restart_proxy():
    os.system(config["base"]["proxy_restart_command"])


def check_video_ids_for_offline_state(video_ids_to_check):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    request = youtube.videos().list(part="status", id=video_ids_to_check)
    response = request.execute()
    for entry in response['items']:
        video_id = entry['id']
        video = session.query(Video).filter(Video.video_id == video_id).scalar()
        if video is not None:
            google_video_status = entry['status']['privacyStatus']
            if google_video_status == "unlisted":
                video.online = video_status["unlisted"]
                logger.info("Setting online state of video " + str(video_id) + " to unlisted, since video is still on youtube, but not in playlist anymore.")
            else:
                video.online = video_status["online"]
                logger.info("Setting online state of video " + str(video_id) + " back to online, since video is still on youtube, but not in playlist anymore.")
                video.online = video_status["online"]
            session.add(video)
        session.commit()


def verify_offline_videos():
    logger.info("Verifying offline video IDs against youtube API")
    # Get all videos with offline status 1 and 3 from database
    videos_to_verify_offline_status = session.query(Video).filter(or_(Video.online == video_status["offline"], Video.online == video_status["hate_speech"], Video.online == video_status["unlisted"])).filter(Video.download_required == 1).all()
    # if no offline videos are in database, stop here. nothing more to do.
    if videos_to_verify_offline_status is None:
        return None
    logger.debug("Found " + str(len(videos_to_verify_offline_status)) + " offline videos")
    i = 0
    j = 0
    google_api_id_limit = 50
    video_ids_to_check = ""
    while i < len(videos_to_verify_offline_status):
        if j != 0:
            video_ids_to_check = video_ids_to_check + ","
        video_ids_to_check = str(video_ids_to_check + videos_to_verify_offline_status[i].video_id)
        logger.debug("Found video ID " + str(videos_to_verify_offline_status[i].video_id) + " in offline videos.")
        i += 1
        j += 1
        if j == google_api_id_limit or i == len(videos_to_verify_offline_status):
            j = 0
            check_video_ids_for_offline_state(video_ids_to_check)
            video_ids_to_check = ""


signal.signal(signal.SIGINT, signal_handler)


if mode == "add_channel":
    add_channel(channel_id)

if mode == "add_user":
    add_user(username)

if mode == "get_playlists":
    get_playlists()

if mode == "get_video_infos":
    get_video_infos()

if mode == "download_videos":
    download_videos()

if mode == "run":
    get_playlists()
    get_video_infos()
    download_videos()
    verify_offline_videos()
    generate_statistics(True)

if mode == "generate_statistics":
    generate_statistics()

if mode == "toggle_channel_download":
    toggle_download_requirement_by_username()

if mode == "verify_offline_videos":
    verify_offline_videos()
