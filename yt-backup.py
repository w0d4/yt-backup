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
import json
import logging
import os
import pickle
import re
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
import sqlalchemy
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
parser.add_argument("mode", action="store", type=str, help="Valid options: add_channel, get_playlists, get_video_infos, download_videos, run, toggle_channel_download, generate_statistics, verify_offline_videos, verify_channels, list_playlists, modify_playlist, modify_channel, add_video")
parser.add_argument("--channel_id", action="store", type=str, help="Defines a channel ID to work on. Required for modes: add_channel")
parser.add_argument("--username", action="store", type=str, help="Defines a channel name to work on. Required for modes: add_channel")
parser.add_argument("--playlist_id", action="store", type=str, help="Defines a playlist ID to work on. Optional for modes: get_video_infos, download_videos")
parser.add_argument("--playlist_name", action="store", type=str, help="Defines a playlist name. Optional for modes: add_playlist, modify_playlist")
parser.add_argument("--download_from", action="store", type=str, help="Defines a date from which videos should be downloaded for a playlist. Format: yyyy-mm-dd hh:mm:ss or all")
parser.add_argument("--retry-403", action="store_true", help="If this flag ist set, yt-backup will retry to download videos which were marked with 403 error during initial download.")
parser.add_argument("--statistics", action="store", type=str, help="Comma seperated list which statistics should be collected during statistics run. Supported types: archive_size,videos_monitored,videos_downloaded")
parser.add_argument("--enabled", action="store_true", help="Switch to control all modes which enables or disables things. Required for modes: toggle_channel_download")
parser.add_argument("--disabled", action="store_true", help="Switch to control all modes which enables or disables things. Required for modes: toggle_channel_download")
parser.add_argument("--monitored", action="store", type=int, help="Can be 1 or 0. Is used in modify_playlist context.")
parser.add_argument("--ignore_429_lock", action="store_true", help="Ignore whether an IP was 429 blocked and continue downloading with it.")
parser.add_argument("--all_meta", action="store_true", help="When adding a channel with --channel-id, all playlists and videos will be downloaded automatically.")
parser.add_argument("--video_id", action="store", type=str, help="When adding a video with add_video, this must be added as option")
parser.add_argument("--video_title", action="store", type=str, help="When adding a video with add_video, this could be added as option")
parser.add_argument("--video_upload_date", action="store", type=str, help="When adding a video with add_video, this could be added as option")
parser.add_argument("--video_description", action="store", type=str, help="When adding a video with add_video, this could be added as option")
parser.add_argument("--downloaded", action="store", type=str, help="When adding a video with add_video, this can be added as option")
parser.add_argument("--resolution", action="store", type=str, help="When adding a video with add_video, this can be added as option")
parser.add_argument("--size", action="store", type=str, help="When adding a video with add_video, this can be added as option")
parser.add_argument("--duration", action="store", type=str, help="When adding a video with add_video, this can be added as option")
parser.add_argument("--video_status", action="store", type=str, help="When adding a video with add_video, this can be added as option")
parser.add_argument("--print_quota", action="store_true", help="Print used quota information during run.")
parser.add_argument("--force_refresh", action="store_true", help="Forces the update of video data of playlists.")
parser.add_argument("--debug", action="store_true")
parser.add_argument("-V", action="version", version="%(prog)s 0.9.5")
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

# save used quota for every run
used_quota_this_run: int = 0

# Psave the parsed arguments for easier use
mode = args.mode
channel_id = args.channel_id
playlist_id = args.playlist_id
username = args.username
statistics = args.statistics
retry_403 = args.retry_403
enabled = args.enabled
disabled = args.disabled
ignore_429_lock = args.ignore_429_lock
download_from = args.download_from
all_meta = args.all_meta
video_id = args.video_id
downloaded = args.downloaded
resolution = args.resolution
size = args.size
duration = args.duration
param_video_status = args.video_status
monitored = args.monitored
playlist_name = args.playlist_name
video_title = args.video_title
video_description = args.video_description
video_upload_date = args.video_upload_date
print_quota = args.print_quota
force_refresh = args.force_refresh

# define video status
video_status = {"offline": 0, "online": 1, "http_403": 2, "hate_speech": 3, "unlisted": 4}


def get_current_timestamp():
    ts = time.time()
    return ts


def add_quota(quota_used: int):
    global used_quota_this_run
    used_quota_this_run = used_quota_this_run + quota_used
    if print_quota:
        logger.info("This API call costed " + str(quota_used) + " API quota. Totally used " + str(used_quota_this_run) + " this run.")


def persist_quota():
    global used_quota_this_run
    if used_quota_this_run == 0:
        return None
    stat_quota = Statistic()
    stat_quota.statistic_type = "used_quota"
    stat_quota.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    stat_quota.statistic_value = str(used_quota_this_run)
    session.add(stat_quota)
    session.commit()
    if print_quota:
        logger.info("Used " + str(used_quota_this_run) + " API Quota totally this run.")
        print_quota_last_24_hours()


def commit_with_retry():
    try:
        session.commit()
    except sqlalchemy.exc.OperationalError as e:
        if "2006" in str(e):
            try:
                logger.error("Connection to database got lost. Trying to reconnect...")
                sleep(3)
                session.commit()
            except:
                raise


def print_quota_last_24_hours():
    logger.debug("Will print the quota used in the last 24h if the user wants to.")
    with engine.connect() as con:
        try:
            rs = con.execute("SELECT SUM(statistic_value) AS used_quota_last_24h FROM statistics WHERE statistics.statistic_type = 'used_quota' AND statistics.statistic_date > DATE_SUB(NOW(), INTERVAL 1 DAY);")
            for row in rs:
                logger.info("Used quota during last 24h: " + str(row[0]))
        except:
            logger.error("Problem during getting quota info from database.")


def signal_handler(sig, frame):
    logger.info('Catched Ctrl+C!')
    set_status("aborted")
    if os.path.exists(config["base"]["download_lockfile"]):
        logger.debug("Removing download lockfile")
        os.remove(config["base"]["download_lockfile"])
    sys.exit(0)


def sanititze_string(name: str):
    if "/" in name:
        name = name.replace('/', '_')
    if "\"" in name:
        name = name.replace('"', '\\"')
    if "[" in name:
        name = name.replace("[", "\\[")
    if "]" in name:
        name = name.replace("]", "\\]")
    return name


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
        logger.debug("Delta seconds since last 429: " + str(delta.total_seconds()))
        if delta.total_seconds() < 48 * 60 * 60:
            return True
        else:
            clear_http_429_state()
            return False


def set_quota_exceeded_state():
    quota_exceeded_state = session.query(Statistic).filter(Statistic.statistic_type == "quota_exceeded_state").scalar()
    if quota_exceeded_state is None:
        quota_exceeded_state = Statistic()
        quota_exceeded_state.statistic_type = "quota_exceeded_state"
    quota_exceeded_state.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    quota_exceeded_state.statistic_value = "Quota exceeded"
    session.add(quota_exceeded_state)
    session.commit()


def clear_quota_exceeded_state():
    quota_exceeded_state = session.query(Statistic).filter(Statistic.statistic_type == "quota_exceeded_state").scalar()
    if quota_exceeded_state is None:
        return None
    session.delete(quota_exceeded_state)
    session.commit()


def get_quota_exceeded_state():
    quota_exceeded_state = session.query(Statistic).filter(Statistic.statistic_type == "http_429_state").scalar()
    return quota_exceeded_state


def check_quota_exceeded_state():
    quota_exceeded_state = get_quota_exceeded_state()
    if quota_exceeded_state is None:
        return False
    else:
        logger.debug("Calculate difference since last quota_exceeded error")
        date_of_last_quota_exceeded_state = datetime.strptime(str(quota_exceeded_state.statistic_date), '%Y-%m-%d %H:%M:%S')
        current_time = datetime.now()
        delta = current_time - date_of_last_quota_exceeded_state
        logger.debug("Delta seconds since last quota_exceeded_state: " + str(delta.total_seconds()))
        if delta.total_seconds() < 48 * 60 * 60:
            return True
        else:
            clear_quota_exceeded_state()
            return False


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
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting playlists")
    request = youtube.channels().list(part="contentDetails", id=local_channel_id)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    return response


def get_playlist_name_from_google(local_playlist_id):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting playlists")
    request = youtube.playlists().list(part="snippet", id=local_playlist_id)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    return response


def is_headless_machine():
    while "the answer is invalid":
        reply = str(input("Are you working on a headless machine?" + ' (Y/N): ')).lower().strip()
        if reply[:1] == 'y':
            return True
        if reply[:1] == 'n':
            return False


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
            if is_headless_machine():
                creds = flow.run_console(authorization_prompt_message='Please visit this URL to authorize this application: {url}', authorization_code_message='Enter the authorization code: ')
            else:
                creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def get_channel_playlists(local_channel_id, monitored=1):
    global playlist_id
    logger.debug("Getting playlist IDs")
    google_response = get_playlist_ids_from_google(local_channel_id)
    if google_response is None:
        logger.error("Got no answer from google. I will skip this.")
        return None
    for playlist in google_response['items'][0]['contentDetails']['relatedPlaylists']:
        if playlist not in ["watchHistory", "watchLater", "favorites", "likes"]:
            playlist_id = str(google_response['items'][0]['contentDetails']['relatedPlaylists'][playlist])
            if session.query(Playlist).filter(Playlist.playlist_id == playlist_id).scalar() is not None:
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
            if download_from is not None:
                modify_playlist()


def get_channel_name_from_google(local_channel_id):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel name")
    request = youtube.channels().list(part="brandingSettings", id=local_channel_id)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    try:
        channel_name = str(response["items"][0]["brandingSettings"]["channel"]["title"])
    except KeyError:
        logger.error("No channel with this id could be found on youtube.")
        return None
    logger.debug("Got channel name " + channel_name + " from google.")
    return channel_name


def get_channel_id_from_google(local_username):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    global channel_id
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel id by username")
    request = youtube.channels().list(part="id", forUsername=local_username)
    try:
        response = request.execute()
        add_quota(1)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    logger.debug(str(response))
    channel_id = str(response["items"][0]["id"])
    logger.debug("Got channel name " + channel_id + " from google.")
    return channel_id


def add_channel(local_channel_id):
    # log start time
    start_time = get_current_timestamp()

    # Check if the channel is already in database
    channel = session.query(Channel.channel_id).filter(Channel.channel_id == local_channel_id).scalar()
    if channel is not None:
        logger.error("This channel is already in database.")
        return None

    # create channel object
    channel = Channel()

    # get Channel details from youtube, in case no user defined name was detected
    channel.channel_id = local_channel_id
    if username is not None:
        logger.info("Found custom channel name " + str(username) + ". Will not request official channel name from youtube API")
        channel.channel_name = str(username)
    else:
        channel_name = get_channel_name_from_google(local_channel_id)
        if channel_name is None:
            logger.error("Got no answer from google. I will skip this.")
            return None
        if "channel_naming" in config["base"] and config["base"]["channel_naming"] is not "":
            logger.debug("Found channel name template in config")
            channel.channel_name = str(config["base"]["channel_naming"]).replace("%channel_name", channel_name).replace(("%channel_id"), local_channel_id)
        else:
            channel.channel_name = channel_name
    # secure channel name
    if "/" in channel.channel_name:
        channel.channel_name = channel.channel_name.replace("/", "_")
        logger.info("Replaced all / characters in channel name with _, since / is not a valid character in file and folder names.")
    # add channel to channel table
    if session.query(Channel).filter(Channel.channel_id == local_channel_id).scalar() is None:
        logger.info("Added Channel " + channel.channel_name + " to database.")
        session.add(channel)
        session.commit()
    else:
        logger.info("Channel is already in database")
    end_time = get_current_timestamp()
    log_operation(end_time - start_time, "add_channel", "Added channel " + channel.channel_name)
    add_uploads_playlist(channel)
    if all_meta:
        get_video_infos()


def add_uploads_playlist(channel):
    logger.info("Adding default playlist uploads to the channel.")
    channels_upload_playlist_id = list(channel.channel_id)
    channels_upload_playlist_id[1] = 'U'
    playlist = Playlist()
    playlist.playlist_id = "".join(channels_upload_playlist_id)
    logger.debug('uploads playlist ID is ' + str(playlist.playlist_id))
    playlist.channel_id = channel.id
    playlist.playlist_name = 'uploads'
    if mode == "add_video":
        playlist.monitored = 0
    else:
        playlist.monitored = 1
    session.add(playlist)
    session.commit()


def get_video_infos_for_one_video(video_id):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel id by video_id")
    request = youtube.videos().list(part="snippet", id=video_id)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    return response


def get_geoblock_list_for_one_video(video_id):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting channel id by video_id")
    request = youtube.videos().list(part="contentDetails", id=video_id)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    geoblock_list = []
    logger.debug(str(response))
    try:
        for entry in response["items"][0]["contentDetails"]["regionRestriction"]["blocked"]:
            geoblock_list.append(str(entry))
            logger.debug("Found " + str(entry) + " in geoblock list of video " + str(geoblock_list) + ".")
        return geoblock_list
    except KeyError:
        logger.error("No blocked countries were found.")
        return None


def add_video(video_id, downloaded="", resolution="", size="", duration="", local_video_status="online"):
    if video_id is None:
        logger.error('You have to supply at least the video id with --video_id')
        return None
    video = session.query(Video).filter(Video.video_id == video_id).scalar()
    if video is not None:
        logger.error(f'Video with ID {video_id} is already in database. Cannot add it a second time.')
        return None
    video = Video()
    video.video_id = video_id
    if local_video_status != "offline":
        video_infos = get_video_infos_for_one_video(video_id)
        logger.debug(str(video_infos))
    if local_video_status == "offline" or video_infos["pageInfo"]["totalResults"] == 0:
        logger.error("Video with ID " + video_id + " is not available on youtube anymore")
        local_video_status = "offline"
        if video_title is None:
            logger.error('When adding offline videos, a title must be specified with --video_title.')
            return None
        if video_description is None:
            logger.error('When adding offline videos, a description must be specified with --video_description.')
            return None
        if playlist_id is None:
            logger.error('When adding offline videos, a channel_id must be specified with --playlist_id.')
            return None
        title = video_title
        description = video_description
        upload_date = video_upload_date
        video_playlist = session.query(Playlist.id).filter(Playlist.playlist_id == playlist_id)
    else:
        local_channel_id = str(video_infos["items"][0]["snippet"]["channelId"])
        title = str(video_infos["items"][0]["snippet"]["title"])
        description = str(video_infos["items"][0]["snippet"]["description"])
        upload_date = str(video_infos["items"][0]["snippet"]["publishedAt"])
        upload_date = datetime.strptime(str(upload_date)[0:19], '%Y-%m-%dT%H:%M:%S')
        add_channel(local_channel_id)
        internal_channel_id = session.query(Channel.id).filter(Channel.channel_id == local_channel_id).scalar()
        video_playlist = session.query(Playlist.id).filter(Playlist.channel_id == internal_channel_id).filter(Playlist.playlist_name == "uploads")
    video.playlist = video_playlist
    video.title = title
    video.description = description
    video.downloaded = downloaded
    video.resolution = resolution
    video.size = size
    video.runtime = duration
    if local_video_status is None:
        video.online = video_status["online"]
    else:
        video.online = video_status[local_video_status]
    video.download_required = 1
    video.upload_date = upload_date
    session.add(video)
    session.commit()
    logger.info(f'Added video {video.video_id} - {video.title} to database.')


def add_user(local_username):
    local_channel_id = get_channel_id_from_google(local_username)
    add_channel(local_channel_id)


def get_playlists():
    global playlist_id
    channels = session.query(Channel).filter(Channel.offline == None)
    save_playlist_id = playlist_id
    if channel_id is not None:
        channels = channels.filter(Channel.channel_id == channel_id)
    for channel in channels:
        start_time = get_current_timestamp()
        logger.info("Getting Playlists for " + str(channel.channel_name))
        get_channel_playlists(channel.channel_id)
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "get_playlists", "Got playlists for channel " + str(channel.channel_name))
    playlist_id = save_playlist_id
    if all_meta:
        get_video_infos()


def get_videos_from_playlist_from_google(local_playlist_id, next_page_token):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    logger.debug("Excuting youtube API call for getting videos")
    if next_page_token is None:
        request = youtube.playlistItems().list(part="snippet,contentDetails", maxResults=50, playlistId=local_playlist_id)
    else:
        request = youtube.playlistItems().list(part="snippet,contentDetails", maxResults=50, playlistId=local_playlist_id, pageToken=next_page_token)
    response = ""
    try:
        response = request.execute()
        add_quota(5)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    return response


def get_changed_playlists(playlists):
    i = 0
    j = 0
    google_api_id_limit = 50
    playlist_ids_to_check = ""
    changed_playlists = []
    while i < len(playlists):
        if j != 0:
            playlist_ids_to_check = playlist_ids_to_check + ","
        playlist_ids_to_check = str(playlist_ids_to_check + playlists[i].playlist_id)
        logger.debug("Found playlist ID " + str(playlists[i].playlist_id) + " in playlists.")
        i += 1
        j += 1
        if j == google_api_id_limit or i == len(playlists):
            j = 0
            youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
            request = youtube.playlists().list(part="contentDetails", id=playlist_ids_to_check)
            try:
                response = request.execute()
                add_quota(3)
            except googleapiclient.errors.HttpError as error:
                if "The request cannot be completed because you have exceeded your" in str(error):
                    set_quota_exceeded_state()
                return None
            logger.debug("Calling youtube API for playlist etags")
            logger.debug("Got " + str(len(response["items"])) + " entries back")
            for entry in response["items"]:
                plid = entry["id"]
                etag = entry["etag"]
                logger.debug("etag from google for playlist " + str(plid) + " is: " + str(etag))

                playlist = session.query(Playlist).filter(Playlist.playlist_id == plid).scalar()
                channel_name = session.query(Channel.channel_name).filter(Channel.id == playlist.channel_id).scalar()
                if force_refresh:
                    logger.info("--force_refresh is set. Deleting etag on playlist " + str(playlist.playlist_name) + " of channel " + str(channel_name))
                    playlist.etag = None
                else:
                    logger.debug("etag in database for playlist " + str(plid) + " is: " + str(etag))
                if playlist.etag != etag:
                    playlist.etag = etag
                    logger.debug("Updated etag of playlist " + str(playlist.playlist_id) + " to " + str(etag))
                    changed_playlists.append(playlist)
                    session.add(playlist)
                else:
                    logger.info("playlist " + str(playlist.playlist_name) + " of channel " + str(channel_name) + " has not changed since last check.")
            playlist_ids_to_check = ""
    session.commit()
    num_changed_playlists = len(changed_playlists)
    logger.info(f'{num_changed_playlists} playlists changed.')
    return changed_playlists


def get_video_infos():
    playlists = session.query(Playlist).filter(Playlist.monitored == 1)
    if channel_id is not None:
        internal_channel_id = session.query(Channel.id).filter(Channel.channel_id == channel_id)
        playlists = playlists.filter(Playlist.channel_id == internal_channel_id)
    if playlist_id is not None:
        playlists = playlists.filter(Playlist.playlist_id == playlist_id)
    changed_playlists = get_changed_playlists(playlists.all())

    for playlist in changed_playlists:
        parsed_from_api = 0
        start_time = get_current_timestamp()
        videos = []
        videos_to_check_against = []
        channel_name = session.query(Channel.channel_name).filter(Channel.id == playlist.channel_id).scalar()
        logger.info("Getting all video metadata for playlist " + playlist.playlist_name + " for channel " + str(channel_name))
        results = []
        try:
            result = get_videos_from_playlist_from_google(playlist.playlist_id, None)
            if result is None:
                logger.error("Got no answer from google. I will skip this.")
                return None
        except googleapiclient.errors.HttpError:
            logger.error("Playlist " + playlist.playlist_name + " in Channel " + channel_name + " is not available")
            continue
        results.append(result)
        videos_in_playlist = result["pageInfo"]["totalResults"]
        logger.debug("Videos in Playlist: " + str(videos_in_playlist))
        next_page_token = None
        try:
            next_page_token = str(result["nextPageToken"])
            logger.debug("Next page token: " + next_page_token)
        except KeyError:
            logger.debug("Playlist " + playlist.playlist_name + " in Channel " + channel_name + " has only one page.")
        while next_page_token is not None:
            result = get_videos_from_playlist_from_google(playlist.playlist_id, next_page_token)
            if result is None:
                logger.error("Got no answer from google. I will skip this.")
                return None
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
                video.upload_date = video_raw["snippet"]["publishedAt"]
                video.upload_date = datetime.strptime(str(video.upload_date)[0:19], '%Y-%m-%dT%H:%M:%S')
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
                    if video.upload_date is None:
                        logger.info("Adding upload date to video")
                        video.upload_date = datetime.strptime(str(video_raw["snippet"]["publishedAt"])[0:19], '%Y-%m-%dT%H:%M:%S')
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
    playlist_videos_in_db = session.query(Video).filter(Video.playlist == local_playlist_id).filter(Video.online == video_status["online"]).filter(Video.downloaded != None).all()
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


def remove_youtube_video_from_archive_file(local_video_id: str):
    logger.debug("Trying to remove " + local_video_id + " from " + str(config["youtube-dl"]["download-archive"]))
    with open(config["youtube-dl"]["download-archive"], "r") as f:
        lines = f.readlines()
        logger.debug("Read the file " + str(config["youtube-dl"]["download-archive"]))
    with open(config["youtube-dl"]["download-archive"], "w") as f:
        for line in lines:
            if line.strip("\n") != "youtube " + str(local_video_id):
                f.write(line)
            else:
                logger.info("Removed video id " + local_video_id + " from " + str(config["youtube-dl"]["download-archive"]))


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
        try:
            r = requests.get("https://ipinfo.io", proxies=proxies)
        except requests.exceptions.ConnectionError:
            logger.error("Cannot get connection to ipinfo.io")
            r = None
    else:
        r = requests.get("https://ipinfo.io")
    if r is not None:
        answer = json.loads(str(r.text))
        current_country = answer["country"]
    else:
        current_country = ""
    video_file = None
    http_429_counter = 0
    if playlist_id is None:
        logger.debug("Playlist ID for downloading is None. Getting all videos.")
        if retry_403:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(or_(Video.online == video_status["online"], Video.online == video_status["hate_speech"], Video.online == video_status["http_403"], Video.online == video_status["unlisted"])).filter(Video.download_required == 1)
        else:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(or_(Video.online == video_status["online"], Video.online == video_status["hate_speech"], Video.online == video_status["unlisted"])).filter(Video.download_required == 1)
    else:
        logger.debug("Playlist ID for downloading is " + str(playlist_id))
        playlist_internal_id = session.query(Playlist.id).filter(Playlist.playlist_id == playlist_id).scalar()
        logger.debug("Got playlist internal ID " + str(playlist_internal_id))
        if retry_403:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(Video.playlist == str(playlist_internal_id)).filter(or_(Video.online == video_status["online"], Video.online == video_status["http_403"], Video.online == video_status["hate_speech"], Video.online == video_status["unlisted"])).filter(Video.download_required == 1)
        else:
            videos_not_downloaded = session.query(Video).filter(Video.downloaded == None).filter(Video.playlist == playlist_internal_id).filter(or_(Video.online == video_status["online"], Video.online == video_status["hate_speech"], Video.online == video_status["unlisted"])).filter(Video.download_required == 1)
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
                    commit_with_retry()
                    continue
        # Get the playlist object of a video. If uploaded date is older than playlist download date, skip download and set download required to 0
        playlist = session.query(Playlist).filter(Playlist.id == video.playlist).scalar()
        if playlist.download_from_date is not None:
            playlist_download_date = datetime.strptime(str(playlist.download_from_date), '%Y-%m-%d %H:%M:%S')
            video_upload_date = datetime.strptime(str(video.upload_date), '%Y-%m-%d %H:%M:%S')
            if playlist_download_date > video_upload_date:
                video.download_required = 0
                session.add(video)
                commit_with_retry()
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
            if video_geoblock_list is not None:
                for entry in video_geoblock_list:
                    geoblock_list = geoblock_list + (str(entry) + ",")
                logger.debug("Geoblock list for video " + str(video.video_id) + " is " + str(geoblock_list))
                video.copyright = geoblock_list
                session.add(video)
                commit_with_retry()
                sleep(60)
            continue
        if video_file == "forbidden":
            continue
        if video_file == "503":
            remove_youtube_video_from_archive_file(str(video.video_id))
            sleep(60)
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
            logger.info("Setting video status to forbidden. I you want to retry the download, add --retry-403")
            session.add(video)
            commit_with_retry()
            continue
        if video_file == "hate_speech":
            video.online = video_status["hate_speech"]
            session.add(video)
            commit_with_retry()
            continue
        if video_file == "not_downloaded":
            video.downloaded = None
            session.add(video)
            commit_with_retry()
            continue
        # get all the needed video infos
        video.downloaded = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        video.runtime = get_video_duration(video_file)
        logger.debug("Video runtime was set to " + str(video.runtime) + " seconds")
        video.resolution = get_video_resolution(video_file)
        logger.debug("Video resolution was set to " + str(video.resolution))
        try:
            video.size = os.path.getsize(video_file)
        except:
            logger.error("Could not find size for video " + str(video.video_id))
            video.size = None
        logger.debug("Video size was set to " + str(video.size) + " bytes")
        # if it was possible to download video, we can safely assume the video is online.
        # We have to set this here, in case we successfully downloaded a video which was flagged as online=2 (HTTP 403 error on first try)
        video.online = video_status["online"]
        session.add(video)
        commit_with_retry()
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
        number_of_videos = session.query(func.count(Video.id)).filter(Video.downloaded != None).scalar()
        log_statistic("videos_downloaded", str(number_of_videos))
        end_time = get_current_timestamp()
        log_operation(end_time - start_time, "statistics_videos_downloaded", "Getting archive size via rclone")


def get_downloaded_video_name(youtube_dl_stdout):
    downloaded_file = None
    youtube_dl_stdout = youtube_dl_stdout.decode('utf-8')
    logger.debug("youtube-dl stdout: " + youtube_dl_stdout)
    youtube_dl_stdout = youtube_dl_stdout.splitlines()
    for line in youtube_dl_stdout:
        logger.debug("Current line: " + str(line))
        line_found = re.findall(r'Merging formats into', line)
        if line_found:
            logger.debug("Found name in line: " + line)
            downloaded_file = line.split('"')[1]
            logger.debug("Parsed downloaded file " + downloaded_file + " from youtube-dl output.")
            return downloaded_file
    # If no merged video found, get the MP4 destination
    for line in youtube_dl_stdout:
        logger.debug("Current line: " + str(line))
        line_found = re.findall(r'\[download\] Destination:', line)
        if line_found:
            logger.debug("Found name in line: " + line)
            downloaded_file = line.split(':')[1].strip()
            logger.debug("Parsed downloaded file " + downloaded_file + " from youtube-dl output.")
            return downloaded_file
    return "not_downloaded"


def download_video(video_id, channel_name):
    logger.debug('Escaped Channel name is ' + sanititze_string(channel_name))
    youtube_dl_command = config["youtube-dl"]["binary_path"] + " --continue " + " -4 --download-archive " + config["youtube-dl"]["download-archive"] + " --output " + config["base"]["download_dir"] + "/\"" + channel_name + "\"/\"" + config["youtube-dl"]["naming-format"] + "\"" + " --ignore-config" + " --ignore-errors --merge-output-format mkv " + " --no-overwrites" + " --format \"" + config["youtube-dl"]["video-format"] + "\" --user-agent \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36\" " + config["youtube-dl"]["additional-options"]
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
        if "HTTP Error 403: Forbidden" in str(output.stderr) or "Got server HTTP error: Downloaded" in str(output.stdout):
            logger.error("Something could not be downloaded for video " + video_id)
            downloaded_video_file = "video_forbidden"
            return downloaded_video_file
        if "HTTP Error 429" in str(output.stderr):
            logger.error("Got HTTP 429 error. Stopping here for today.")
            downloaded_video_file = "429"
            return downloaded_video_file
        if "HTTP Error 503" in str(output.stderr):
            logger.error("Got HTTP 503 error. Will sleep for a while and continue with next video. This video will be downloaded again next run.")
            downloaded_video_file = "503"
            return downloaded_video_file
        if "This video has been removed for violating YouTube's policy on hate speech" in str(output.stderr):
            logger.error("This video is blocked in your current country. Try again from different country.")
            downloaded_video_file = "hate_speech"
            return downloaded_video_file
        if "WARNING: video doesn't have subtitles" in str(output.stderr):
            downloaded_video_file = get_downloaded_video_name(output.stdout)
            logger.debug("Video name is " + downloaded_video_file)
            return downloaded_video_file


    if "has already been recorded in archive" in str(output.stdout):
        logger.info("The video is already in youtube-dl archive file. We assume video is already downloaded. If not, remove from archive file.")
        downloaded_video_file = "exists_already"
    else:
        downloaded_video_file = get_downloaded_video_name(output.stdout)
        logger.debug("Video name is " + downloaded_video_file)
        return downloaded_video_file
    return downloaded_video_file


def get_video_duration(file):
    result = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = None
    try:
        retval = float(result.stdout)
    except ValueError:
        logger.error("Could not find video size for video " + str(file))
    return retval


def get_video_resolution(file):
    result = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0", file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    resolution = result.stdout
    resolution = resolution.decode('utf-8')
    if str(file) in resolution:
        logger.error("Could not find resolution for video " + str(file))
        return None
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
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    request = youtube.videos().list(part="status", id=video_ids_to_check)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
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


def check_channel_ids_for_offline_state(channel_ids_to_check):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    request = youtube.channels().list(part="status", id=channel_ids_to_check)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    online_ids = []
    # Put all channel ID's received from youtube into a list
    for entry in response['items']:
        online_ids.append(entry['id'])
        logger.debug("Found channel " + str(entry['id']) + " in online video list.")
    channel_ids_to_check_list = channel_ids_to_check.split(",")
    logger.info("Updating online status of all channels.")
    for local_channel_id in channel_ids_to_check_list:
        channel = session.query(Channel).filter(Channel.channel_id == local_channel_id).scalar()
        if channel is not None:
            logger.debug("Updating online status of channel " + str(channel.channel_name))
            if channel.channel_id not in online_ids and channel.offline is None:
                logger.info("Channel " + str(channel.channel_name) + " is not online anymore. Setting status to offline.")
                channel.offline = 1
                session.add(channel)
                channel_playlists = session.query(Playlist).filter(Playlist.channel_id == channel.id).all()
                if channel_playlists is not None:
                    logger.info("Getting all playlists for channel " + str(channel.channel_name) + " from database for setting them all unmonitored.")
                    for playlist in channel_playlists:
                        logger.debug("Setting playlist " + str(playlist.playlist_id) + " to unmonitored, since channel is not existing anymore.")
                        playlist.monitored = 0
                        session.add(playlist)
                        playlists_videos = session.query(Video).filter(Video.playlist == playlist.id).all()
                        if playlists_videos is not None:
                            logger.info("Getting all videos for playlist " + str(playlist.playlist_name) + " of channel " + str(channel.channel_name) + " for setting them offline.")
                            for video in playlists_videos:
                                logger.debug("Setting video " + str(video.video_id) + " to state offline, since channel is not exsisting anymore.")
                                video.online = 0
                                session.add(video)

            if channel.channel_id in online_ids and channel.offline is not None:
                logger.info("Channel " + str(channel.channel_name) + " is online. Setting status to online.")
                channel.offline = None
                session.add(channel)
                channel_playlists = session.query(Playlist).filter(Playlist.channel_id == channel.id).all()
                if channel_playlists is not None:
                    logger.info("Getting all playlists for channel " + str(channel.channel_name) + " from database for setting them all monitored.")
                    for playlist in channel_playlists:
                        logger.debug("Setting playlist " + str(playlist.playlist_id) + " to monitored, since channel is existing again.")
                        playlist.monitored = 1
                        session.add(playlist)
                        playlists_videos = session.query(Video).filter(Video.playlist == playlist.id).all()
                        if playlists_videos is not None:
                            logger.info("Getting all videos for playlist " + str(playlist.playlist_name) + " of channel " + str(channel.channel_name) + " for setting them online again.")
                            for video in playlists_videos:
                                logger.debug("Setting video " + str(video.video_id) + " to state online, since channel is exsisting again.")
                                video.online = 1
                                session.add(video)
        session.commit()


def verify_channels():
    logger.info("Verifying channel status against youtube API for all channels")
    # Channels with without offline flag from database
    channels_to_verify_online_status = session.query(Channel).filter(Channel.offline == None).all()
    # if no offline videos are in database, stop here. nothing more to do.
    if channels_to_verify_online_status is None:
        return None
    logger.debug("Found " + str(len(channels_to_verify_online_status)) + " channels which where online until now")
    i = 0
    j = 0
    google_api_id_limit = 50
    channel_ids_to_check = ""
    while i < len(channels_to_verify_online_status):
        if j != 0:
            channel_ids_to_check = channel_ids_to_check + ","
        channel_ids_to_check = str(channel_ids_to_check + channels_to_verify_online_status[i].channel_id)
        logger.debug("Found channel ID " + str(channels_to_verify_online_status[i].channel_id) + " in online channels.")
        i += 1
        j += 1
        if j == google_api_id_limit or i == len(channels_to_verify_online_status):
            j = 0
            check_channel_ids_for_offline_state(channel_ids_to_check)
            channel_ids_to_check = ""


def list_playlists():
    channels = session.query(Channel)
    if username is not None:
        channels = channels.filter(Channel.channel_name == username)
    if channel_id is not None:
        channels = channels.filter(Channel.channel_id == channel_id)
    for channel in channels:
        playlists = session.query(Playlist).filter(Playlist.channel_id == channel.id)
        print(f'ID: {channel.id} Channel Name: {channel.channel_name} Youtube Channel-ID: {channel.channel_id}')
        for playlist in playlists:
            if playlist.download_from_date is None:
                download_from_date = "All"
            else:
                download_from_date = str(playlist.download_from_date)
            video_count = len(session.query(Video.id).filter(Video.playlist == playlist.id).all())
            print(f'ID: {playlist.id} Playlist Name: {playlist.playlist_name} Youtube Playlist-ID: {playlist.playlist_id} Download From: {download_from_date} Monitored: {playlist.monitored} etag: {playlist.etag} Videos: {video_count}')
        print('\n')


def check_video_ids_for_upload_date(video_ids_to_check, download_date_limit=None):
    # Check for exceeded google quota
    if check_quota_exceeded_state():
        logger.error("Cannot proceed with getting data from youtube API. Quota exceeded.")
        return None
    logger.debug("Getting upload date from google for the following video ids: " + str(video_ids_to_check))
    youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=get_google_api_credentials())
    request = youtube.videos().list(part="snippet", id=video_ids_to_check)
    try:
        response = request.execute()
        add_quota(3)
    except googleapiclient.errors.HttpError as error:
        if "The request cannot be completed because you have exceeded your" in str(error):
            set_quota_exceeded_state()
        return None
    for entry in response['items']:
        video_id = entry['id']
        video = session.query(Video).filter(Video.video_id == video_id).scalar()
        google_published_at = entry["snippet"]["publishedAt"]
        video.upload_date = datetime.strptime(str(google_published_at)[0:19], '%Y-%m-%dT%H:%M:%S')
        logger.debug("Set upload date of video " + str(video.video_id) + " to " + str(video.upload_date))
        if download_date_limit is not None:
            if video.upload_date >= download_date_limit:
                logger.debug("Video " + str(video.video_id) + " uploaded date " + str(video.upload_date) + " is newer then given limit date " + str(download_date_limit) + ". Setting download required for video to 1")
                video.download_required = 1
            else:
                video.download_required = 0
                logger.debug("Video " + str(video.video_id) + " uploaded date " + str(video.upload_date) + " is older then given limit date " + str(download_date_limit) + ". Setting download required for video to 0")
            session.add(video)
    session.commit()


def modify_playlist():
    global download_from
    if playlist_id is None:
        logger.error("--playlist-id is needed. If you don't know your playlist ID, try the \"python3 yt-backup.py list_playlists\" command.")
        return None
    playlist = session.query(Playlist).filter(Playlist.playlist_id == playlist_id).scalar()
    if playlist is None:
        logger.error("Given playlist-id is not in database. Please find correct one with \"python3 yt-backup.py list_playlists\" command or add channel with playlist first.")
        return None
    if download_from == "now":
        download_from = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    if download_from == "all":
        playlist.download_from_date = None
        videos = session.query(Video).filter(Video.playlist == playlist.id)
        for video in videos:
            video.download_required = 1
            session.add(video)
        session.commit()
    if download_from is not None and download_from != "all":
        playlist.download_from_date = datetime.strptime(str(download_from), '%Y-%m-%d %H:%M:%S')
        videos = session.query(Video).filter(Video.playlist == playlist.id)
        videos_without_upload_date = []
        for video in videos:
            if video.upload_date is None:
                logger.debug("Video " + str(video.video_id) + " does not have an upload date. Have to check later.")
                videos_without_upload_date.append(video)
                continue
            else:
                if video.upload_date >= playlist.download_from_date:
                    logger.debug("Video " + str(video.video_id) + " uploaded date " + str(video.upload_date) + " is newer then playlist download from date " + str(playlist.download_from_date) + ". Setting download required for video to 1")
                    video.download_required = 1
                else:
                    logger.debug("Video " + str(video.video_id) + " uploaded date " + str(video.upload_date) + " is older then playlist download from date " + str(playlist.download_from_date) + ". Setting download required for video to 0")
                    video.download_required = 0
                session.add(video)
                session.commit()
        logger.debug("Found " + str(len(videos_without_upload_date)) + " videos without upload date.")
        i = 0
        j = 0
        google_api_id_limit = 50
        video_ids_to_check = ""
        while i < len(videos_without_upload_date):
            if j != 0:
                video_ids_to_check = video_ids_to_check + ","
            video_ids_to_check = str(video_ids_to_check + videos_without_upload_date[i].video_id)
            logger.debug("Found video ID " + str(videos_without_upload_date[i].video_id) + " in videos without upload date.")
            i += 1
            j += 1
            if j == google_api_id_limit or i == len(videos_without_upload_date):
                j = 0
                check_video_ids_for_upload_date(video_ids_to_check, playlist.download_from_date)
                video_ids_to_check = ""
    if monitored is 1 or monitored is 0:
        playlist.monitored = monitored
        logger.info('Set monitored flag of the playlist to ' + str(playlist.monitored))
    session.add(playlist)
    session.commit()


def verify_and_update_data_model():
    current_data_model_version_stat: Statistic = session.query(Statistic).filter(Statistic.statistic_type == "data_model_version").scalar()
    logger.debug("Current data model: " + str(current_data_model_version_stat))
    if current_data_model_version_stat is None:
        logger.debug("Current data model is None. Updating to v1.")
        current_data_model_version_stat = Statistic()
        current_data_model_version = 1
        current_data_model_version_stat.statistic_value = str(current_data_model_version)
        current_data_model_version_stat.statistic_type = "data_model_version"
        current_data_model_version_stat.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        with engine.connect() as con:
            try:
                rs = con.execute('ALTER TABLE playlists ADD download_from_date DATETIME NULL DEFAULT NULL AFTER channel_id;')
                rs = con.execute('ALTER TABLE videos ADD upload_date DATETIME NULL DEFAULT NULL AFTER download_required;')
                logger.info("Data model has been updated to " + str(current_data_model_version))
            except sqlalchemy.exc.OperationalError:
                logger.info("Table columns are already existing.")
        session.add(current_data_model_version_stat)
        session.commit()

    current_data_model_version_stat: Statistic = session.query(Statistic).filter(Statistic.statistic_type == "data_model_version").scalar()
    logger.debug("Current data model: " + str(current_data_model_version_stat))
    if current_data_model_version_stat.statistic_value == "1":
        logger.debug("Current data model is None. Updating to v2.")
        current_data_model_version = 2
        current_data_model_version_stat.statistic_value = str(current_data_model_version)
        current_data_model_version_stat.statistic_type = "data_model_version"
        current_data_model_version_stat.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        with engine.connect() as con:
            try:
                rs = con.execute('ALTER TABLE channels ADD offline INT NULL AFTER channel_name;')
                logger.info("Data model has been updated to " + str(current_data_model_version))
            except sqlalchemy.exc.OperationalError:
                logger.info("Table columns are already existing.")
        session.add(current_data_model_version_stat)
        session.commit()

    current_data_model_version_stat: Statistic = session.query(Statistic).filter(Statistic.statistic_type == "data_model_version").scalar()
    logger.debug("Current data model: " + str(current_data_model_version_stat))
    if current_data_model_version_stat.statistic_value == "2":
        logger.debug("Current data model is None. Updating to v3.")
        current_data_model_version = 3
        current_data_model_version_stat.statistic_value = str(current_data_model_version)
        current_data_model_version_stat.statistic_type = "data_model_version"
        current_data_model_version_stat.statistic_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        with engine.connect() as con:
            try:
                rs = con.execute('ALTER TABLE playlists ADD etag VARCHAR(255) NULL AFTER download_from_date;')
                logger.info("Data model has been updated to " + str(current_data_model_version))
            except sqlalchemy.exc.OperationalError:
                logger.info("Table columns are already existing.")
        session.add(current_data_model_version_stat)
        session.commit()


def modify_channel():
    if username == None:
        logger.error("You need a new username to set for the channel with --username.")
        return None
    if channel_id == None:
        logger.error("You need to specify a valid channel ID to rename with --channel_id.")
        return None
    channel = session.query(Channel).filter(Channel.channel_id == channel_id).scalar()
    if channel is None:
        logger.error("Could not get any channel with channel_id " + str(channel_id) + " from database. Please verify if channel_id is a valid channel_id with \"python3 yt-backup list_playlists\"")
        return None
    logger.debug("Found channel with current name " + str(channel.channel_name) + " in database.")
    logger.info("Will rename channel " + str(channel.channel_name) + " to " + str(username).replace("/", "_"))
    logger.warning("This action will not move any files. Please rename channel directories by hand.")
    if "/" in str(username):
        channel.channel_name = str(username).replace("/", "_")
        logger.info("Replaced all / characters in channel name with _, since / is not a valid character in file and folder names.")
    else:
        channel.channel_name = str(username)
    session.add(channel)
    session.commit()


def add_playlist():
    global playlist_name
    if playlist_id is None:
        logger.error("You must define the playlist_id to add with --playlist_id")
        return None
    if channel_id is None:
        logger.error("You must define the channel_id to which the playlist belongs with --channel_id")
        return None
    channel = session.query(Channel).filter(Channel.channel_id == channel_id).scalar()
    if channel is None:
        logger.error("The defined channel with channel ID " + channel_id + " could not be found in database. Please add it first.")
        return None
    playlist = session.query(Playlist).filter(Playlist.playlist_id == playlist_id).scalar()
    if playlist is not None:
        logger.error("This playlist is already in database. Cannot add it a second time.")
        return None
    playlist = Playlist()
    if playlist_name is None:
        response = get_playlist_name_from_google(playlist_id)
        if response is None:
            logger.error("Got no answer from google. I will skip this.")
            return None
        try:
            playlist_name = str(response["items"][0]["snippet"]["title"]).replace(" ", "_")
        except:
            logger.error("Cannot find this playlist id on youtube. Please verify the playlist ID")
            return None
    playlist.playlist_id = playlist_id
    playlist.channel_id = channel.id
    playlist.playlist_name = playlist_name
    playlist.monitored = 1
    if monitored:
        playlist.monitored = monitored
    session.add(playlist)
    session.commit()
    logger.info(f'Added playlist {playlist.playlist_id} with name {playlist.playlist_name} to the database.')


signal.signal(signal.SIGINT, signal_handler)

verify_and_update_data_model()

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
    verify_channels()
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

if mode == "list_playlists":
    list_playlists()

if mode == "modify_playlist":
    modify_playlist()

if mode == "modify_channel":
    modify_channel()

if mode == "add_video":
    add_video(video_id, downloaded, resolution, size, duration, param_video_status)

if mode == "add_playlist":
    add_playlist()

if mode == "verify_channels":
    verify_channels()

persist_quota()
