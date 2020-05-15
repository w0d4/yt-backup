
# yt-backup
Youtube Backup made easy

## What is this?
This is a backup utility for youtube videos. Since youtube-dl works very well, yt-backup will use youtube-dl as downloader.
Since youtube started aggressively blocking extensive youtube-dl crawls with HTTP 429 error, I have started fetching all metadata about channels, playlists and videos via google's youtube API and using youtube-dl only for downloading videos itself.
All metadata will be stored in a database. To be flexible at this point, I have chosen sqlalchemy as ORM. So you can use any database which is supported by sqlalchemy.
Databases are fine for storing stuff and getting it again, but not for visualizing things, I have created some grafana dashboards for visualizing the stats of the tool.
Additionally, I have added support for automatic proxy restarts, in case you get a 429 error on your current IP. This option assumes, that after each command execution, the proxy will have a new IP adress.

##  Dependencies
- [Python 3.x.](http://www.python.org/)
- Packages: sqlalchemy ConfigParser google-api-python-client google_auth_oauthlib mysql dateutil

Install them with `pip`:
```
$ pip install -r requirements.txt
```
- Any sqlalchemy supported database with utf8mb4 support
- [rclone](https://rclone.org/)
- [youtube-dl](http://ytdl-org.github.io/youtube-dl/)
- YouTube API key
- credentials.json file for your API key
- working grafana installation

## Installation
1. Clone this repo
2. Create user in your DBMS with write permissions for a schema with utf8mb4 encoding
```sql
CREATE DATABASE mydatabase CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL ON mydatabase.* TO 'user' IDENTIFIED BY 'password';
```
3. Configure a rclone remote. If remote points to cloud storage, I strongly recommend to add a crypt remote
4. Modify `config.json` to match your system paths, database and rclone remote

   ***Note:*** `git commit` your config.json, so it will not be overwritten by new ones in the repo every time you pull   

5. Put your client secret json from Google into project directory and name it `client_secret.json`
6. Add your database as datasource in Grafana. Best name it yt-backup.

### Creating client_secrets file
- Go to the Google [console](https://console.developers.google.com/).
- *Create project*.
- Side menu: *APIs & auth -> APIs*.
- Top menu: *Enabled API(s)*: Enable all YouTube APIs.
- Side menu: *APIs & auth -> Credentials*.
- *Create a Client ID*: Add credentials -> OAuth 2.0 Client ID -> Other -> Name: youtube-download -> Create -> OK
- *Download JSON*: Under the section "OAuth 2.0 client IDs". Save the file to your local system.
- Copy this JSON to `client_secret.json` in the project directory.


### Automatic downloading using systemd
- Copy yt-backup.service and yt-backup.timer from systemd-units folder to /etc/systemd/system/
- Edit /etc/systemd/system/yt-backup.service and replace all placeholders with your system specific values
- Edit /etc/systemd/system/yt-backup.timer and insert as many times as you want
- As root run `systemctl daemon-reload`
- As root run `systemctl enable --now yt-backup.timer`

## Config options
### database
- connection_info: Connection information to your already installed database. Make shure to append ?charset=utf8mb4 or something matching for your database engine.

### base
- download_dir: Directory where youtube-dl should put your videos before uploading it via rclone. BE CAREFUL!!! This directory will be cleaned with every new run. All data in this directory will be lost!
- download_lockfile: Where to put download lockfile. This prevents, that multiple download jobs will run if script is planned via job
- proxy_restart_command: If you have a proxy which can change it's IP adress, add it's restart command here.

### rclone
- binary_path: Where to find your clone binary
- config_path: Where to find your rclone config file
- move_or_copy: Should rclone move or copy videos after download. Strongly recommend move.
- upload_base_path: Where to upload the videos in your rclone remote
- upload_target: The rclone remote to which the videos should be pushed

### youtube-dl
- binary_path: Where to find your youtube-dl binary
- download-archive: Where to find your youtube-dl download archive. Could be an existing file.
- video-format: This will be put to youtube-dl as --format option. Defaults to the best video possible
- min_sleep_interval: How many seconds to sleep between two video downloads minimum
- max_sleep_interval: How many seconds to sleep between two video downloads maximum
- proxy: Which proxy and port youtube-dl should use to download videos. Leave empty for No proxy usage

## Usage
### Get help output
- `python3 yt-backup.py --help`

### Add a channel
#### By channel ID (better option)
- `python3 yt-backup.py add_channel --channel_id <youtube-channel-id>`
#### By channel ID with custom channel name
- `python3 yt-backup.py add_channel --channel_id <youtube-channel-id> --username <custom name>`
#### By username
- `python3 yt-backup.py add_channel --username <youtube-user-id>`
#### By channel id with downloading all playlists and video infos and limit video download to videos starting from now
- `python3 yt-backup.py add_channel --channel_id <youtube-channel-id> --all_meta --download_from now`

### Get all playlists for channels
#### For all channels
- `python3 yt-backup.py get_playlists`
#### For only one channel
- `python3 yt-backup.py get_playlists --channel_id <youtube channel_id>`

### Get all videos from all playlists
- `python3 yt-backup.py get_video_infos`

### Download all videos which are not downloaded currently
- `python3 yt-backup.py download_videos`

### Download all videos from one specific playlist ID
- `python3 yt-backup.py download_videos --playlist_id`

All videos which are in database, but not in the channel's playlist anymore, will be marked as offline.

If you want to know if they are completely gone or just private, you should run `python3 yt-backup.py verify_offline_videos`

### Generate Statistics
- `python3 yt-backup.py generate_statistics --statistics <archive_size,videos_monitored,videos_downloaded>`

### Get playlists, get video infos, download new videos, check all offline videos against youtube API and generate statistics in one command
- `python3 yt-backup.py run`

### Enable or disable the download for videos of a channel
- `python3 yt-backup.py toggle_channel_download --username <channel_name> --disable`
- `python3 yt-backup.py toggle_channel_download --username <channel_name> --enable`

### Verify all marked offline videos if they are really offline or only private
- `python3 yt-backup.py verify_offline_videos`
All videos which are marked as offline in database will be checked in packages of 50 videos against the YouTube API. Each video which is not returned in answer, will be marked as offline. If a video is part of the answer, it will be marked as online again or as unlisted if the API reports this.

### List channels with playlists
#### For all channels
- `python3 yt-backup.py list_playlists`
#### For only one channel by username
- `python3 yt-backup.py list_playlists --username <channel name from DB>`
#### For only one channel by channel ID
- `python3 yt-backup.py list_playlists --channel_id <channel_id>`

### Modify a playlist
#### Set a specific date and time for download date limit
- `python3 yt-backup.py modify_playlist --playlist_id <playlist_id> --download_from "2019-06-01 00:00:00"`
#### Remove download date limit from playlist
- `python3 yt-backup.py modify_playlist --playlist_id <playlist_id> --download_from all`
##### What will happen?
If a video has no upload date, it will be checked against YouTube API to get download date.
If a videos upload date is newer than it's playlist download date limit, download required will be set to 1. Else it will be set to 0.
#### Change a playlists monitored state
- `python3 yt-backup.py modify_playlist --playlist_id <playlist_id> --monitored <0/1>`

### Rename a channel
- `python3 yt-backup.py modify_channel --channel_id <channel_id> --username <new channel name>`
The channel will be renamed in database to something new. Spaces will be replaced by _.
No files will be moved. You have to do this by hand.

### Add a playlist manually
You can add a playlist by hand. This can be useful in case you have the playlist ID of a unlisted Playlist
For this you need the playlist ID and the channel ID to which the playlist belongs
Optionally you can add --playlist_name <name> and --monitored <1/0> in case you want to change the defaults
- `python3 yt-backup.py add_playlist --playlist_id <playlist_id> --channel_id <channel_id>`

### Add a single video
You can add a single video to the script. You must specify the video_id with --video_id
If the video belongs to a channel which is not in database, it will be added
If the channel must be added, it's playlists will be fetched and added as not monitored, so only the added video will be in the playlist

Optionally, you can add the following parameters (all values are sample values):
- --downloaded "YYYY-MM-DD hh:mm:ss" in case you have the video already downloaded
- --resolution 1920x1080
- --size 12345 (size must be specified in bytes)
- --duration 1234s (duration must be specified in seconds)
- --video_status unlisted (default is online, if you want to add an unlisted video, use unlisted)
- `python3 yt-backup.py add_video --video_id <video_id>`


## Grafana Dashboards
You need a running grafana installation for this.
There is also an [official docker](https://grafana.com/docs/grafana/latest/installation/docker/) image in case you do not have a running grafana installation.

### Create a new views in your database
#### MySQL/MariaDB
```SQL
CREATE OR REPLACE
ALGORITHM = UNDEFINED
VIEW downloaded_and_available_videos_by_channel
AS SELECT 
t1.channel_name, video_count, COALESCE(downloaded_count,0) as downloaded_count
FROM (
	SELECT channels.channel_name AS channel_name,COUNT(*) AS video_count
	FROM videos
	INNER JOIN playlists ON videos.playlist=playlists.id
	INNER JOIN channels ON playlists.channel_id=channels.id GROUP BY playlists.id ORDER BY video_count DESC) t1
	LEFT JOIN (
		SELECT channels.channel_name AS channel_name,COUNT(*) AS downloaded_count
		FROM videos
		INNER JOIN playlists ON videos.playlist=playlists.id
		INNER JOIN channels ON playlists.channel_id=channels.id
		WHERE videos.downloaded IS NOT NULL
		GROUP BY playlists.id ORDER BY downloaded_count DESC) t2
	ON t1.channel_name = t2.channel_name
ORDER BY video_count DESC
```
```SQL
CREATE OR REPLACE
ALGORITHM=UNDEFINED 
VIEW downloaded_and_available_videos_by_channel_with_percent AS
select downloaded_and_available_videos_by_channel.channel_name AS channel_name,downloaded_and_available_videos_by_channel.video_count AS video_count,downloaded_and_available_videos_by_channel.downloaded_count AS downloaded_count,round(downloaded_and_available_videos_by_channel.downloaded_count * 100.0 / downloaded_and_available_videos_by_channel.video_count,1) AS Percent
FROM downloaded_and_available_videos_by_channel
```

### Import the dashboard json files from [the grafana dashboards folder](https://github.com/w0d4/yt-backup/tree/master/grafana-dashboards) into your grafana installation
- https://grafana.com/docs/grafana/latest/reference/export_import/#importing-a-dashboard
- Correct all the links on the overview dashboard to match your dashboard IDs


## Problems
### I get strange error messages during run or get_video_infos regarding encoding errors
Make sure your database, tables and columns are created with utf8mb4 encoding support.

Execute the following statements against your database in case you are using MariaDB/MySQL. Make sure the only output is utf8mb4
```SQL
SELECT default_character_set_name FROM information_schema.SCHEMATA 
WHERE schema_name = "yt-backup";
```
```SQL
SELECT CCSA.character_set_name FROM information_schema.`TABLES` T,
       information_schema.`COLLATION_CHARACTER_SET_APPLICABILITY` CCSA
WHERE CCSA.collation_name = T.table_collation
  AND T.table_schema = "yt-backup"
  AND T.table_name = "videos";
```
```SQL
SELECT character_set_name FROM information_schema.`COLUMNS` 
WHERE table_schema = "yt-backup"
  AND table_name = "videos"
  AND column_name = "description";
```

## License
Copyright (C) 2020  w0d4
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

## Questions
### What happens if a video is already marked as downloaded in youtube-dl archive, but not in database and download is started
yt-backup will find the video id in youtube-dl download archive and set it's download date to 1972-01-01 23:23:23 in database. Since we don't know when the video was downloaded originally, we have marked it as downloaded anyways.
