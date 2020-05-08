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

from sqlalchemy import Column, String, Integer, ForeignKey, Text, DateTime

from base import Base


class Video(Base):
    __tablename__ = 'videos'
    id = Column(Integer, primary_key=True)
    playlist = Column(Integer, ForeignKey('playlists.id'), nullable=False)
    video_id = Column(String(length=255), nullable=False, unique=True)
    title = Column(Text(length=5000), nullable=False)
    description = Column(Text(length=99999), nullable=False)
    size = Column(String(length=30))
    resolution = Column(String(length=20))
    runtime = Column(String(length=20))
    downloaded = Column(DateTime)
    online = Column(Integer)
    copyright = Column(String(length=3000))
    download_required = Column(Integer)
    upload_date = Column(DateTime)
