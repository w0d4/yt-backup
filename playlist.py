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

from sqlalchemy import Column, String, Integer, ForeignKey

from base import Base


class Playlist(Base):
    __tablename__ = 'playlists'
    id = Column(Integer, primary_key=True)
    playlist_id = Column(String(255), nullable=False, unique=True)
    playlist_name = Column(String(255), nullable=False)
    entries = Column(Integer)
    monitored = Column(Integer, nullable=False)
    channel_id = Column(Integer, ForeignKey('channels.id'), nullable=False)