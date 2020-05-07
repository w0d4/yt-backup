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

from sqlalchemy import Column, String, Integer, DateTime

from base import Base


class Statistic(Base):
    __tablename__ = 'statistics'
    id = Column(Integer, primary_key=True)
    statistic_date = Column(DateTime, nullable=False)
    statistic_type = Column(String(255), nullable=False)
    statistic_value = Column(String(255), nullable=False)
