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

from sqlalchemy import Column, String, Integer, Text, DateTime

from base import Base


class Operation(Base):
    __tablename__ = 'operations'
    id = Column(Integer, primary_key=True)
    operation_date = Column(DateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    operation_type = Column(String(255), nullable=False)
    operation_description = Column(Text(3000))
