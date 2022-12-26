from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from flask import current_app as app
Base = declarative_base()

from sqlalchemy.dialects.postgresql import (CIDR,MACADDR,FLOAT)
class SystemInfo(Base):
    __tablename__ = 'System_info_table'
    # id = Column(Integer, primary_key=True)
    ip_address = Column(CIDR , primary_key=True)
    mac_address = Column(MACADDR, nullable=False)
    os = Column(String, nullable=False)
    Manufacturer = Column(String, nullable=False)
    hostname = Column(String, nullable=False)
    no_of_processors = Column(Integer, nullable=False)
    systemtype = Column(String,nullable=False)
    cpu_usage = Column(FLOAT, nullable=False)
    total_diskspace	=  Column(Integer, nullable=False)
    used_diskspace = Column(Integer, nullable=False)
    available_diskspace = Column(Integer, nullable=False)
    total_ram = Column(Integer, nullable=False)
    available_ram = Column(Integer, nullable=False)
    system_model = Column(String, nullable=False)
    system_up_time = Column(String, nullable=False)