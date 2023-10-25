from datetime import datetime, timezone
from contextlib import contextmanager
from http import HTTPStatus

from sqlalchemy.orm import sessionmaker

from swagger_server.models.systeminfo_model import *


@contextmanager
def session_scope(engine):
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session

    except:
        session.rollback()
        raise
    finally:
        session.commit()
        session.close()


def persisting_system_info(requests_data: dict, engine) -> None:

    print('Inserting to system_info in the database')
    try:
        with session_scope(engine) as session:            
            ip_address = requests_data.get('ip-address')
            mac_address = requests_data.get('mac-address')
            os = requests_data.get('platform')
            Manufacturer = requests_data.get('Manufacturer')
            hostname = requests_data.get('hostname')
            no_of_processors = requests_data.get('Number Of Processors')
            systemtype = requests_data.get('architecture')
            cpu_usage = requests_data.get('cpu usage')
            total_diskspace	=  requests_data.get('Total disk space')
            used_diskspace = requests_data.get('Used disk space')
            available_diskspace = requests_data.get('Available disk space')
            total_ram = requests_data.get('Total ram')
            available_ram = requests_data.get('Available ram')
            system_model = requests_data.get('Model')
            system_up_time = requests_data.get('system up time')
            new_data = SystemInfo(ip_address=ip_address,mac_address=mac_address, os=os,
                    Manufacturer=Manufacturer,hostname=hostname,no_of_processors=no_of_processors,
                    systemtype=systemtype,cpu_usage=cpu_usage,total_diskspace=total_diskspace,used_diskspace=used_diskspace,
                    available_diskspace=available_diskspace,total_ram=total_ram,available_ram=available_ram,system_model=system_model,
                    system_up_time=system_up_time)
            print(new_data)
            session.add(new_data)
            return HTTPStatus.OK
    except Exception as ex:
        message = 'Error while updating System_info_table in the database: {}'.format(str(ex))
        print(message)
        return HTTPStatus.INTERNAL_SERVER_ERROR
