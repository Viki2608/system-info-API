import platform,socket,re,uuid,json,psutil,logging,wmi,os,time
from datetime import datetime
def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return "%d:%02d:%02d" % (hour, minutes, seconds)

def getSystemInfo():
    try:
        info={}
        info['platform']=platform.system()
        if platform.system()=='Windows':
            c = wmi.WMI()
            my_system = c.Win32_ComputerSystem()[0]
            info['Manufacturer']= my_system.Manufacturer
            info['Model']= my_system. Model
            info['Number Of Processors']= my_system.NumberOfProcessors
            info['SystemType']=my_system.SystemType
            info['SystemFamily']= my_system.SystemFamily
        info['architecture']=platform.machine()
        info['hostname']=socket.gethostname()
        info['ip-address']=socket.gethostbyname(socket.gethostname())
        info['mac-address']=':'.join(re.findall('..', '%012x' % uuid.getnode()))
        info['processor']=platform.processor()
        info['cpu cores']=os.cpu_count()
        info['cpu usage']=psutil.cpu_percent()
        hdd = psutil.disk_usage('/')
        info['Total disk space'] =  hdd.total // (2**30)
        info['Used disk space'] =  hdd.used // (2**30)
        info['Available disk space'] =  hdd.free // (2**30)
        info['system up time']= convert(time.time() - psutil.boot_time())
        info['Total ram']=int(round(psutil.virtual_memory().total / (1024.0 **3)))
        info['Available ram']=int(round(psutil.virtual_memory().available/(1024.0 **3)))
        for idx, usage in enumerate(psutil.cpu_percent(percpu=True)):
            info[f'CORE_{idx+1}'] = int(float(usage))
        return json.dumps(info)
    except Exception as e:
        logging.exception(e)
A=(convert(time.time() - psutil.boot_time()))
print(A)
print(type(A))
datetime_object = datetime.strptime(A, '%H:%M:%S').time()
print(datetime_object)
print(type(datetime_object))