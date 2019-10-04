
import datetime

def write_log(log):
    time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('{}: {}'.format(time_stamp, log))

