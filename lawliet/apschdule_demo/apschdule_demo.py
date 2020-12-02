# @Time    : 2020/12/2 8:11 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : apschdule_demo.py
# @Software: PyCharm
# @Description:


from datetime import datetime
import os
from apscheduler.schedulers.blocking import BlockingScheduler

def tick():
    print('Tick! The time is: %s' % datetime.now())



def interval():
    scheduler = BlockingScheduler()
    scheduler.add_job(tick, 'interval', seconds=3)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C    '))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass




def job_corn():
    scheduler = BlockingScheduler()
    scheduler.add_job(tick, 'cron', hour=20,minute=18)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C    '))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    job_corn()
