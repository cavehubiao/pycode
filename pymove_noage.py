import os
import sys
import ceasythread
import logging
import subprocess

class clog:
    info_logger = logging.getLogger("progress")
    error_logger = logging.getLogger("failuin")
    pro_logger= logging.getLogger("pro")

    gcnt = 0

    @staticmethod
    def init():
        info_fh = logging.FileHandler('./progress.log.' + sys.argv[1].strip('/'))
        error_fh = logging.FileHandler('./failuin.log.' + sys.argv[1].strip('/'))
        pro_fh= logging.FileHandler('./clock.log.' + sys.argv[1].strip('/'))

        log_formate = logging.Formatter('%(asctime)s\t[td=%(thread)d]\t%(message)s', '[%Y%m%d %H:%M:%S]')

        info_fh.setFormatter(log_formate)
        error_fh.setFormatter(log_formate)
        pro_fh.setFormatter(log_formate)

        clog.info_logger.addHandler(info_fh)
        clog.error_logger.addHandler(error_fh)
        clog.pro_logger.addHandler(pro_fh)

    @staticmethod
    def info(msg):
        clog.info_logger.error(msg)

    @staticmethod
    def error(msg):
        clog.error_logger.error(msg)

    @staticmethod
    def clock(msg):
        clog.pro_logger.error(msg)


def run(path):
    files = [map(lambda y : os.path.join(path, x, y) ,os.listdir(os.path.join(path, x))) for x in os.listdir(path)]
    file_list = []
    for x in files:
        file_list += x

    for line in open("data_before_has_done.txt"):
        file_list.remove(line.rstrip('\n'))
    print len(file_list)
    print file_list


    clog.init()
    #file_list = [path]
    tpool = ceasythread.CThread(100, worker, lock_args = (file_list,))
    tpool.run()

def worker(file_list):
    while len(file_list) > 0:
        af = file_list.popone()
        clog.info('%s start' %(af))
        #cppcmd = subprocess.Popen(["./move_data_to_ckv_stdin"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        pam2 = sys.argv[1] + ".uin"
        cppcmd = subprocess.Popen(["./move_data_to_ckv_file_new", af, pam2], stdout=subprocess.PIPE)
        cpp_stdout = cppcmd.communicate()[0]
        cpp_ret = cppcmd.returncode
        if cpp_ret != 0:
            clog.info('%s %d' % (af, cpp_ret))
        else:
            clog.info('%s done' %(af))

if "__main__" == __name__:
    run(sys.argv[1])
