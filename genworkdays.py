import os
import sys
import ceasythread
import logging
import subprocess
import datetime

def datelist(start, end):
    start_date = datetime.date(*start)
    end_date = datetime.date(*end)

    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    return result


class clog:
    info_logger = logging.getLogger("progress")
    #error_logger = logging.getLogger("failuin")
    #pro_logger= logging.getLogger("pro")


    @staticmethod
    def init():
        info_fh = logging.FileHandler('./progress.log.' + sys.argv[1].strip('/'))
        #error_fh = logging.FileHandler('./failuin.log.' + sys.argv[1].strip('/'))
        #pro_fh= logging.FileHandler('./clock.log.' + sys.argv[1].strip('/'))

        log_formate = logging.Formatter('%(asctime)s\t[td=%(thread)d]\t%(message)s', '[%Y%m%d %H:%M:%S]')

        info_fh.setFormatter(log_formate)
        #error_fh.setFormatter(log_formate)
        #pro_fh.setFormatter(log_formate)

        clog.info_logger.addHandler(info_fh)

    @staticmethod
    def info(msg):
        clog.info_logger.error(msg)

class gstaff:
    agg_dir = "workday"
    holiday = set()
    data99files = dict()

    @staticmethod
    def init():
        all_date = datelist((2016,1,1), (2016,12,31))
        gstaff.holiday = gstaff.holiday | set([20160101, 20160207, 20160208, 20160209,
                20160210, 20160211, 20160212, 20160213,
                20160404, 20160501, 20160609, 20160610,
                20160611, 20160915, 20160916, 20160917,
                20161001, 20161002, 20161003, 20161004,
                20161005, 20161006, 20161007])
        for x in all_date:
            w = datetime.datetime(int(x[0:4]), int(x[4:6]), int(x[6:])).strftime('%w')
            if w == '0' or w == '6':
                gstaff.holiday.add(int(x))
        #print gstaff.holiday
        data99 = os.listdir("data99")
        for x in data99:
            gstaff.data99files[x] = os.path.join("data99", x)
            print x, data99files[x]

def run(path):
    data_list = ["data" + str(i) for i in range(10)]
    for x in data_list:
        apath = os.path.join(gstaff.agg_dir, x)
        if os.path.exists(apath) == False:
            os.makedirs(apath)

    #files = [map(lambda y : os.path.join(path, x, y) ,os.listdir(os.path.join(path, x))) for x in os.listdir(path)]
    files = [map(lambda y : os.path.join(x, y), os.listdir(x)) for x in data_list]
    file_list = []
    for x in files:
        file_list += x

    clog.init()
    gstaff.init()
    
    for x in file_list:
        print os.path.split(x)[1]
    #file_list = [path]
    #tpool = ceasythread.CThread(1, worker, lock_args = (file_list,))
    #tpool.run()

def worker(file_list):
    while len(file_list) > 0:
    	table = {}
        af = file_list.popone()
        clog.info('%s start' %(af))
        wfd = open(os.path.join(gstaff.agg_dir, af), 'w')
        for line in open(af, 'r'):
            li = line.rstrip('\n').split('\t')
            if li[0] == "fuin":
                continue
            uin, day, step = int(li[0]), int(li[1]), int(li[2])
            if step == 0 or day < 20160101:
                continue
            if day in gstaff.holiday:
                if table.has_key(uin):
                    table[uin][2] += 1
                    table[uin][3] += step
                else:
                    table[uin] = [0, 0, 1, step]
            else:
                if table.has_key(uin):
                    table[uin][0] += 1
                    table[uin][1] += step
                else:
                    table[uin] = [1, step, 0, 0]
        fname = os.path.split(af)[1]
        if gstaff.data99files.has_key(fname):
            for line in open(gstaff.data99files[fname], 'r'):
                li = line.rstrip('\n').split('\t')
                if li[0] == "fuin":
                    continue
                uin, day, step = int(li[0]), int(li[1]), int(li[2])
                if step == 0 or day < 20160101:
                    continue
                if day in gstaff.holiday:
                    if table.has_key(uin):
                        table[uin][2] += 1
                        table[uin][3] += step
                    else:
                        table[uin] = [0, 0, 1, step]
                else:
                    if table.has_key(uin):
                        table[uin][0] += 1
                        table[uin][1] += step
                    else:
                        table[uin] = [1, step, 0, 0]



        for k in table:
            table[k].insert(0, k)
            wli = map(str, table[k])
            wfd.write("\t".join(wli) + "\n")
        wfd.close()
        clog.info("%s done"%(af))

if "__main__" == __name__:
    run(sys.argv[1])
