import logging
import sys
class clog:
    info_logger = logging.getLogger("progress")
    #error_logger = logging.getLogger("failuin")
    #pro_logger= logging.getLogger("pro")

    gcnt = 0

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
        #clog.error_logger.addHandler(error_fh)
        #clog.pro_logger.addHandler(pro_fh)

    @staticmethod
    def info(msg):
        clog.info_logger.error(msg)

    @staticmethod
    def error(msg):
        clog.error_logger.error(msg)

    @staticmethod
    def clock(msg):
        clog.pro_logger.error(msg)

if __name__  == "__main__":
    clog.info("hi!")
