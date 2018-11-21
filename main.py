import sys
import time
from spider  import run_spider
from multiprocessing import Pool


def main():
    """
    进程池长度为3, 每次运行三个窗口进行爬取数据
    :return:
    """
    list_params = sys.argv
    pool = Pool(3)
    for i in range(int(list_params[1]),int(list_params[2]),2):
        pool.apply_async(run_spider, args=(i, i+1))
        time.sleep(7)
    pool.close()
    pool.join()
if __name__ == '__main__':
    main()


