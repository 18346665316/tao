from store_to_mysql import Mysql_Store
import json
import sys
import time
list_params = sys.argv
mysql_store = Mysql_Store()
start_time = time.clock()

def main():
    for i in range(int(list_params[1]),int(list_params[2]),2):
        try:
            f = open(r'data/%s-%s页.txt'%(i, i+1), 'r')
        except Exception as e:
            print(e)
        else:
            for content in f.readlines():
                mysql_store.run(json.loads(content, encoding='utf-8'))
                print('线程结束')
            f.close()

    print('用时: ', time.clock() - start_time)
if __name__ == '__main__':
    main()