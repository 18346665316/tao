import pymysql
from utils.bloom_filter import bloom


class Mysql_Store():
    """
    将dict对象存储到mysql中
    """
    def __init__(self):
        self.bloom = bloom
        self.conn = pymysql.Connect(host='127.0.0.1', port=3306, database='taobao_spider', user='root',
                                    password='mysql', charset='utf8')
        self.cursor = self.conn.cursor()
        create_skulist_table_sql = "CREATE TABLE SKULIST (id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, skuid VARCHAR(80)  NOT NULL UNIQUE,spuid VARCHAR(80) NOT NULL ,size VARCHAR(40),style VARCHAR(40) ,price VARCHAR(15),stock int unsigned)"
        create_spulist_table_sql = "CREATE TABLE SPULIST (id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,spuid VARCHAR(80)  NOT NULL UNIQUE,title VARCHAR(250),price VARCHAR(30),discount_price VARCHAR(30))"

        create_sizelist_table_sql = "CREATE TABLE SIZELIST (id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,sizenum VARCHAR(80)  NOT NULL UNIQUE,name VARCHAR(50))"
        create_stylelist_table_sql = "CREATE TABLE STYLELIST (id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,stylenum VARCHAR(80)  NOT NULL UNIQUE,stylename VARCHAR(50),imgurl VARCHAR(100),discount_price VARCHAR(15),isonsale VARCHAR(10))"
        try:
            self.cursor.execute(create_skulist_table_sql)
            self.conn.commit()
        except:
            print('skutable already exists')

        try:
            self.cursor.execute(create_stylelist_table_sql)
            self.conn.commit()
        except:
            print('styletable already exists')

        try:
            self.cursor.execute(create_spulist_table_sql)
            self.conn.commit()
        except:
            print('sputable already exists')

        try:
            self.cursor.execute(create_sizelist_table_sql)
            self.conn.commit()
        except:
            print('sizetable already exists')


    def reConnect(self):
        """
        该函数实现数据库检测重连操作
        :return:
        """
        try:
            self.conn.connection.ping()
        except:
            self.conn = pymysql.Connect(host='127.0.0.1', port=3306, database='taobao_spider', user='root',
                                    password='mysql', charset='utf8')
            self.cursor = self.conn.cursor()

    def store_to_skuList(self, text):
        """
        :param text: text为json转换 的字典
        :return:
        """
        skulist = text["valItemInfo"]['skuList']
        self.reConnect()
        list_sku = list()
        sql = """insert into SKULIST(skuid, spuid,size, style,price,stock) VALUES(%(skuid)s, %(spuid)s, %(size)s, %(style)s,%(price)s, %(stock)s)"""
        for goods_sku in skulist:
            if self.bloom.do_filter(goods_sku['skuId']):
                continue
            value = {'skuid': goods_sku['skuId'], 'spuid': text['rateConfig']['spuId'],
                     'size': goods_sku['pvs'].split(';')[0], 'style': goods_sku['pvs'].split(';')[1],
                     'stock': text["valItemInfo"]['skuMap'][';' + goods_sku['pvs'] + ';']['stock'],
                     'price': text["valItemInfo"]['skuMap'][';' + goods_sku['pvs'] + ';']['price']}
            list_sku.append(value)
        self.cursor.executemany(sql, list_sku)
        self.conn.commit()

    def store_to_spulist(self, text):
        if self.bloom.do_filter(str(text['rateConfig']['spuId'])):
            return
        sql_add_spu = """insert into SPULIST(spuid,price, discount_price,title) VALUES(%(spuid)s, %(price)s, %(discount_price)s, %(title)s)"""
        value = {'spuid': text['rateConfig']['spuId'],
                 'title': text['itemDO']['title'],
                 'price': text['detail']['defaultItemPrice'],
                 'discount_price': text['detail']['defaultdiscountprice'], }
        self.reConnect()
        try:
            self.cursor.execute(sql_add_spu, value)
            self.conn.commit()
            print('存入成功-------spulist')
        except:
            print('存入失败 -------------spulist')


    def store_to_size(self, text):
        sql_add_spu = """insert into SIZELIST(sizenum, name) VALUES(%(sizenum)s, %(name)s)"""
        name_dict = text['valItemInfo']['skuList']
        self.reConnect()
        size_list = list()
        for i in name_dict:
            name_list = i["names"].split(' ')
            num_list = i['pvs'].split(';')
            if self.bloom.do_filter(str(num_list[0])):
                continue
            value = {"sizenum": num_list[0], "name": name_list[0]}
            size_list.append(value)
        self.cursor.executemany(sql_add_spu, size_list)
        self.conn.commit()
    def store_to_style(self, text):
        sql_add_spu = """insert into STYLELIST(stylenum, stylename, imgurl, discount_price, isonsale) VALUES(%(stylenum)s, %(stylename)s, %(imgurl)s, %(discount_price)s, %(isonsale)s)"""
        style_list = list()
        styledict = text['propertyPics']
        namelist = text['valItemInfo']['skuList']
        for dict_demo in namelist:
            stylenum = dict_demo['pvs'].split(';')[-1]
            stylename = dict_demo['names'].split()[-1]
            stylekey = ';' + stylenum + ';'
            if self.bloom.do_filter(str(stylenum)):
                continue
            try:
                value = {"stylenum": stylenum.strip(';'), "stylename": stylename,
                         "imgurl": "https://" + styledict[stylekey][0],
                         "discount_price": styledict[stylekey][-1]['discount_price'],
                         "isonsale": styledict[stylekey][-1]['on_sale']}
            except:
                print('商品数据有误, 略过')
                break
            else:
                style_list.append(value)

        self.cursor.executemany(sql_add_spu, style_list)
        self.conn.commit()

    def run(self, text):
        """
        单线程执行代码
        :param text:
        :return:
        """
        self.store_to_style(text)
        self.store_to_size(text)
        self.store_to_spulist(text)
        self.store_to_skuList(text)
        print('end')




