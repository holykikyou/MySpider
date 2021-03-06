"""
直接运行，方便快捷，输入百科URL，即可按词条关联深度或词条数量获取领域相关百科数据，代码可以根据个人情况进行修改
"""
import collections

class UrlManager(object):

    """

    考虑建立词表，
    分层次保存数据
    """

    def __init__(self,maxdepth):
        self.lemma=[]   #词条
        self.depth=0 #层次遍历深度
        # self.new_urls = set()
        # self.old_urls = set()#使用集合是为了不进入重复的url，其实可以写个检查逻辑
        self.new_urls=collections.deque()
        self.old_urls=collections.deque()
        self.maxdepth=maxdepth
        self.title=[]  #标题

    def check_depth(self):
        if(self.depth>self.maxdepth):
            return True
        else:
            return False



    def add_new_url(self, url):
        if url is None:
            return
        if url not in self.new_urls and url not in self.old_urls:
            self.new_urls.append(url)

    def add_new_urls(self, urls):
        #self.depth+=1#不是在这里加深度
        if urls is None or len(urls) == 0 or self.check_depth():
            return
        for url in urls:
            self.add_new_url(url)

    def has_new_url(self):
        return len(self.new_urls) != 0

    def get_new_url(self):
        new_url = self.new_urls.popleft()
        self.old_urls.append(new_url)
        return new_url


from bs4 import BeautifulSoup
# import urlparse
# from urllib import parse
from urllib.parse import urljoin
import re




class HtmlParser(object):

    def _get_new_urls(self, page_url, soup):
        new_urls = set()
        # /view/123.html
        # /item/Python/407313
        # links = soup.find_all('a', href=re.compile(r"/view/\d+\.htm"))
        links = soup.find_all('a', href=re.compile(r"/item/*"))   #界面解析发现更多链接

        for link in links:
            new_url = link['href']
            new_full_url = urljoin(page_url, new_url)   #拼接url，加入url池
            new_urls.add(new_full_url)
        return new_urls



    def _get_new_data(self, page_url, soup):
        res_data = {}

        res_data['url'] = page_url
# <dd class="lemmaWgt-lemmaTitle-title"> <h1>Python</h1>

        title_node = soup.find('dd', class_="lemmaWgt-lemmaTitle-title").find("h1")   #获取词条名称
        res_data['title'] = title_node.get_text()

        """
        保存父节点
        """
        print (res_data['title'])
        # class = "lemma-summary" label-module="lemmaSummary" >   #摘要
        # summary_node = soup.find('div', classs_="lemma-summary")
        # res_data['summary'] = summary_node.get_text()

        # ff = soup.select('div[class="para-title level-2"] > h2')
        # print(ff)

        catlog_node = soup.select('div[class="para"]')
        # for content in catlog_node:
            # print(content.get_text().strip())

        return res_data


    def parse(self, page_url, html_cont):
        if page_url is None or html_cont is None:
            print ('page_url is None')
            return

        soup = BeautifulSoup(html_cont, 'html.parser', from_encoding='utf-8')
        new_urls = self._get_new_urls(page_url, soup)
        new_data = self._get_new_data(page_url, soup)
        return new_urls, new_data


import urllib.request
from urllib import error
import ssl


#ssl._create_default_https_context = ssl._create_unverified_context

class HtmlDownloader(object):

    def download(self, url):
        if url is None:
            print ('url is None')
            return  None
        try:
            response = urllib.request.urlopen(url, timeout=10)
            if response.getcode() != 200:
                print ('false')
                return None
            print ('success')
        except error.URLError as e:
            print (e.reason)
            return None
        # print response.read()
        return response.read()

class HtmlOutputer(object):  #输出html

    def __init__(self):
        self.datas = []

    def collect_data(self, data):
        if data is None:
            return
        self.datas.append(data)

    def output_html(self):
        fout = open('webdata/output.html', 'w')

        fout.write("<html>")
        fout.write("<body>")
        fout.write("<table>")

        for data in self.datas:
            fout.write("<tr>")
            fout.write("<td>%s</td>" % data['url'])
            fout.write("<td>%s</td>" % data['title'].encode('utf-8'))
            # fout.write("<td>%s</td>" % data['summary'].encode('utf-8'))
            fout.write("</tr>")

        fout.write("</table>")
        fout.write("</body>")
        fout.write("</html>")

class SpiderMain(object):
    def __init__(self,maxdepth=3):
        self.maxdepth = maxdepth
        self.urls = UrlManager(self.maxdepth)
        self.downloader = HtmlDownloader()
        self.parser = HtmlParser()
        self.outputer = HtmlOutputer()



    def craw(self, root_url):

        self.urls.add_new_url(root_url)
        while self.urls.has_new_url():
            count=0#深度优先遍历可能会引入无关的内容
            nums_of_urls = len(self.urls.new_urls)  # 这一轮的遍历数量
            for i in range(nums_of_urls):
                try:
                    new_url = self.urls.get_new_url()  # 广度优先遍历
                    count = count + 1
                    print('craw %d/%d : %s' % (count, nums_of_urls, new_url))
                    html_cont = self.downloader.download(new_url)
                    """
                    在下面或许可以增加实体分类的逻辑
                    """

                    new_urls, new_data = self.parser.parse(new_url, html_cont)
                    self.urls.title.append(new_data['title'])  # 加入title
                    self.urls.add_new_urls(new_urls)  #
                    print("depth:", self.urls.depth)
                    self.outputer.collect_data(new_data)
                except:
                    print('craw failed')

                # new_url = self.urls.get_new_url()




                # new_url = self.urls.get_new_url()
                # count = count + 1
                # print('craw %d : %s' % (count, new_url))
                # html_cont = self.downloader.download(new_url)
                # new_urls, new_data = self.parser.parse(new_url, html_cont)   #这是一般的无规则遍历
                # self.urls.add_new_urls(new_urls)
                # self.outputer.collect_data(new_data)
            self.urls.depth += 1

                # if count == 10:
                #     print ('done')
                #     break



        self.outputer.output_html()

if __name__=="__main__":
    # root_url = "https://baike.baidu.com/view/10812319.htm"
    root_url = "https://baike.baidu.com/item/%E9%9B%86%E6%88%90%E7%94%B5%E8%B7%AF"#集成电路
    #root_url = "https://baike.baidu.com/item/python/407313"
    #root_url='https://baike.baidu.com/item/%E9%87%91%E8%9E%8D/860' #金融
    #root_url='https://baike.baidu.com/item/%E4%B8%AD%E8%8A%AF%E5%9B%BD%E9%99%85'

    obj_spider = SpiderMain(maxdepth=2)
    obj_spider.craw(root_url)
