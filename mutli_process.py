"""
author:holykikyou
data:0624

应该是不可维护的多线程爬虫代码
"""


class mySpider:

    def __init__(self):
        pass

    def extract(self,detail_url_list):
        return len(detail_url_list) #测试
        pass


class Mydata:
    """多线程爬虫实例化一个类来汇总所有数据"""

    def __init__(self):
        self.all_data = []

    def add_data(self, data):
        self.all_data.extend(data)


def myfunction(s: mySpider, data_adder:mySpider, sub2_detail_url_list):
    """先获取所有详情页URL才做多线程爬取
    输入：一小部分URL
    """

    data = s.extract(sub2_detail_url_list)
    data_adder.add_data(data)


from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def mythread(s: mySpider, data_adder: Mydata, function=myfunction, **param: tuple):
    """
    自定义任务函数为参数

    """
    batch = 10
    index, process_num, detail_url_list = param[0], param[1], param[2]

    """计算这一进程能分配到的URL数"""
    subtask = detail_url_list // process_num + 1
    assert 0 <= index < process_num
    if index == process_num - 1:
        sub_detail_url_list = detail_url_list[index * subtask:]
    else:
        sub_detail_url_list = detail_url_list[index * subtask:(index + 1) * subtask]
    """根据上面的batch和URL数计算myThread要分配的线程数"""
    n = sub_detail_url_list // batch + 1
    thread_worker = ThreadPoolExecutor(max_workers=n)
    """开始向每个线程分配URL"""
    for i in range(n):
        start_num = i * batch
        if ((i + 1) * batch < len(sub_detail_url_list)):
            end_num = (i + 1) * batch

        else:
            end_num = len(sub_detail_url_list)
        thread_worker.submit(myfunction, s, data_adder, sub_detail_url_list[start_num:end_num])

"""实例化我的爬虫类"""
mydata = Mydata()
s=mySpider()

def myprocess(s:mySpider, data_adder: Mydata,all_detail_url_list:list, process_num=2):
    """
    data_adder是多线程数据汇总类的实例
    设定每个个进程数量，传入URL列表，每个线程执行10个URL，自动分配每个进程执行线程数量
    """

    pool = ProcessPoolExecutor(max_workers=process_num)
    for i in range(process_num):
        pool.submit(mythread, (s, data_adder, i, process_num, all_detail_url_list))  # detail_url_list 第i批次加入线程中
    return data_adder.all_data
import random
def test():
    a=list(range(100))
    data_all=myprocess(s,mydata,a,process_num=2)
    print(data_all)




test()
