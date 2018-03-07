import sys


def my_urljoin(first_url, second_url):
    return first_url.rstrip('/') + '/' + second_url.lstrip('./')


# FIXME: 修复只会显示该函数名的问题
def get_func_name():
    return sys._getframe().f_code.co_name
    