import os
import time
from retrying import retry
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from retrying import retry
# disable https ssl safe warning
from urllib3.exceptions import InsecureRequestWarning
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options

from selenium.common import exceptions as sel_exec

import config as cf

def header():
    return {'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'en-US,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
            }

def config():
    config = {
        "header": header(),
    }
    return config

@retry(stop_max_attempt_number=3, wait_fixed=60*3)
def createInstance(module_name, class_name, window_width=960, window_high=600, *args, **kwargs):
    '''
    create user input browser object ??????????????
    :return: object
    '''
    # set browser header
    for key, value in header().items():
        webdriver.DesiredCapabilities.PHANTOMJS['{}.page.customHeaders.{}'.format(class_name.lower(),key)] = value
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    # try:
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    # except AttributeError as e:
    #     raise AttributeError('not is %s' % module_name)
    obj.set_window_size(window_width, window_high)
    return obj

# def createInstance(module_name, class_name, *args, **kwargs):
#     '''
#     create user input browser object ??????????????
#     :return: object
#     '''
#
#     # set browser header
#     for key, value in header().items():
#         webdriver.DesiredCapabilities.CHROME['chrome.page.customHeaders.{}'.format(key)] = value
#     module_meta = __import__(module_name, globals(), locals(), [class_name])
#     # try:
#     class_meta = getattr(module_meta, class_name)
#     obj = class_meta(*args, **kwargs)
#     # except AttributeError as e:
#     #     raise AttributeError('not is %s' % module_name)
#     return obj

# def browser(browser_name="chrome", window_width=960, window_high=600, implicit_time=10):
#     dcap_lib = getattr(DesiredCapabilities, browser_name.upper())
#     if not dcap_lib:capabilities
#         raise
#     dcap = dict(dcap_lib)
#     dcap["phantomjs.page.settings.userAgent"] = (header()["User-Agent"])  # ?? userAgent
#
#     driver = createInstance("selenium.webdriver", browser_name.capitalize(),
#                             desired_capabilities=dcap,
#                             service_args=['--ignore-ssl-errors=true', '--ssl-protocol=TLSv1'])
#     # driver.implicitly_wait(30.00)
#     driver.set_window_size(window_width, window_high)  # set window size
#     # driver.maximize_window()  # set maximize_window
#     # driver.set_page_load_timeout(self.__page_load_timeout)  # set time out
#     return driver

def browser(browser_name=cf.BROWSER_NAME, implicit_time=10):
    dcap_lib = getattr(DesiredCapabilities, browser_name.upper())
    if not dcap_lib:
        raise
    dcap = dict(dcap_lib)
    dcap["chrome.page.settings.userAgent"] = (header()["User-Agent"])  # ?? userAgent
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    desired_capabilities=DesiredCapabilities.CHROME
    # driver = createInstance("selenium.webdriver", browser_name.capitalize(),
    kwg = {
        "command_executor": cf.WEBDRIVER_URL,
        "options":options,
    }
    kwg["desired_capabilities"] = desired_capabilities
    try:
        driver = createInstance("selenium.webdriver", "Remote", **kwg)
    except sel_exec.WebDriverException:
        driver = createInstance("selenium.webdriver", "Remote", **kwg)
    # driver.implicitly_wait(30.00)
      # set window size
    # driver.maximize_window()  # set maximize_window
    # driver.set_page_load_timeout(self.__page_load_timeout)  # set time out
    return driver

def get_element(driver, ele, by=By.XPATH, time_out=30):
    locator = (by, ele)
    wait = WebDriverWait(driver, time_out)
    # visibility_of_any_elements_located visibility_of_element_located
    try:
        ele = wait.until(EC.visibility_of_element_located(locator))
        ele = wait.until(EC.presence_of_element_located(locator))
        # ele_val = wait.until(EC.text_to_be_present_in_element_value(locator,ele))
        # print(ele, ele.text)
        # return driver.find_element_by_xpath(ele)
    except sel_exec.TimeoutException as e:
        screenshots(driver)
        raise e
    return ele

@retry(stop_max_attempt_number=3, wait_fixed=1000)
def get_url(br, index):
    br.get(index.url)
    return br

def execute_script(br, js):
    br.execute_script(js)
    return br

def new_tab(br, index):
    js = 'window.open("{}")'.format(index.url)
    execute_script(br,js)

def current_window(br):
    return br.current_window_handle

def screenshots(br, path=os.getcwd()):
    br.save_screenshot("{}/{}.png".format(path,time.strftime("%Y-%m-%d-%X", time.localtime())))

def handles(br):
    return br.window_handles

def switch_window(br, index):
    br.switch_to_window(index.handle)