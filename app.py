import logging
import sys
import traceback
from collections import namedtuple

import config as cf
from lib import utils
from lib.browser import *
from bin import newyorkfed
from bin import longtermtrends
from bin import cpi
from bin import treasury
from bin import breadth
from bin import federal_founds_rate
from bin import us_etf
from bin import jobless_claims
from bin import us_etf_ark
from bin import gld

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('main.log', encoding="utf-8")
ch = logging.StreamHandler()
logger.level = logging.DEBUG
logger.addHandler(fh)
logger.addHandler(ch)


@utils.extract_context_info
def market_breadth(br=None, debug=False):
    path = os.path.join(cf.DATA_US_DIR,'sp500_all.json')
    all = utils.read_json_file(path)
    if not debug:
        breadth.start(br)
        breadth.loading_page(br)
        current = breadth.save_last(br)
        all[current['time']] = current['data']
    utils.save_to_json(path, all)
    breadth.split_total(all, cf.OUTPUT)
    # 保存所有数据
    output = os.path.join(cf.OUTPUT, 'sp500_all.json')
    utils.save_to_json(output, all)
    # utils.day_trading_save(all)
    utils.split_save_json(all, cf.OUTPUT, 'sp500_{}.json')

@utils.extract_context_info
def main():
    logger.info(sys.version)
    logger.info("start... today: {} new: {}".format(utils.today().isoformat(), utils.now().isoformat()))
    utils.init_sentry()
    if utils.is_market_open():
        logger.info("not is_market_open. exit. today: {} new: {}".format(utils.today().isoformat(), utils.now().isoformat()))
        return
    logger.info("is_market_open. run.. today: {} new: {}".format(utils.today().isoformat(), utils.now().isoformat()))

    if not utils.check_tradecal():
        return
    run()

@utils.extract_context_info
@retry(stop_max_attempt_number=20, wait_fixed=3)
def run():
    while True:
        try:
            # market breadth | 市场宽度
            br = browser()
            market_breadth(br)

            # # 经济数据
            newyorkfed.bin()
            longtermtrends.bin()
            cpi.bin()
            treasury.bin()
            federal_founds_rate.bin()
            jobless_claims.bin()
            gld.bin()

            # etf
            us_etf.bin()
            us_etf_ark.bin()

            breadth.quit_browser(br)
            return 
        except Exception as e :
            logger.error(e,exc_info=1)
            raise e

if __name__ == '__main__':
    main()
