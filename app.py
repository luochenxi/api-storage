import logging
import sys
from tornado import ioloop
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
from bin import async_breadth

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('main.log', encoding="utf-8")
ch = logging.StreamHandler()
logger.level = logging.DEBUG
logger.addHandler(fh)
logger.addHandler(ch)




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
        logger.info("not trad. exit. today: {} new: {}".format(utils.today().isoformat(), utils.now().isoformat()))
        return
    run()

@utils.extract_context_info
@retry(stop_max_attempt_number=20, wait_fixed=3)
def run():
    while True:
        try:
            # market breadth | 市场宽度
            ioloop.IOLoop.current().run_sync(async_breadth.main)

            # # # 经济数据
            # newyorkfed.bin()
            # longtermtrends.bin()
            # cpi.bin()
            # treasury.bin()
            # federal_founds_rate.bin()
            # jobless_claims.bin()
            # gld.bin()
            #
            # # etf
            # us_etf.bin()
            # us_etf_ark.bin()
            return
        except Exception as e :
            logger.error(e,exc_info=1)
            raise e

if __name__ == '__main__':
    main()
