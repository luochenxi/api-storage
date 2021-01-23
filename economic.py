import logging
from tornado import ioloop

from bin import economic_index
from bin import yf
from bin import us_etf
from lib import utils

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('main.log', encoding="utf-8")
ch = logging.StreamHandler()
logger.level = logging.DEBUG
logger.addHandler(fh)
logger.addHandler(ch)

@utils.extract_context_info
def main():
    economic_index.wei()
    economic_index.gold()
    economic_index.fftr()
    economic_index.INFLATION_USA()
    economic_index.effr()
    economic_index.market_some_hold()
    economic_index.USTREASURY_REALYIELD()
    ioloop.IOLoop.current().run_sync(us_etf.main)
    # 期货和大宗商品 不全
    yf.get()
    yf.oli_copper_gold()


if __name__ == '__main__':
    main()