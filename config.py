import os

from pytz import timezone, country_timezones

BASE_DIR = os.path.dirname((os.path.abspath(__file__)))
BASE_URL = "https://cn.tradingview.com/symbols/"
TOKEN = os.getenv("TOKEN", "c67ed59a3d67f720f9b9b1e89214348a9e0210a9dd8887fc46885cd9")
# WEBDRIVER_URL = os.getenv("WEBDRIVER_URL", "http://10.0.0.9:9099/wd/hub")
WEBDRIVER_URL = os.getenv("WEBDRIVER_URL", "http://127.0.0.1:4444/wd/hub")
DEBUG = bool(os.getenv("DEBUG", True))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_US_DIR = os.path.join(BASE_DIR, 'data', 'us')
DATA_US_ETF_DIR = os.path.join(BASE_DIR, 'data', 'us', 'etf')
ALL_ETF_DIR = os.path.join(BASE_DIR, 'data', 'us', 'etf', "ALL.json")
BOARD_LEFT = os.path.join(BASE_DIR, 'data', "BOARD_LEFT.json")
OUTPUT = os.path.join(BASE_DIR, os.getenv("OUTPUT", 'output'))
US_ETF_OUTPUT = os.path.join(BASE_DIR, os.getenv("OUTPUT", 'output'), 'etf')
ALL_ETF_OUTPUT = os.path.join(US_ETF_OUTPUT, "ALL.json")
NEWYORKFED_SOMA_SRC_DATA = os.path.join(DATA_US_DIR, 'newyorkfed_some_hold.json')
NEWYORKFED_WEI_SRC_DATA = os.path.join(DATA_US_DIR, 'newyorkfed_wei.json')
OLI_GOLD_RATIO_SRC_DATA = os.path.join(DATA_US_DIR, 'oil_gold_ratio.json')
OLI_SRC_DATA = os.path.join(DATA_US_DIR, 'oli.json')
GOLD_SRC_DATA = os.path.join(DATA_US_DIR, 'gold.json')
COPPER_GOLD_RATIO_SRC_DATA = os.path.join(DATA_US_DIR, 'copper_gold_ratio.json')
INTEREST_RATES_SRC_DATA = os.path.join(DATA_US_DIR, 'interest_rates.json')
COPPER_SRC_DATA = os.path.join(DATA_US_DIR, 'copper.json')
GOLD_SINCE_SRC_DATA = os.path.join(DATA_US_DIR, 'gold_since.json')
CPI_SRC_DATA = os.path.join(DATA_US_DIR, 'cpi.json')
TREASURY_REAL_RATES_SRC_DATA = os.path.join(DATA_US_DIR, 'treasury_real_rates.json')
FEDERAL_FUNDS_RATE_SRC_DATA = os.path.join(DATA_US_DIR, 'federal_founds_rate.json')
US_INITIAL_JOBLESS_SRC_DATA = os.path.join(DATA_US_DIR, 'initial_jobless.json')
US_CONTINUING_JOBLESS_SRC_DATA = os.path.join(DATA_US_DIR, 'continuing_jobless.json')
US_GLD_SRC_DATA = os.path.join(DATA_US_DIR, 'gld.csv')

NEWYORKFED_SOMA_HOLD_URL = "https://markets.newyorkfed.org/read?productCode=30&startDt={}&endDt={}&query=summary&format=json"
NEWYORKFED_WEI_URL = "https://www.newyorkfed.org/medialibrary/research/interactives/data//wei_data.csv"
OLI_GOLD_RATIO_URL = "https://www.longtermtrends.net/data-oil-gold-ratio/"
OLI_URL = "https://www.longtermtrends.net/data-gold2/"
GOLD_URL = "https://www.longtermtrends.net/data-oil/"
COPPER_GOLD_RATIO_URL = "https://www.longtermtrends.net/data-copper-gold-ratio/"
INTEREST_RATES_URL = "https://www.longtermtrends.net/data-interest-rates/"
COPPER_URL = "https://www.longtermtrends.net/data-copper/"
GOLD_SINCE_URL = "https://www.longtermtrends.net/data-gold-since-1850/"
CPI_URL = "https://ycharts.com/charts/fund_data.json?annotations=&annualizedReturns=false&calcs=&chartType=interactive" \
          "&chartView=&correlations=&dateSelection=range&displayDateRange=false&displayTicker=false&endDate=&format=rea" \
          "l&legendOnChart=false&note=&partner=basic_2000&quoteLegend=false&recessions=false&scaleType=linear&securiti" \
          "es=id%3AI%3AUSCPIYY%2Cinclude%3Atrue%2C%2C&securityGroup=&securitylistName=&securitylistSecurityId=&sour" \
          "ce=false&splitType=single&startDate=&title=&units=false&useCustomColors=false&useEstimates=false&zoom=" \
          "10&redesign=true&maxPoints=891"
TREASURY_REAL_RATES_URL = "https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=realyieldYear&year={}"
FEDERAL_FUNDS_RATE_URL = "https://ycharts.com/charts/fund_data.json?annotations=&annualizedReturns=false&calcs=&chartType=interactive&chartView=&correlations=&dateSelection=range&displayDateRange=false&displayTicker=false&endDate=&format=real&legendOnChart=false&note=&partner=basic_2000&quoteLegend=false&recessions=false&scaleType=linear&securities=id%3AI%3AEFFRND%2Cinclude%3Atrue%2C%2C&securityGroup=&securitylistName=&securitylistSecurityId=&source=false&splitType=single&startDate=&title=&units=false&useCustomColors=false&useEstimates=false&zoom=5&redesign=true&maxPoints=787"
US_INITIAL_JOBLESS_URL = "https://sbcharts.investing.com/events_charts/us/294.json"
US_CONTINUING_JOBLESS_URL = "https://sbcharts.investing.com/events_charts/us/522.json"
GLD_URL = "http://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv"

NEWYORKFED_HOLD_OUTPUT_NAME = 'newyorkfed_makert_hold.json'
NEWYORKFED_WEI_NAME = 'newyorkfed_wei.json'
OLI_OUTPUT_NAME = os.path.join(OUTPUT, 'oli.json')
OLI_GOLD_RATIO_OUTPUT_NAME = os.path.join(OUTPUT, 'oil_gold_ratio.json')
GOLD_OUTPUT_NAME = os.path.join(OUTPUT, 'gold.json')
COPPER_GOLD_RATIO_NAME = os.path.join(OUTPUT, 'copper_gold_ratio.json')
INTEREST_RATES_NAME = os.path.join(OUTPUT, 'interest_rates.json')
COPPER_NAME = os.path.join(OUTPUT, 'copper.json')
GOLD_SINCE_NAME = os.path.join(OUTPUT, 'gold_since.json')
INITIAL_JOBLESS_OUTPUT = os.path.join(OUTPUT, 'initial_jobless.json')
CONTINUING_JOBLESS_OUTPUT = os.path.join(OUTPUT, 'continuing_jobless.json')
US_JOBLESS_OUTPUT = os.path.join(OUTPUT, 'us_jobless.json')
CPI_FFR_OUTPUT = os.path.join(OUTPUT, 'cpi_ffr.json')
GLD_OUTPUT = os.path.join(OUTPUT, 'gld.json')
SP500_SMI_OUTPUT = os.path.join(OUTPUT, 'sp500smi.json')
DASHBOARD_LEFT = os.path.join(OUTPUT, 'board_left.json')
TREASURY_REAL_RATES_NAME = 'treasury_real_rates_{}.json'
FEDERAL_FUNDS_RATE_NAME = 'federal_founds_rate_{}.json'

LOOP_GAP = int(os.getenv("LOOP_GAP", 2))
BROWSER_NAME = os.getenv("BROWSER_NAME", "chrome")
CN_TIMEZONE = timezone("Asia/Shanghai")
US_TIMEZONE = timezone('America/New_York')

SENTRY_SDN = os.getenv("SENTRY_SDN", "https://d9043653a7fc461daa61c8905e960c23@o356170.ingest.sentry.io/5440867")

ETF_LIST = [
    'VTI',
    'DIA',
    'OEF',
    'MDY',
    'SPY',
    'RSP',
    'QQQ',
    'QTEC',
    'IWB',
    'IWM',
    'MTUM',
    'VLUE',
    'QUAL',
    'USMV',
    'IWF',
    'IWD',
    'IVW',
    'IVE',
    'MOAT',
    'FFTY',
    'IBUY',
    'HACK',
    'SKYY',
    'IPAY',
    'FINX',
    'XT',
    'ARKK',
    'BOTZ',
    'MOO',
    'ARKG',
    'MJ',
    'ARKW',
    'ARKQ',
    'PBW',
    'BLOK',
    'SNSR',

    'XLC',
    'XLY',
    'XHB',
    'XRT',
    'XLP',
    'XLE',
    'XOP',
    'OIH',
    'TAN',
    'URA',
    'XLF',
    'KBE',
    'KIE',
    'IAI',
    'XLV',
    'IBB',
    'IHI',
    'IHF',
    'XPH',
    'XLI',
    'ITA',
    'IYT',
    'JETS',
    'XLB',
    'GDX',
    'XME',
    'LIT',
    'REMX',
    'IYM',
    'XLRE',
    'VNQ',
    'VNQI',
    'REM',
    'XLK',
    'VGT',
    'FDN',
    'SOCL',
    'IGV',
    'SOXX',
    'XLU',
    'AAPL',
    'FB',
    'AMZN',
    'MSFT',
    'NVDA',
    'NFLX',
    'GOOG',
]

LEFT = [
    "^TNX", # Treasury Yield 10 | 'UST 10Y Yield'
    "DX=F", # 'US Dollar'
    "EURUSD=X", # 'EUR'
    "JPYUSD=X", # 'JPY'
    "GBPUSD=X", # 'GBP'
    "CADUSD=X", # 'CAD'
    "MXNUSD=X", # 'MXN'
    "AUDUSD=X", # 'AUD'
    "KRWUSD=X", # 'KRW'
    "INRUSD=X", # 'INR'
    "BTC-USD", # 'Bitcoin'
    "CL=F", # 'WEI Crude'
    "BZ=F", # 'Brent'
    "RB=F", # 'Gasoline'
    "NG=F", # 'Natural Gas'
    "GC=F", # 'Gold'
    "SI=F", # 'Silver'
    "PL=F", # 'Platinum'
    "PA=F", # 'Palladium'
    "HG=F", # 'Copper'
    "ZC=F", # 'Corn'
    "ZW=F", # 'Wheat'
    "ZS=F", # 'Soybean'
    "KC=F", # 'Coffee'
    "SB=F", # 'Sugar'
    "CT=F", # 'Cotton'
    "CC=F", # 'Cocoa'
    "LE=F", # 'Live Cattle'
]

LEFT_MAP = {
    "^TNX" :'UST 10Y Yield',
    "GC=F":'Gold',
    "DX=F":'US Dollar',
    "EURUSD=X":'EUR',
    "JPYUSD=X":'JPY',
    "GBPUSD=X":'GBP',
    "CADUSD=X":'CAD',
    "MXNUSD=X":'MXN',
    "AUDUSD=X":'AUD',
    "KRWUSD=X":'KRW',
    "INRUSD=X":'INR',
    "BTC-USD":'Bitcoin',
    "CL=F":'WEI Crude',
    "BZ=F":'Brent',
    "RB=F":'Gasoline',
    "NG=F":'Natural Gas',
    "SI=F":'Silver',
    "PL=F":'Platinum',
    "PA=F":'Palladium',
    "HG=F":'Copper',
    "ZC=F":'Corn',
    "ZW=F":'Wheat',
    "ZS=F":'Soybean',
    "KC=F":'Coffee',
    "SB=F":'Sugar',
    "CT=F":'Cotton',
    "CC=F":'Cocoa',
    "LE=F":'Live Cattle',
}

FANGMAN = [    'AAPL',
               'FB',
               'AMZN',
               'MSFT',
               'NVDA',
               'NFLX',
               'GOOG',]

ARK_DATA_URL_LIST = [
    {'fund':'ARKK', 'url':'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv'},
    {'fund':'ARKW','url':'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv'}, 
    {'fund':'ARKQ', 'url':'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv'},
    {'fund':'ARKG','url':'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv'},
    {'fund':'ARKF','url':'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv'}
]
BREADTH_TABLE_NAME = os.getenv("DYNAMODB_NAME", 'Breadth-pro')


if __name__ == '__main__':
    # timezone('Asia/Shanghai')
    # print(timezone("us"))
    print(country_timezones('us'))
    print(DEBUG)

