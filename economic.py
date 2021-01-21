from bin import economic_index
from bin import yf

def main():
    economic_index.wei()
    economic_index.gold()
    economic_index.fftr()
    economic_index.INFLATION_USA()
    economic_index.effr()
    economic_index.USTREASURY_REALYIELD()
    # 期货和大宗商品 不全
    yf.get()


if __name__ == '__main__':
    main()