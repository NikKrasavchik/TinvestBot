from pyrogram import Client as tgClient
from pyrogram import filters
from tinkoff.invest import Client, InstrumentStatus, OrderDirection, OrderType, Quotation
from pandas import DataFrame
from datetime import datetime
from time import sleep
from threading import Thread
from math import floor, ceil


TOKEN = "t.WCWdn68CBp2eKsjA9IQK0IXZS2_Ws-wKYTYpVU1AGIeKMEfXzO7W7JJ9895kTLOL1AP7k2W_dexpRG8MxL8LQw"

API_ID = 11765528
API_HASH = '7f2e6a3543a3f70a00797af94d46956c'
SOURCE_PUBLICS = ['kentblablabla']
PHONE_NUMBER = '+79188653411'
app = tgClient("nikkk", api_id=API_ID, api_hash=API_HASH, phone_number=PHONE_NUMBER)

BALANCE = 0
BALANCE_FREE = 0
LOT_PROPORTION = 0.05

SHARES = []
ACC_ID = ''
SHARE_INFO_DF = []
ORDERS = []


class Share:
    ticker = ""
    lots = 0
    sell = 0
    stop = 0
    profit = 0
    buy = 0
    limit_profit_id = ""
    limit_stop_id = ""

    def __init__(self, ticker_, lots_, profit_, stop_, buy_):
        self.ticker = ticker_
        self.lots = lots_
        self.profit = profit_
        self.stop = stop_
        self.buy = buy_

    def selling(self, sell_status):
        if self.lots / 3 < 1 and self.lots / 2 < 1:
            sell_status = 1

        if sell_status == 0:
            if self.lots / 2 < 1:
                self.sell = 1
            if self.sell == 0:
                isSell = sell_share(self.ticker, self.lots//3)
                if isSell:
                    self.lots = self.lots // 3 * 2
                    self.sell += 1
                    return True
            if self.sell == 1:
                isSell = sell_share(self.ticker, self.lots//2)
                if isSell:
                    self.lots //= 2
                    self.sell += 1
                    return True
            if self.sell == 2:
                isSell = sell_share(self.ticker, self.lots)
                if isSell:
                    return False

        if sell_status == 1:
            isSell = sell_share(self.ticker, self.lots)
            if isSell:
                return False

        return True

    def print(self):
        print("\nShare:")
        print("Ticker:", self.ticker)
        print("Lots:", self.lots)
        print("Profit:", self.profit)
        print("Stop:", self.stop)
        print("Sell status:", self.sell)
        print()

    def returnDF(self):
        return [self.ticker, self.lots, self.profit, self.stop, self.buy]

def set_limit(figi_, share, limit_status):
    global ACC_ID

    price_profit = Quotation(units=int(share.profit), nano=(int(share.profit*1e9 - int(share.profit)*1e9)))
    price_stop = Quotation(units=int(share.stop), nano=(int(share.stop*1e9 - int(share.stop)*1e9)))

    if limit_status:
        share.limit_profit_id = str(datetime.utcnow().timestamp())
    else:
        share.limit_stop_id = str(datetime.utcnow().timestamp())

    with Client(TOKEN) as client:
        client.orders.post_order(
            order_id=share.limit_profit_id if limit_status else share.limit_stop_id,
            figi=figi_,
            price=price_profit if limit_status else price_stop,
            quantity=share.lots,
            account_id=ACC_ID,
            direction=OrderDirection.ORDER_DIRECTION_BUY if limit_status else OrderDirection.ORDER_DIRECTION_SELL,
            order_type=OrderType.ORDER_TYPE_LIMIT)

def limit_stop():
    global SHARE_INFO_DF
    global ACC_ID
    global ORDERS

    #if (datetime.now())

    with Client(TOKEN) as client:
        while True:
            for share in SHARES:
                if ORDERS[ORDERS['order_id'] == share.limit_stop_id].shape[0] == 0:
                    figi_ = SHARE_INFO_DF[SHARE_INFO_DF['ticker'] == share.ticker]['figi'].iloc[0]

                    book = client.market_data.get_order_book(figi=figi_, depth=50)
                    limit_down = book.limit_down.units + book.limit_down.nano / 1e9

                    if share.stop > limit_down:
                        set_limit(figi_, share, 0)
            sleep(30)

def limit_profit():
    global SHARE_INFO_DF
    global ACC_ID
    global ORDERS
    with Client(TOKEN) as client:
        while True:
            for share in SHARES:
                if ORDERS[ORDERS['order_id'] == share.limit_profit_id].shape[0] == 0:
                    figi_ = SHARE_INFO_DF[SHARE_INFO_DF['ticker'] == share.ticker]['figi'].iloc[0]

                    book = client.market_data.get_order_book(figi=figi_, depth=50)
                    limit_up = book.limit_up.units + book.limit_up.nano / 1e9

                    if share.profit < limit_up:
                        set_limit(figi_, share, 1)
            sleep(30)

def buy_share(figi_, lots_):
    global ACC_ID

    with Client(TOKEN) as client:
        order_info = client.orders.post_order(
            order_id=str(datetime.utcnow().timestamp()),
            figi=figi_,
            quantity=lots_,
            account_id=ACC_ID,
            direction=OrderDirection.ORDER_DIRECTION_BUY,
            order_type=OrderType.ORDER_TYPE_MARKET)
        sleep(5)
        if order_info.execution_report_status == 'EXECUTION_REPORT_STATUS_REJECTED':
            return False
        return True

def sell_share(ticker_, lots_):
    global ACC_ID
    global SHARE_INFO_DF
    figi_ = SHARE_INFO_DF[SHARE_INFO_DF['ticker'] == ticker_]['figi'].iloc[0]

    global SHARES
    for share in SHARES:
        if share.ticker == ticker_:
            share.stop = share.buy

    with Client(TOKEN) as client:
        DF = DataFrame(client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE).instruments,
                       columns=['figi', 'ticker'])
        figi_ = DF[DF['ticker'] == ticker_]['figi'].iloc[0]

        order_info = client.orders.post_order(
            order_id=str(datetime.utcnow().timestamp()),
            figi=figi_,
            quantity=lots_,
            account_id=ACC_ID,
            direction=OrderDirection.ORDER_DIRECTION_SELL,
            order_type=OrderType.ORDER_TYPE_MARKET)
        return False

def tinkoff_buy(shareBuyInfo):
    global SHARES
    global SHARE_INFO_DF

    ticker = str(shareBuyInfo[0][1::])
    stop = float(shareBuyInfo[1][:-1:])
    profit = float(shareBuyInfo[2][:-1:])
    Lside, Rside = float(shareBuyInfo[3]), float(shareBuyInfo[4][:-1:])
    figi = ""
    lot = 0
    buy = 0

    with Client(TOKEN) as client:
        figi = SHARE_INFO_DF[SHARE_INFO_DF['ticker'] == ticker]['figi'].iloc[0]
        lot = SHARE_INFO_DF[SHARE_INFO_DF['ticker'] == ticker]['lot'].iloc[0]
        book = client.market_data.get_order_book(figi=figi, depth=1)
        bid = book.bids[0].price.units + book.bids[0].price.nano / 1e9
        ask = book.asks[0].price.units + book.asks[0].price.nano / 1e9
        buy = (bid + ask) / 2

    if Lside < buy and buy < Rside:
        lots = ceil(BALANCE * LOT_PROPORTION / (buy*lot))
        if BALANCE_FREE < buy*lots:
            lots = floor(BALANCE_FREE / buy*lot)
        if lots >= 1:
            isBuying = buy_share(figi, lots)
            if isBuying:
                shareWas = False
                for share in SHARES:
                    if share.ticker == ticker:
                        share.lots += lots
                        share.profit = profit
                        share.stop = stop
                        share.buy = buy
                        shareWas = True
                        break
                if not shareWas:
                    sh = Share(ticker, lots, profit, stop, buy)
                    SHARES.append(sh)
                return True
    return False

def tinkoff_sell(shareSellInfo, sell_status):
    global SHARES
    ticker = str(shareSellInfo[1::])

    for i in range(len(SHARES)):
        if SHARES[i].ticker == ticker:
            isAlive = SHARES[i].selling(sell_status)

            if not isAlive:
                SHARES.pop(i)

def define_buy(msgList):
    ticker = ""
    stop = ""
    profit = ""
    Lside, Rside = "", ""

    flag = False
    for i in msgList:
        if i[0] == "#":
            ticker = i
            flag = True
            break
    if not flag:
        return []

    flag = False
    for i in range(len(msgList)):
        if msgList[i][:-1:].isalpha() and msgList[i][:-1:].lower() == "стоп":
            flag = True
        if flag and msgList[i][0].isdigit():
            stop = msgList[i]
            break
    if not flag:
        return []

    flag = False
    for i in range(len(msgList)):
        if msgList[i][:-1:].isalpha() and msgList[i][:-1:].lower() == "цель":
            flag = True
        if flag and msgList[i][0].isdigit():
            profit = msgList[i]
            break
    if not flag:
        return []

    Lflag = False
    Rflag = False
    for i in range(len(msgList)):
        if msgList[i][:-1:].isalpha() and msgList[i][:-1:].lower() == "входа":
            Lflag = True
        if Rflag and msgList[i][0].isdigit():
            Rside = msgList[i]
            break
        if Lflag and msgList[i][0].isdigit():
            Lside = msgList[i]
            Rflag = True
    if not Rflag:
        return []
    else:
        return [ticker, stop, profit, Lside, Rside]

def define_sell(msgList):
    ticker = ""

    for i in msgList:
        if i[0] == "#":
            ticker = i

    return ticker

def defineMsg(msg):
    recieved = list(map(str, msg.split()))

    if (recieved[0].lower() == "покупка"):
        request_buy = define_buy(recieved)
        print("Покупаю", request_buy[0])
        if len(request_buy) != 0:
            if tinkoff_buy(request_buy):
                return True
        return False

    elif (recieved[0].lower() == "фиксирую"):
        request_sell = define_sell(recieved)
        print("Фиксирую", request_sell)
        if request_sell != "":
            if tinkoff_sell(request_sell, 0):
                return True
        return False

    elif (recieved[0].lower() == "закрыта" or recieved[0].lower() == "закрытапо"):
        request_sell = define_sell(recieved)
        print("Закрываю", request_sell)
        if request_sell != "":
            tinkoff_sell(request_sell, 1)
            return True
        return False

    else:
        for i in recieved:
            if i[0] == '#':
                print(i)
                return False

    print("UNKNOWN")
    return False

def get_BALANCE():
    with Client(TOKEN) as client:
        global BALANCE
        global BALANCE_FREE

        portfolio_info = client.operations.get_portfolio(account_id=ACC_ID)

        total_amount_portfolio = portfolio_info.total_amount_portfolio
        BALANCE = total_amount_portfolio.units + total_amount_portfolio.nano / 1e9

        total_amount_currencies = portfolio_info.total_amount_currencies
        BALANCE_FREE = total_amount_currencies.units + total_amount_currencies.nano / 1e9

def format_message(message):
    result_message = ""
    for i in message:
        if (i.isalpha() or i.isdigit() or i in " +%$,.\n#:"):
            result_message += i
    return result_message

@app.on_message(filters.chat(SOURCE_PUBLICS))
def new_channel_post(tgclient, message):
    get_BALANCE()
    tgMessage = format_message(message.text)
    defineMsg(tgMessage)

    global SHARES
    sharesDF = DataFrame([share.returnDF() for share in SHARES], columns=["ticker", "lots", "profit", "stop", "buy"])
    print(sharesDF)
    print()

def load_SHARE_INFO_DF():
    global SHARE_INFO_DF
    with Client(TOKEN) as client:
        while True:
            SHARE_INFO_DF = DataFrame(client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE).instruments,
                                      columns=['figi', 'ticker', 'lot'])
            sleep(10)

def load_ORDERS():
    global ORDERS
    global ACC_ID
    with Client(TOKEN) as client:
        ORDERS = DataFrame(client.orders.get_orders(account_id=ACC_ID).orders, columns=['order_id', 'execution_report_status'])
        sleep(10)

def load_SHARES():
    global SHARES
    global ACC_ID
    global SHARE_INFO_DF
    with Client(TOKEN) as client:
        DF = DataFrame(client.operations.get_positions(account_id=ACC_ID).securities,
                       columns=['figi', 'balance'])
        for i in range(len(DF)):
            ticker = SHARE_INFO_DF[SHARE_INFO_DF['figi'] == DF['figi'].iloc[i]]['ticker'].iloc[0]

            book = client.market_data.get_order_book(figi=DF['figi'].iloc[0], depth=1)
            if len(book.bids) == 0 or len(book.asks) == 0:
                continue
            bid = book.bids[0].price.units + book.bids[0].price.nano / 1e9
            ask = book.asks[0].price.units + book.asks[0].price.nano / 1e9
            buy = (bid + ask) / 2

            lots = DF['balance'].iloc[i]

            profit = int((buy*1.1) * 100)//100
            stop = int((buy*0.95) * 100)//100

            sh = Share(ticker, lots, profit, stop, buy)
            SHARES.append(sh)

def update_SHARES():
    global SHARES
    global ACC_ID
    global SHARE_INFO_DF
    with Client(TOKEN) as client:
        while True:
            DF = DataFrame(client.operations.get_positions(account_id=ACC_ID).securities,
                           columns=['figi', 'balance'])

            indexes = []
            isAslive = False
            for i in range(len(SHARES)):
                isAslive = False
                for j in range(len(DF)):
                    figi = SHARE_INFO_DF[SHARE_INFO_DF['figi'] == DF['figi'].iloc[j]]['figi'].iloc[0]
                    ticker = SHARE_INFO_DF[SHARE_INFO_DF['figi'] == figi]['ticker'].iloc[0]

                    if SHARES[i].ticker == ticker:
                        isAslive = True
                if not isAslive:
                    indexes.append(i)

            for i in range(len(indexes)-1, 0, -1):
                SHARES.pop(indexes[i])
            sleep(60)

def init():
    global ACC_ID
    global SHARES

    with Client(TOKEN) as client:
        acc = client.users.get_accounts().accounts
        ACC_ID = str(acc[0].id)
    print("ACC_ID was uploud")

    load_DF = Thread(target=load_SHARE_INFO_DF)
    load_DF.start()
    sleep(5)
    print("SHARE_INFO_DF was upload")

    #update_shares = Thread(target=update_SHARES)
    #update_shares.start()
    #print("Updating shares is enable")

    #load_SHARES()
    #sleep(1)
    #print("SHARES was upload")

    #orders = Thread(target=load_ORDERS)
    #orders.start()
    #sleep(1)
    #print("ORDERS was upload")

    #limit_down = Thread(target=limit_stop)
    #limit_up = Thread(target=limit_profit)
    #limit_down.start()
    #limit_up.start()
    #print("Limits are active")

    get_BALANCE()
    print("BALANCE =", BALANCE)
    print("BALANCE_FREE =", BALANCE_FREE)
    sharesDF = DataFrame([share.returnDF() for share in SHARES], columns=["ticker", "lots", "profit", "stop", "buy"])
    print(sharesDF)
    print()

if __name__ == '__main__':
    print('Bot was started!')

    #print(datetime.now().minute)

    init()
    app.run()

