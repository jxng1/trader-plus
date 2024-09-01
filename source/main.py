# system to get python version
import sys

# scraping
import requests
import bs4 as bs

# threading
from concurrent.futures import ProcessPoolExecutor # multiprocessing is faster
from multiprocessing import Pool
import ray # ray is the fastest

# time
import time

# pandas
import pandas as pd

# to fetch stock data from Yahoo Finance
import yfinance as yf

# constants specifically for program
from constants import GUI, LOG_LEVEL

# import helper functions
import util

# trading logic
import traders

# ui logic
import ui

PY_VERSION = sys.version_info[:3] # version into major, minor, micro

# TODO: fetch from trading instead, from the Exchange object
traders = []

def fetch_stock_symbols_from_index(index="S&P 100"):    
    # scrape symbols from Wikipedia, works for S&P 500, S&P 100
    # TODO: switch over to scraping https://markets.businessinsider.com/index/components/s&p_500 as it has Dow Jones, S&P 500, etc.
    resp = requests.get(f'https://en.wikipedia.org/wiki/{index}')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    symbols = []
    for row in table.findAll('tr')[1:]:
        symbol = row.findAll('td')[0].text
        symbols.append(symbol.replace('\n', ''))
        
    symbols = [symbol if '.' not in symbol else symbol.replace('.', '-') for symbol in symbols] # fix for yahoo # TODO: find a better way
    return symbols

# TODO: refactor all fetch code to be simplified
# fetch stock data(partial) from yfinance - used to display stock list
def get_yfinance_stock_data_partial(ticker, period="1d", interval="1m", prepost=True, rounding=False):
    # fetch data from yf
    ticker_data = yf.Ticker(ticker)
        
    # get info and history
    info = ticker_data.info
    history = ticker_data.history(period=period, interval=interval, prepost=prepost, rounding=rounding)

    # if something went wrong with partial fetch, try a full fetch
    if not info or history.empty:
        util.log(f"Partial fetch failed for {ticker}, trying full fetch...", LOG_LEVEL.WARNING)
        out = get_yfinance_stock_data_full(ticker, period, interval, prepost, rounding)
        
        history = out[0]
        info = out[1]

    # form dictionary
    data = {
        "symbol": ticker, # symbol
        "name": info["shortName"], # ticker name
        "country": info["country"], # company country
        "current": history["Close"].values[-1], # current close price
        "open": history["Open"].values[-1], # current open price
        "high": history["High"].values[-1], # current high price
        "low": history["Low"].values[-1], # current low price
        "previous_close": info["previousClose"], # end of day price
        "day_high": info["dayHigh"], # day high
        "day_low": info["dayLow"], # day low
    }
        
    return data

def get_yfinance_stock_data_partial_from_tickers(tickers):
    res = []

    if PY_VERSION > (3, 11, 0): # ray non-compatible with 3.12(as of yet)    
        # multiprocessing
        start = time.time()
        num_processes = 5 # >= os.cpu_count() is worse, due to context switching no doubt
        with Pool(num_processes) as p:
            res = p.map(get_current_stock_data_partial, tickers)
        util.log(f"(multiprocessing) time taken to fetch data: {time.time() - start} seconds", LOG_LEVEL.INFO)
    elif PY_VERSION == (3, 11, 0): 
        # ray - note I had to downgrade to 3.11 for this to work(ray should be compatible with 3.12 soon, 3-4 months)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True) # pre init
        start = time.time()
        res_ids = [get_current_stock_data_partial.remote(s) for s in tickers]
        for id in res_ids:
            res.append(ray.get(id))
        util.log(f"(ray) time taken to fetch data: {time.time() - start} seconds", LOG_LEVEL.INFO)
    else:
        util.log("Python version not supported", LOG_LEVEL.ERROR)
        return []
        
    return res

# fetch stock data(full) from yfinance - used for traders
def get_yfinance_stock_data_full(ticker, period="1d", interval="1m", prepost=True, rounding=False):
    # fetch data from yf
    ticker_data = yf.Ticker(ticker)
        
    # get full info and history
    info = ticker_data.info
    history = ticker_data.history(period=period, interval=interval, prepost=prepost, rounding=rounding)
        
    return [history, info]

def get_yfinance_stock_data_full_from_tickers(tickers):
    res = []

    if PY_VERSION > (3, 11, 0): # ray non-compatible with 3.12(as of yet)    
        # multiprocessing
        start = time.time()
        num_processes = 5 # >= os.cpu_count() is worse, due to context switching no doubt
        with Pool(num_processes) as p:
            res = p.map(get_current_stock_data_full, tickers)
        util.log(f"(multiprocessing) time taken to fetch data: {time.time() - start} seconds", LOG_LEVEL.INFO)
    elif PY_VERSION == (3, 11, 0): 
        # ray - note I had to downgrade to 3.11 for this to work(ray should be compatible with 3.12 soon, 3-4 months)
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True) # pre init
        start = time.time()
        res_ids = [get_current_stock_data_full.remote(s) for s in tickers]
        for id in res_ids:
            res.append(ray.get(id))
        util.log(f"(ray) time taken to fetch data: {time.time() - start} seconds", LOG_LEVEL.INFO)
    else:
        util.log("Python version not supported", LOG_LEVEL.ERROR)
        return []
        
    return res

# this is quite messy as python has no preprocessor directives
if PY_VERSION > (3, 11, 0): # ray non-compatible with 3.12(as of yet)
    def get_current_stock_data_partial(ticker):
        return get_yfinance_stock_data_partial(ticker, period="7d", interval="1m", prepost=True, rounding=False)
    
    def get_current_stock_data_full(ticker):
        return get_yfinance_stock_data_full(ticker, period="7d", interval="1m", prepost=True, rounding=False)
elif PY_VERSION == (3, 11, 0): # ray compatible with 3.11(and below, but ignore)
    @ray.remote
    def get_current_stock_data_partial(ticker):
        return get_yfinance_stock_data_partial(ticker, period="7d", interval="1m", prepost=True, rounding=False)
    
    @ray.remote
    def get_current_stock_data_full(ticker):
        return get_yfinance_stock_data_full(ticker, period="7d", interval="1m", prepost=True, rounding=False)

# run market session logic
def run_market_sessions(root, treeview, label, progress_bar, ttype):
    # TODO: remove and use traders list
    # TODO: lazy as buyers match sellers spec
    parameters = get_yfinance_stock_data_full_from_tickers(fetch_stock_symbols_from_index())
    
    # create specification, 2-tuple but parameters[1] is a list of details
    buyers_spec = [('MAC', parameters),
                   ('PC', parameters),
                   ('MR', parameters), 
                   ('RSI', parameters),
                   ('VC', parameters),
                   ('MC', parameters),
                   ('MAC', parameters)]
    sellers_spec = buyers_spec

    # set up traders
    traders_spec = {'sellers': sellers_spec, 'buyers': buyers_spec}

    # setup default market session parameters
    n_days = 0.001 # 0.1% of a day
    start_time = time.time() # should be the current time from epoch, not epoch as it was used prior
    #end_time = 60.0 * 60.0 * 24 * n_days
    end_time = start_time + (60.0 * 60.0 * 24 * n_days) # how many days in seconds, it's quite naive as it doesn't account for leap seconds/years
    duration = end_time - start_time
    n_trials = 1
    
    trial = 1 # start at 1 for display
    
    # reset progress bar
    progress_bar['value'] = 0.0
    while trial < (n_trials + 1):      
        util.log(f"Running trial {trial}...", LOG_LEVEL.INFO)
        
        # create unique id for trial
        trial_id = f"tp_{n_days}:{n_trials}:{duration}"
        
        # fetch data for tickers
        tickers_data = get_yfinance_stock_data_full_from_tickers(fetch_stock_symbols_from_index())
        
        # TODO: get from dropdown
        # create market session object and run it
        market_session = traders.Market_Session(trial_id,
                                               tickers_data,
                                               start_time, 
                                               end_time,
                                               traders_spec,
                                               None) # TODO: lob dump file
        market_session.run()
        # TODO: display traders list after session
        ui.update_trader_treeview(root, treeview, label, market_session.traders)
        
        # update progress bar
        progress_bar.step((trial / n_trials) * 100)
        
        # increment trial
        trial += 1

# add trader logic
def add_trader(root, ttype):
    result_label = ttk.Label(root)
        
    parameters = get_yfinance_stock_data_full_from_tickers(fetch_stock_symbols_from_index()) # TODO: find another way to do this

    # add trader to traders list # TODO: do this locally
    traders.append((ttype, parameters))

    # clear input fields
    root.set('')
        
    # display success message
    result_label.configure(text="Trader added successfully.")

# code entry point
if __name__ == "__main__": 
    # create root display
    root = ui.create_root_display()
    root.mainloop()