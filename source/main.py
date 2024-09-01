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
from datetime import datetime

# pandas
import pandas as pd

# matplotlib
import matplotlib.pyplot as plt

# to fetch stock data from Yahoo Finance
import yfinance as yf

# GUI
import tkinter as tk
from tkinter import ttk

# constants specifically for program
from constants import GUI, LOG_LEVEL

# import helper functions
import util

# trading logic
import trading

PY_VERSION = sys.version_info[:3] # version into major, minor, micro

# TODO: fetch from trading instead, from the Exchange object
traders = []

# display
def create_root_display():
    # create root window
    root = tk.Tk()
    root.title("Trader Plus")
    root.resizable(width=0, height=0)
    x = (root.winfo_screenwidth() // 2) - (GUI.WINDOW_WIDTH // 2)
    y = (root.winfo_screenheight() // 2) - (GUI.WINDOW_HEIGHT // 2)
    root.geometry('{}x{}+{}+{}'.format(GUI.WINDOW_WIDTH, GUI.WINDOW_HEIGHT, x, y))
    
    # create notebook(tabs)
    notebook = ttk.Notebook(root)
    notebook.pack(fill='both', expand=True)

    stock_list_tab = ttk.Frame(notebook)
    market_session_tab = ttk.Frame(notebook)
    add_traders_tab = ttk.Frame(notebook)
    
    notebook.add(stock_list_tab, text='Stocks List')
    notebook.add(market_session_tab, text='Market Session')
    #notebook.add(add_traders_tab, text='Add Traders')
    
    # create tab displays
    display_stock_list_tab(stock_list_tab)
    display_market_session_tab(market_session_tab)
    #display_add_traders_tab(add_traders_tab)

    return root

# stocks display
def display_stock_list_tab(root):       
    # create treeview
    columns = ["Symbol", "Name", "Country", "Current", "Previous Close", "Day High", "Day Low"]
    stocks_treeview = ttk.Treeview(root, columns=columns, show='headings')
    # setup heading and columns    
    for col in columns:
        # setup heading and sorting function
        stocks_treeview.heading(col, 
                                text=col,
                                command=lambda _col=col: util.sort_treeview(stocks_treeview, _col, False))
        # setup columns
        if col == "Name":
            stocks_treeview.column(col, width=150, anchor="center", stretch=True)
        elif col == "Country":
            stocks_treeview.column(col, width=80, anchor="center")
        else:
            stocks_treeview.column(col, width=50, anchor="center")
    stocks_treeview.pack(side='bottom', anchor='s', expand=True, fill='both')
    
    # create dropdown
    #indices_dropdown = ttk.Combobox(root, values=STOCK_INDICES, state='readonly')
    #indices_dropdown.set('S&P 500')
    #indices_dropdown.pack(side='top')
    #indices_dropdown.bind("<<ComboboxSelected>>", lambda event: display_stock_list(stocks_treeview, indices_dropdown.get()))
    
    # create scrollbar
    y_scrollbar = ttk.Scrollbar(stocks_treeview, orient='vertical', command=stocks_treeview.yview)
    y_scrollbar.pack(side='right', fill='y')
    
    stocks_treeview.configure(yscrollcommand=y_scrollbar.set)
    
    # create a label to display the timer
    time_label = tk.Label(root, 
                           text=f'Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                           font=("Helvetica", 12))
    time_label.pack(side='top', anchor='ne')
    
    display_stock_list(stocks_treeview, time_label) # initial

# TODO: add index dropdown
def display_stock_list(root, label, dropdown=None):
    util.log("Updating stock list...", LOG_LEVEL.INFO)

    symbols = fetch_stock_symbols_from_index() # TODO: get from dropdown

    insert_stock_data_treeview(root, symbols)
    
    util.log("Stock list updated", LOG_LEVEL.INFO)
    
    # setup timer to update every 30 seconds, needs to be re-registered everytime
    #TODO: use config
    root.after(30000, lambda: display_stock_list(root, label, dropdown))
    label.after(30000, lambda: label.config(text=f'Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'))

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

def insert_stock_data_treeview(root, data):
    # clear treeview
    for item in root.get_children():
        root.delete(item)

    # fetch results
    res = get_yfinance_stock_data_partial_from_tickers(data)
    
    for data in res:
        # insert data into treeview
        root.insert("", "end", values=[data["symbol"],
                                       data["name"],
                                       data["country"],
                                       data["current"],
                                       data["previous_close"],
                                       data["day_high"],
                                       data["day_low"]],
                    tags=("up" if data["current"] >= data["previous_close"] else "down"))
        
        # configure tags colour(up is green, down is red)
        root.tag_configure("up", foreground="green")
        root.tag_configure("down", foreground="red")

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

# traders display
def display_market_session_tab(root):
    # create a label to display the timer
    time_label = tk.Label(root, 
                           text=f'Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                           font=("Helvetica", 12))
    time_label.pack(side='top', anchor='ne')
    
    # TODO: show options for market session
    # create trader type dropdown
    values = ["All",
              "Moving Average Crossover",
              "Price Change",
              "Volume Change",
              "Mean Reversion",
              "Market Cap",
              "RSI"] # TODO: change to enum
    trader_type_dropdown = ttk.Combobox(root, values=values, state='readonly')
    trader_type_dropdown.set('All')
    trader_type_dropdown.pack(side='top', anchor='n', pady=10)
    trader_type_dropdown.bind("<<ComboboxSelected>>",
                          lambda event: update_trader_treeview(root, traders_treeview, time_label, trader_type_dropdown.get()))

    # create_progress_bar
    progress_bar = ttk.Progressbar(root, orient='horizontal', length=200, mode='determinate', maximum=100)
    progress_bar.pack(side='top', pady=(0, 10))

    # create button to run market sessions
    run_market_sessions_button = ttk.Button(root,
                                            text="Run Market Sessions", 
                                            command=lambda: run_market_sessions(root,
                                                                                traders_treeview,
                                                                                time_label,
                                                                                progress_bar,
                                                                                trader_type_dropdown.get()))
    run_market_sessions_button.pack(side='top', pady=(0, 10))
    
    # create traders treeview
    columns = ["TID", "Type", "Balance", "Number of Trades", "Profit/Time"]
    traders_treeview = ttk.Treeview(root, columns=columns, show='headings')
    # setup heading and columns
    for col in columns:
        # setup heading and sorting function
        traders_treeview.heading(col, 
                                text=col,
                                command=lambda _col=col: util.sort_treeview(traders_treeview, _col, False))
        # setup columns 
        if col == "Number of Trades":
            traders_treeview.column(col, width=120, stretch=True, anchor="center")
        else:
            traders_treeview.column(col, width=80, stretch=True, anchor="center")
    traders_treeview.pack(side='left', anchor='w', fill='both', expand=True)
    
    # create scrollbar
    y_scrollbar = ttk.Scrollbar(traders_treeview, orient='vertical', command=traders_treeview.yview)
    y_scrollbar.pack(side='right', fill='y')
    
    traders_treeview.configure(yscrollcommand=y_scrollbar.set)

# TODO: refactor to use enums for trader types
def update_trader_treeview(root, treeview, label, traders):
    util.log("Updating trader list...", LOG_LEVEL.INFO)
    
    # clear treeview
    for item in treeview.get_children():
        treeview.delete(item)
    
    # display results    
    for tid in traders:
        trader = traders.get(tid)
        
        # firstly show the trader with it's tid, type, balance, trades and profit per time
        # clicking on the row will show details on the right side(like a sideview) with actual trade information
        treeview.insert("", "end", values=[trader.tid,
                                       trader.ttype,
                                       f"{trader.balance:.2f}", # truncate to 2 decimal places for display
                                       trader.n_trades,
                                       f"{trader.profit_per_time:.2f}"], # truncate to 2 decimal places for display
                    tags=("up" if trader.balance >= 0.0 else "down"))
        treeview.tag_configure("up", background="green")
        treeview.tag_configure("down", background="red")
        
        # update time label
        label.config(text=f'Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # bind row select event to treeview
    treeview.bind("<Button-1>", lambda event: on_treeview_row_select(event, root, treeview, traders))
    
    util.log("Trader list updated", LOG_LEVEL.INFO)

def on_treeview_row_select(event, root, treeview, traders):
    row_id = treeview.identify('item', event.x, event.y)
    item = treeview.item(row_id)
    
    if item:
        # delete previous panel
        for widget in root.winfo_children():
            if isinstance(widget, tk.Frame):
                widget.destroy()
        
        values = item['values']
        
        current_orders = traders.get(values[0]).current_orders
        trades_executed = traders.get(values[0]).trades_executed
        
        # create side panel
        side_panel = tk.Frame(root, bg='white', width=200)
        side_panel.pack(side='right', anchor='e', fill='y', expand=False)

        # create treeview for current orders
        columns = ["QID", "Ticker", "Type", "Price", "Quantity", "Time"]
        current_orders_treeview = ttk.Treeview(side_panel, columns=columns, show='headings')
        # setup heading and columns
        for col in columns:
            # setup heading and sorting function
            current_orders_treeview.heading(col, 
                                    text=col,
                                    command=lambda _col=col: util.sort_treeview(current_orders_treeview, _col, False))
            if col == "QID":
                current_orders_treeview.column(col, width=120, stretch=False, anchor="center")
            else:
                current_orders_treeview.column(col, width=75, stretch=False, anchor="center")
        current_orders_treeview.pack(side='top', anchor='n', fill='x', expand=True)

        for order in current_orders:
            # insert data into current orders treeview
            current_orders_treeview.insert("", "end", values=[order.qid,
                                                              order.ticker,
                                                              order.order_type,
                                                              f"{order.price:.2f}", # truncate to 2 decimal places for display
                                                              order.quantity,
                                                              datetime.fromtimestamp(order.time)])

        # create treeview for trades executed
        columns = ["Counterparty", "Ticker", "Type", "Price", "Quantity", "Time"]
        trades_executed_treeview = ttk.Treeview(side_panel, columns=columns, show='headings')
        # setup heading and columns
        for col in columns:
            # setup heading and sorting function
            trades_executed_treeview.heading(col, 
                                    text=col,
                                    command=lambda _col=col: util.sort_treeview(trades_executed_treeview, _col, False))
            if col == "Counterparty":
                trades_executed_treeview.column(col, width=120, stretch=False, anchor="center")
            else:
                trades_executed_treeview.column(col, width=75, stretch=False, anchor="center")
        trades_executed_treeview.pack(side='bottom', anchor='n', fill='x', expand=True)
                
        for trade in trades_executed:
            # insert data into trades executed treeview
            trades_executed_treeview.insert("", "end", values=[trade["party2"],
                                                               trade["ticker"],
                                                               trade["type"],
                                                               f"{trade['price']:.2f}", # truncate to 2 decimal places for display
                                                               trade["qty"],
                                                               datetime.fromtimestamp(trade["time"])])

        # clear graphs
        plt.clf()
           
        # display graphs
        tickers = list(set([trade['ticker'] for trade in trades_executed])) # set to remove duplicate tickers
        tickers_data = get_yfinance_stock_data_full_from_tickers(tickers)
        for ticker in tickers:
            trade_prices_for_ticker = [trade['price'] for trade in trades_executed if trade['ticker'] == ticker]
            time_period = range(1, len(trade_prices_for_ticker) + 1)
            # plot trader performance
            plt.plot(time_period, trade_prices_for_ticker, label=f'Price Curve for {ticker}')
            
            # plot actual performance from yfinance
            yf_data = [data[0] for data in tickers_data if data[1]['symbol'] == ticker][0]
            close = yf_data['Close'].iloc[-len(trade_prices_for_ticker):] # TODO: cheats, need to use time of trade and pandas indexing

            plt.plot(time_period, close, label=f'Price Curve for {ticker} from yfinance')
        
        plt.xlabel('Time Period')
        plt.ylabel('Price at Trade Execution')
        plt.title('Trader Performance vs Actual Performance')
        plt.legend()        
        plt.grid(True)
        plt.show()

# add traders display
def display_add_traders_tab(root):
    # values # TODO: fetch this from somewhere else
    dropdown_values_dict = {'Moving Average Crossover': 'MAC', 
              'Price Change': 'PC',
              'Volume Change': 'VC',
              'Mean Reversion': 'MR',
              'Market Cap': 'MC',
              'Relative Strength Index': 'RSI'}
    
    # create trader type label and dropdown
    trader_type_label = ttk.Label(root, text="Trader Type:")
    trader_type_label.pack(side='top', pady=10)
    trader_type_dropdown = ttk.Combobox(root, 
                                        values=list(dropdown_values_dict.keys()), 
                                        state='readonly')
    trader_type_dropdown.pack(side='top')

    # create add trader button
    # get values from dict using dropdown key
    add_trader_button = ttk.Button(root, text="Add Trader", 
                                   command=lambda: add_trader(trader_type_dropdown, dropdown_values_dict[trader_type_dropdown.get()]))
    add_trader_button.pack(side='top', pady=10)

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
        market_session = trading.Market_Session(trial_id,
                                               tickers_data,
                                               start_time, 
                                               end_time,
                                               traders_spec,
                                               None) # TODO: lob dump file
        market_session.run()
        # TODO: display traders list after session
        update_trader_treeview(root, treeview, label, market_session.traders)
        
        # update progress bar
        progress_bar.step((trial / n_trials) * 100)
        
        # increment trial
        trial += 1

# code entry point
if __name__ == "__main__": 
    # create root display
    root = create_root_display()
    root.mainloop()