# constants specifically for program
from constants import GUI, LOG_LEVEL

# GUI
import tkinter as tk
from tkinter import ttk

# matplotlib
import matplotlib.pyplot as plt

# time
from datetime import datetime

# pandas
import pandas as pd

# util
import util

# main
import main

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
def display_stock_list(root, label, dropdown=None, symbols=[]):
    util.log("Updating stock list...", LOG_LEVEL.INFO)
    
    insert_stock_data_treeview(root, symbols)
    
    util.log("Stock list updated", LOG_LEVEL.INFO)
    
    # setup timer to update every 30 seconds, needs to be re-registered everytime
    #TODO: use config
    root.after(30000, lambda: display_stock_list(root, label, dropdown))
    label.after(30000, lambda: label.config(text=f'Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'))

def insert_stock_data_treeview(root, res):
    # clear treeview
    for item in root.get_children():
        root.delete(item)
    
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
    # TODO: needs a better way to use main as this tends to be a bit hacky
    run_market_sessions_button = ttk.Button(root,
                                            text="Run Market Sessions", 
                                            command=lambda: main.run_market_sessions(root,
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
                                   command=lambda: main.add_trader(trader_type_dropdown, dropdown_values_dict[trader_type_dropdown.get()]))
    add_trader_button.pack(side='top', pady=10)

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
        tickers_data = main.get_yfinance_stock_data_full_from_tickers(tickers) # TODO: call util instead or some query, just not main(?)
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