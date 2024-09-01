import util
from constants import LOG_LEVEL

# random
import random

# time
import time as chrono
from datetime import datetime

# numpy
import numpy as np

# order
class Order:
    def __init__(self, tid, order_type, price, quantity, time, qid, ticker):
        self.qid = qid # quote ID, unique to each order
        self.tid = tid # trader ID, unique to each trader
        self.ticker = ticker # ticker symbol, all traders can have multiple quotes, but only one per ticker
        self.order_type = order_type # buy or sell order type
        self.price = price # price
        self.quantity = quantity # quantity
        self.time = time # timestamp
        
    def __str__(self):
        return f"[QID: {self.qid} TID: {self.tid} Ticker: {self.ticker} Type: {self.order_type} Price: {self.price} Quantity: {self.quantity} Time: {self.time}]"

# half order book
# a full order book would have two half-order books, one for bids and one for asks
class Half_Orderbook:
    def __init__(self, book_type):
        self.book_type = book_type # bids or asks
        self.orders = {} # dictionary of orders, indexed by trader ID and then by ticker
        self.lob = {} # limit order book, indexed by price and with order info
        self.lob_anon = [] # anonymised list with only price and quantity info
        # summary stats
        self.best_price = None # best price on the book
        self.best_tid = None # trader ID of the order at the best price
        self.worst_price = None # worst price on the book
        self.session_extreme_price = None # most extreme price quoted in this session
        self.n_orders = 0 # number of orders on the book
        self.lob_depth = 0 # number of different prices on the book

    def anonymise_lob(self):
        # anonymise a lob by stripping out order details, except for the order price, quantity and ticker
        # returned as a sorted list
        self.lob_anon = []
        for price in sorted(self.lob):
            qty = self.lob[price][0]
            ticker = self.lob[price][1]
            self.lob_anon.append([price, qty, ticker])
            
    # pass the ticker too, as it's needed to get data
    def build_lob(self, ticker):
        # take the current dictionary of orders and build a limit order book (LOB) for this half
        self.lob = {}
        for tid in self.orders:
            order = self.orders.get(tid).get(ticker)
            
            if not order:
                continue

            price = order.price
            if price in self.lob:
                # update existing entry
                qty = self.lob[price][0] # quantity
                ticker = self.lob[price][1] # ticker
                orders = self.lob[price][2] # orders
                orders.append([order.time, order.quantity, order.tid, order.qid]) # append order to list
                self.lob[price] = [qty + order.quantity, ticker, orders] # form new lob entry
            else:
                # create a new dictionary entry
                self.lob[price] = [order.quantity, order.ticker, [[order.time, order.quantity, order.tid, order.qid]]]
                
        self.anonymise_lob() # anonymise lob
        
        # record stats
        if len(self.lob) > 0:
            if self.book_type == "Bid": # TODO: use enums
                self.best_price = self.lob_anon[-1][0] # sorted, so last entry for highest bid
            else:
                self.best_price = self.lob_anon[0][0] # sorted, so first entry for lowest ask
            self.best_tid = self.lob[self.best_price][2][0][2]
        else :
            self.best_price = None
            self.best_tid = None

    def update_orders_length(self):
        self.n_orders = sum(len(o) for o in self.orders.values())

    def add_to_book(self, order):
        # add an order to the book or update an existing order from this trader
        # traders can have only one order for each ticker at a time
        
        # update session extreme price given it is an ask order#
        # TODO use enums
        if self.book_type == "Ask" \
            and (self.session_extreme_price is None or order.price > self.session_extreme_price):
            self.session_extreme_price = order.price
        
        # add the order to the book
        n_orders = self.n_orders
        
        # init dictionary for tid if it doesn't exist
        self.orders[order.tid] = {}
        self.orders[order.tid][order.ticker] = order
        self.update_orders_length()
        self.build_lob(order.ticker)
        
        # TODO: use enums
        if n_orders != self.n_orders:
            return "Order Added"
        else:
            return "Order Updated"

    def delete_from_book(self, order):
        # delete an order from the book, assumes that there is only one order per trader per ticker
        # checks to make sure trader exists in the book before attempt
        if self.orders.get(order.tid) is not None \
            and self.orders.get(order.tid).get(order.ticker) is not None:
            del self.orders[order.tid][order.ticker]
            self.update_orders_length()
            self.build_lob(order.ticker)
        
    def delete_best(self):
        # delete the best bid/ask from the book when it has been hit, the trader ID of the order is returned
        best_price_orders = self.lob[self.best_price]
        best_price_qty = best_price_orders[0]
        best_price_trader = best_price_orders[2][0][2]
        best_ticker = best_price_orders[1]
            
        if best_price_qty == 1:
            del self.lob[self.best_price]
            del self.orders[best_price_trader][best_ticker]
            self.update_orders_length()
            if self.n_orders > 0:
                if self.book_type == "Bid":
                    self.best_price = max(self.lob.keys()) # fetch next-highest, which will be         
                else:
                    self.best_price = min(self.lob.keys()) # fetch next-lowest, which will be
                self.lob_depth = len(self.lob.keys())   
        else:
            self.lob[self.best_price] = [best_price_qty - 1, best_price_orders[1][1:]] # update lob with decremented quantity
                
            # update bids
            del (self.orders[best_price_trader][best_ticker])
            self.update_orders_length()
            
        self.build_lob(best_ticker)
            
        return best_price_trader

# full orderbook
class Orderbook(Half_Orderbook):
    def __init__(self):
        self.bids = Half_Orderbook("Bid") # ask bid half
        self.asks = Half_Orderbook("Ask") # ask book half
        self.tape = [] # record of trades executed
        self.quote_id = 0 # must start at 0, quote ID, unique to each quote accepted into book # TODO: look at using ISINs + a counter?
        self.lob_string = '' # string representation of the current LOB

# exchange, trades are conducted through this
class Exchange(Orderbook):
    def add_order(self, order):
        # add a quote and update the order book, return the qid
        order.qid = self.quote_id
        self.quote_id = order.qid + 1
        
        if order.order_type == "Bid": # TODO: use enums
            response = self.bids.add_to_book(order)
            best_price = self.bids.lob_anon[-1][0]
            self.bids.best_price = best_price
            self.bids.best_tid = self.bids.lob[best_price][2][0][2]
        else:
            response = self.asks.add_to_book(order)
            best_price = self.asks.lob_anon[0][0]
            self.asks.best_price = best_price
            self.asks.best_tid = self.asks.lob[best_price][2][0][2]
        
        return [order.qid, response]

    def delete_order(self, time, order):
        # delete an order from the book
        if order.order_type == "Bid": # TODO: use enums
            self.bids.delete_from_book(order)
            
            if self.bids.n_orders > 0:
                best_price = self.bids.lob_anon[-1][0]
                self.bids.best_price = best_price
                self.bids.best_tid = self.bids.lob[best_price][2][0][2]
            else:
                # empty book
                self.bids.best_price = None
                self.bids.best_tid = None
            cancel_record = {'type': 'Cancel', # TODO: use enums
                             'time': time,
                             'order': order}
            self.tape.append(cancel_record)
        elif order.order_type == "Ask": # TODO: use enums
            self.asks.delete_from_book(order)
            
            if self.asks.n_orders > 0:
                best_price = self.asks.lob_anon[0][0]
                self.asks.best_price = best_price
                self.asks.best_tid = self.asks.lob[best_price][2][0][2]
            else:
                # empty book
                self.asks.best_price = None
                self.asks.best_tid = None
            cancel_record = {'type': 'Cancel', # TODO: use enums
                             'time': time,
                             'order': order}
            self.tape.append(cancel_record)
        else: # neither bid nor ask, error
            util.log("Bad order when trying to delete quote from book", LOG_LEVEL.ERROR)           

    def process_order(self, time, order):
        order_price = order.price
        ticker = order.ticker
        trader = None
        price = None
        
        [qid, response] = self.add_order(order) # add to orders, and overwrite if required
        order.qid = qid
        util.log(f"Order {order} added to book", LOG_LEVEL.INFO)
        
        best_ask = self.asks.best_price
        best_ask_tid = self.asks.best_tid
        best_bid = self.bids.best_price
        best_bid_tid = self.bids.best_tid
        
        if order.order_type == "Bid": # TODO: enum
            if self.asks.n_orders > 0 and best_bid >= best_ask:
                # bid lifts the best ask
                util.log(f"Best bid hits best ask", LOG_LEVEL.INFO)
                trader = best_ask_tid
                price = best_ask # bid crossed ask, so ask price is used, and order fulfilled
                util.log(f"Trader {trader} hit best ask at price {order_price}", LOG_LEVEL.INFO)
                
                self.asks.delete_best()
                self.bids.delete_best()
        elif order.order_type == "Ask": # TODO: enum
            if self.bids.n_orders > 0 and best_ask <= best_bid:
                # ask hits the best bid
                util.log(f"Best ask hits best bid", LOG_LEVEL.INFO)
                trader = best_bid_tid
                price = best_bid # ask crossed bid, so bid price is used, and order fulfilled
                util.log(f"Trader {trader} hit best bid at price {order_price}", LOG_LEVEL.INFO)
                
                self.bids.delete_best()
                self.asks.delete_best()
        else:
            util.log("Neither bid nor ask when trying to process order", LOG_LEVEL.ERROR)
            
        if trader:
            transaction_record = {
                'type': 'Trade', #  TODO: use enums
                'time': time,
                'price': price,
                'party1': trader,
                'party2': order.tid,
                'ticker': ticker,
                'qty': order.quantity,
            }
            
            self.tape.append(transaction_record)
            
            return transaction_record
        else:
            return None

    def publish_lob(self, time, lob_file):
        # publish a snapshot of the current LOB, accessible to traders
        data = {
            'time': time,
            'qid': self.quote_id,
            'bids': {
                'best': self.bids.best_price,
                'worst': self.bids.worst_price,
                'n': self.bids.n_orders,
                'lob': self.bids.lob_anon
            },
            'asks': {
                'best': self.asks.best_price,
                'worst': self.asks.worst_price,
                'n': self.asks.n_orders,
                'lob': self.asks.lob_anon
            },
            'tape': self.tape
        }

        if lob_file is not None:
            lob_string = '[Bid: ,]'
            n_bids = len(self.bids.lob_anon)
            if n_bids > 0:
                lob_string += f'{n_bids},'
                for lob in self.bids.lob_anon:
                    lob_string += f'{lob[0]},{lob[1]},'
            else:
                lob_string += '0,'
            lob_string += '[Ask: ,]'
            
            n_asks = len(self.asks.lob_anon)
            if n_asks > 0:
                lob_string += f'{n_asks},'
                for lob in self.asks.lob_anon:
                    lob_string += f'{lob[0]},{lob[1]},'
            else:
                lob_string += '0,'
                
            # check if it is the same as previous lob string
            if lob_string != self.lob_string:
                lob_file.write('{%.2f}, {}\n'.format(time, lob_string))
        
        return data

# trader superclass
class Trader:
    def __init__(self, ttype, tid, balance, params, time):
        self.ttype = ttype # type of trader
        self.tid = tid # unique ID for trader
        self.balance = balance # current finance of trader
        self.params = params # parameters for trader-type or individual trader
        self.birth_time = time # time of trader creation
        self.trades_executed = [] # list of trades executed by this trader
        self.n_trades = 0 # total number of trades made by this trader
        self.current_orders = [] # current list of active orders
        self.lastquote = None # record of last quote
        self.profit_per_time = 0 # profit per unit time
        self.time_period = 60 # minimum time period for profit calculation
        self.n_quotes_live = 0 # number of live quotes on limit order book
    
    def __str__(self):
        return f"[TID: {self.tid} \
            Type: {self.ttype} \
            Balance:{self.balance} \
            Trades: {self.n_trades} \
            Profit/Time:{self.profit_per_time}]"
    
    def add_order(self, order):
        response= "Proceed"
        
        for o in self.current_orders:
            if o.ticker == order.ticker:
                # cancel previous order for same ticker
                response = "LOB_Cancel" # TODO: use enum
                break
            else:
                response = "Proceed" # TODO: use enum

        self.current_orders.append(order)
        util.log(f"Trader {self.tid} added order {order} with response {response}", LOG_LEVEL.INFO)
        
        return response
    
    def delete_order(self, order):
        if self.n_quotes_live > 0:
            for o in self.current_orders:
                if o.qid == order.qid:
                    self.current_orders.remove(o)
                    util.log(f"Trader {self.tid} deleted order {order}", LOG_LEVEL.INFO)
        else:
            util.log(f"Trader {self.tid} has no live quotes to delete", LOG_LEVEL.ERROR)
    
    def bookkeep(self, trade, order, time):
        out_string = ""
        for order in self.current_orders:
            out_string += str(order)
    
        self.trades_executed.append(trade)
        
        # calculate transacton price # TODO: currently just uses the trade price, but should fetch the actual commission fees
        transaction_price = trade['price']
        # calculate profit, ask orders usually make no profit
        
        current_order = [o for o in self.current_orders if o.ticker == trade['ticker']][0]
        
        if current_order.order_type == "Bid": # TODO: use enums
            profit = current_order.price - transaction_price
        else:
            profit = transaction_price - current_order.price
        # update balance
        self.balance += profit
        # update number of trades
        self.n_trades += 1
        # update profit/time
        self.profit_per_time = self.balance / (self.birth_time - time)
        
        if profit < 0.0:
            util.log(f"Trader {self.tid} has bankrupted from the trade {trade}", LOG_LEVEL.ERROR)

        util.log(f"{out_string} Profit: {profit}, Balance: {self.balance} Profit/Time: {str(self.profit_per_time)}",
                 LOG_LEVEL.INFO)
    
    # how does the market act given the data
    def get_order(self, data, ticker, time, lob):
        raise NotImplementedError("Subclasses must implement get_order() method")
    
    def profit_per_time_update(self, time, birth_time, total_profit):
        time_alive = time - birth_time
        if time_alive >= self.time_period:
            profit_per_time = total_profit / time_alive
        else:
            # not been alive long enough to calculate profit per time, divide by minute instead
            profit_per_time = total_profit / self.time_period
    
        return profit_per_time
    
    # how does the trader respond to the market
    def respond(self, time, lob, trade):
        # respond to the market given the current state of the market
        
        # trade subclasses with custom respond() must call this method
        self.profit_per_time = self.profit_per_time_update(time, self.birth_time, self.balance)
        
        return None
    
    # how does the trader mutate its data
    def mutate(self, time, lob, trade):
        # this will be overloaded by subclasses
        return None

class Trader_MovingAverageCrossover(Trader):
    def __init__(self, ttype, name, balance, parameters, time0):
        super().__init__(ttype, name, balance, parameters, time0)
        
    def get_order(self, data, ticker, time, lob):
        if len(self.current_orders) < 1:
            return None

        current_order = [o for o in self.current_orders if o.ticker == ticker][0]
        quote_price = current_order.price
        order_type = current_order.order_type
        
        action = "Hold"
        if order_type == "Bid": # TODO: use enums
            action = "Buy"
            if self.params[ticker]["short_ma"].iloc[-1] > self.params[ticker]["long_ma"].iloc[-1]: # TODO: check for error
                quote_price = data[0][0]['Close'].iloc[-1]
        else:
            action = "Sell"
            if self.params[ticker]["short_ma"].iloc[-1] < self.params[ticker]["long_ma"].iloc[-1]:
                quote_price = self.params[ticker]["short_ma"].iloc[-1]
        util.log(f"{self.ttype} (Moving Average Crossover) is {action}ing", LOG_LEVEL.INFO)
        
        order = Order(self.tid, 
                      order_type,
                      quote_price,
                      current_order.quantity,
                      time,
                      lob['qid'],
                      ticker)
        self.lastquote = order
        
        return order

class Trader_PriceChange(Trader):
    def __init__(self, ttype, name, balance, parameters, time0):
        super().__init__(ttype, name, balance, parameters, time0)
        
    def get_order(self, data, ticker, time, lob):
        if len(self.current_orders) < 1:
            return None
        
        current_order = [o for o in self.current_orders if o.ticker == ticker][0]
        
        quote_price = current_order.price
        order_type = current_order.order_type
        
        action = "Hold"
        if order_type == "Bid": # TODO: use enums
            action = "Buy"
            if self.params[ticker]["price_change"] > 0:
                quote_price = data[0][0]['Close'].iloc[-1]
        else:
            action = "Sell"
            if self.params[ticker]["price_change"] < 0:
                quote_price = data[0][0]['Close'].iloc[-1]
                #quote_price =  min(data[0][0]['Close'][-5:]) # TODO: use config for the window, should reflect in unpack_parameters
        util.log(f"{self.ttype} (Price Change) is {action}ing", LOG_LEVEL.INFO)
        
        order = Order(self.tid,
                      order_type,
                      quote_price,
                      current_order.quantity,
                      time,
                      lob['qid'],
                      ticker)  
        self.lastquote = order
        
        return order

class Trader_VolumeChange(Trader):
    def __init__(self, ttype, name, balance, parameters, time0):
        super().__init__(ttype, name, balance, parameters, time0)
        
    def get_order(self, data, ticker, time, lob):
        if len(self.current_orders) < 1:
            return None
        
        current_order = [o for o in self.current_orders if o.ticker == ticker][0]
        
        quote_price = current_order.price
        order_type = current_order.order_type
        
        action = "Hold"
        if order_type == "Bid": # TODO: use enums
            action = "Buy"
            if self.params[ticker]["volume_change"] > 0:
                quote_price = data[0][0]['Close'].iloc[-1]
        else:
            action = "Sell"
            if self.params[ticker]["volume_change"] < 0:
                quote_price = data[0][0]['Close'].iloc[-2]
                # quote_price = min(data[0][0]['Close'][-5:]) # TODO: use window param in config, should reflect in unpack_parameters
        util.log(f"{self.ttype} (Volume Change) is {action}ing", LOG_LEVEL.INFO)

        order = Order(self.tid,
                      order_type,
                      quote_price,
                      current_order.quantity,
                      time,
                      lob['qid'],
                      ticker)
        self.lastquote = order
        
        return order

class Trader_MeanReversion(Trader):
    def __init__(self, ttype, name, balance, parameters, time0):
        super().__init__(ttype, name, balance, parameters, time0)
        
    def get_order(self, data, ticker, time, lob):
        if len(self.current_orders) < 1:
            return None
        
        current_order = [o for o in self.current_orders if o.ticker == ticker][0]
        
        quote_price = current_order.price
        order_type = current_order.order_type
        
        action = "Hold"
        if order_type == "Bid": # TODO: use enums
            action = "Buy"
            quote_price = self.params[ticker]["rolling_mean"].iloc[-1]
        else:
            action = "Sell"
            quote_price = self.params[ticker]["rolling_mean"].iloc[-1]
        util.log(f"{self.ttype} (Mean Reversion) is {action}ing", LOG_LEVEL.INFO)
        
        order = Order(self.tid,
                      order_type,
                      quote_price,
                      current_order.quantity,
                      time,
                      lob['qid'],
                      ticker)
        self.lastquote = order
        
        return order

class Trader_MarketCap(Trader):
    def __init__(self, ttype, name, balance, parameters, time0):
        super().__init__(ttype, name, balance, parameters, time0)
        
    def get_order(self, data, ticker, time, lob):
        if len(self.current_orders) < 1:
            return None
        
        current_order = [o for o in self.current_orders if o.ticker == ticker][0]
        
        quote_price = current_order.price
        order_type = current_order.order_type
        
        action = "Hold"
        if order_type == "Bid": # TODO: use enums
            action = "Buy"
            
            # if current price is greater than historical average, try to buy at historical average
            if data[0][0]['Close'].iloc[-1] > self.params[ticker]["historical_average"]: # TODO: check for error
                quote_price = self.params[ticker]["historical_average"] # try to buy at historical average
        else:
            action = "Sell"
            
            # if current price is less than historical average, try to sell at historical average
            if data[0][0]['Close'].iloc[-1] < self.params[ticker]["historical_average"]: # 
                quote_price = self.params[ticker]["historical_average"]
        util.log(f"{self.ttype} (Market Cap) is {action}ing", LOG_LEVEL.INFO)
        
        order = Order(self.tid,
                      order_type,
                      quote_price,
                      current_order.quantity,
                      time,
                      lob['qid'],
                      ticker)   
        self.lastquote = order

        return order

class Trader_RSI(Trader):
    def __init__(self, ttype, name, balance, parameters, time0):
        super().__init__(ttype, name, balance, parameters, time0)
        
    def get_order(self, data, ticker, time, lob):
        if len(self.current_orders) < 1:
            return None
        
        current_order = [o for o in self.current_orders if o.ticker == ticker][0]
        
        quote_price = current_order.price
        order_type = current_order.order_type
        
        action = "Hold"
        if order_type == "Bid": # TODO: use enums
            action = "Buy"
            
            if self.params[ticker]["rsi"].iloc[-1] >= self.params[ticker]["overbought_threshold"]:
                quote_price *= 0.95 # TODO: use config for the percentage
        else:
            action = "Sell"

            if self.params[ticker]["rsi"].iloc[-1] <= self.params[ticker]["oversold_threshold"]:
                quote_price *= 1.05 # TODO: use config for the percentage
        util.log(f"{self.ttype} (RSI) is {action}ing", LOG_LEVEL.INFO)

        order = Order(self.tid,
                      order_type,
                      quote_price,
                      current_order.quantity,
                      time,
                      lob['qid'],
                      ticker)  
        self.lastquote = order
        
        return order

class Market_Session:
    def __init__(self, session_id, tickers_data, start_time, end_time, trader_spec, lob_file):
        self.session_id = session_id # unique session ID
        self.traders = {} # dictionary of traders
        self.tickers_data = tickers_data
        self.lob_file = lob_file # file to write lob data to
        self.exchange = Exchange() # create exchange object
        self.time = start_time # current time
        self.start_time = start_time # start time
        self.end_time = end_time # end time
        self.pending_orders = [] # list of pending orders
        self.kill_orders = [] # list of canceled orders
        self.stats = {} # dictionary of session stats
        self.spec = trader_spec # dictionary of session specifications
                 
    def populate_market(self, shuffle=False):
        def trader_type(type, name, parameters):
            balance = 0.0
            time0 = chrono.time()
            
            if type == 'MAC':
                return Trader_MovingAverageCrossover('MAC', name, balance, parameters, time0)
            elif type == 'PC':
                return Trader_PriceChange('PC', name, balance, parameters, time0)
            elif type == 'VC':
                return Trader_VolumeChange('VC', name, balance, parameters, time0)
            elif type == 'MR':
                return Trader_MeanReversion('MR', name, balance, parameters, time0)
            elif type == 'MC':
                return Trader_MarketCap('MC', name, balance, parameters, time0)
            elif type == 'RSI':
                return Trader_RSI('RSI', name, balance, parameters, time0)
            else:
                util.log(f"Unknown trader type {type}", LOG_LEVEL.ERROR)
                return None
            
        def shuffle_traders(ttype_char, n, traders):
            for swap in range(n):
                # swap each trader with another randomly selected trader
                t1 = (n - 1) - swap
                t2 = random.randint(0, t1)
                
                t1name = f'{ttype_char}{t1:02d}'
                t2name = f'{ttype_char}{t2:02d}'
                
                traders[t1name].tid = t2name
                traders[t2name].tid = t1name
                
                temp = traders[t1name]
                traders[t1name] = traders[t2name]
                traders[t2name] = temp

        # TODO: update if additional parameters are added
        # parameters are packed into a list and can be made of two parts(as of yet): history and info
        # history is a dataframe of OHLC data, whereas info is a dictionary of various data, including
        # company metadata, and data needed for the trader subclasses
        def unpack_parameters(parameters):
            ret_p = {}
            
            for p in parameters:
                history = p[0]
                info = p[1]
                
                # form dictionary by using symbol as key, for each trader
                ret_p[f'{info["symbol"]}'] = {}
                
                if ttype == 'MAC': # TODO: use enums
                    short_window = 20 # TODO: use config or tweak to EMA
                    long_window = short_window * 3 # TODO: implement rules e.g. doubling, tripling, algos.
                    
                    short_ma = history['Close'].rolling(window=short_window).mean()
                    long_ma= history['Close'].rolling(window=long_window).mean()
                    
                    # series data
                    ret_p[f'{info["symbol"]}']['short_ma'] = short_ma
                    ret_p[f'{info["symbol"]}']['long_ma'] = long_ma
                elif ttype == 'PC':
                    current_price = history['Close'].iloc[-1]
                    previous_price = history['Close'].iloc[-2] # TODO: if day interval, it'll be the previous close
                    
                    ret_p[f'{info["symbol"]}']['price_change'] = current_price - previous_price
                elif ttype == 'VC':
                    current_volume = history['Volume'].iloc[-1]
                    previous_volume = history['Volume'].iloc[-2]
                    
                    ret_p[f'{info["symbol"]}']['volume_change'] = current_volume - previous_volume
                elif ttype == 'MR':
                    rolling_mean = history['Close'].rolling(window=20).mean() # TODO: use config for window
                    
                    ret_p[f'{info["symbol"]}']['rolling_mean'] = rolling_mean
                elif ttype == 'MC':
                    ret_p[f'{info["symbol"]}']['historical_average'] = history['Close'].mean()
                    ret_p[f'{info["symbol"]}']['market_cap'] = info['marketCap']
                elif ttype == 'RSI':
                    ret_p[f'{info["symbol"]}']['overbought_threshold'] = 70 # default overbought threshold is 70 # TODO: use config
                    ret_p[f'{info["symbol"]}']['oversold_threshold'] = 30 # default oversold threshold is 30 # TODO: use config
                    
                    delta = history['Close'].diff(1)
                    rsi_period = 14 # default RSI period is 14 # TODO: use config
                    
                    gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period).mean()
        
                    # clean data
                    loss[loss == 0] = np.nan
                    
                    # calculate rs and rsi
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))

                    ret_p[f'{info["symbol"]}']['rsi'] = rsi

            return ret_p
        
        n_buyers = 0
        for bs in self.spec['buyers']:
            ttype = bs[0]
            name = f'B{n_buyers:02d}'
            
            if len(bs[1]) > 0: # parameters exist
                params = unpack_parameters(bs[1])
            else:
                params = None
            
            self.traders[name] = trader_type(ttype, name, params)
            n_buyers += 1
        
        if n_buyers < 1:
            util.log("No buyers specified", LOG_LEVEL.CRITICAL)
        
        if shuffle:
            shuffle_traders('B', n_buyers, self.traders)
        
        n_sellers = 0
        for ss in self.spec['sellers']:
            ttype = ss[0]
            name = f'S{n_sellers:02d}'

            if len(ss[1]) > 0: # parameters exist
                params = unpack_parameters(ss[1])
            else:
                params = None

            self.traders[name] = trader_type(ttype, name, params)
            n_sellers += 1
                
        if n_sellers < 1:
            util.log("No sellers specified", LOG_LEVEL.CRITICAL)
            
        if shuffle:
            shuffle_traders('S', n_sellers, self.traders)
        
        return {'n_buyers': n_buyers, 'n_sellers': n_sellers}
            
    def orders(self):    
        # get issuing times for traders for one ticker, with the range of the price(day low to day-high)
        # whether to shuffle the issuing times, and whether to fit the issuing times to the interval
        def get_issue_times(ticker_data, n_traders, range, shuffle, fit_to_interval):
            if n_traders < 1:
                util.log("No traders specified", LOG_LEVEL.ERROR)
                
            timestamps = ticker_data.index.to_list()
            
            if fit_to_interval:
                interval = range[1] - range[0]
                num_intervals = len(timestamps) // interval
                timestamps = timestamps[:num_intervals * interval]
            
            if shuffle:
                random.shuffle(timestamps)
            
            issue_times = timestamps[:n_traders]
            
            return issue_times
        
        def get_order_price(data, issue_time):
            high = data[1]['dayHigh']
            low = data[1]['dayLow']
            
            order_price = random.uniform(low, high) # introduce some randomness # TODO: use day high/low, could use targets instead
            
            return order_price

        n_buyers = self.stats['n_buyers']
        n_sellers = self.stats['n_sellers']
            
        cancellations = []

        if len(self.pending_orders) < 1:
            new_pending = []
            
            # choose a random stock from tickers_data and run trades with it # TODO: improve/refactor this
            data = random.choice(self.tickers_data)
            
            # generate issue times for buyers(demand)
            # TODO: generate issue times
            # buyers_issue_times = get_issue_times(tickers_data,
            #                             n_buyers,
            #                             [data[0]['Low'].iloc[-1], data[0]['High'].iloc[-1]],
            #                             True,
            #                             True)
            buyers_issue_times = [self.time for _ in range(n_buyers)] # TODO: cheat and use current time for now

            order_type = "Bid" # TODO: use enums
            for i in range(n_buyers):
                issue_time = self.time # use realtime # TODO: add issue times randomness
                name = f'B{i:02d}'
                order_price = get_order_price(data, issue_time)
                #max_quantity = traders[name].balance // order_price # add multishare trades
                #quantity = random.randint(1, max_quantity) # TODO: add float shares support
                ticker = data[1]['symbol']

                # sleep for a bit to simulate the time
                chrono.sleep(0.0001) # TODO: issue times should resolve the need for this
          
                order = Order(name, order_type, order_price, 1, issue_time, chrono.time(), ticker)
                new_pending.append(order)

            # generate issue times for sellers(supply)
            # TODO: generate issue times
            # sellers_issue_times = get_issue_times(tickers_data,
            #                             n_sellers,
            #                             [tickers_data['Low'].iloc[-1], tickers_data['High'].iloc[-1]],
            #                             True,
            #                             True)
            sellers_issue_times = [self.time for _ in range(n_sellers)] # TODO: cheat and use current time for now
            order_type = "Ask" # TODO: use enums
            for i in range(n_sellers):
                issue_time = self.time # use realtime # TODO: add issue times randomness
                name = f'S{i:02d}'
                order_price = get_order_price(data, issue_time)
                #quantity = random.randint(1, max_quantity) # add mutlishare trades
                ticker = ticker = data[1]['symbol']
                
                # sleep for a bit to simulate the time
                chrono.sleep(0.0001) # TODO: issue times should resolve the need for this
                
                current_time = chrono.time()
                order = Order(name, order_type, order_price, 1, issue_time, chrono.time(), ticker)
                new_pending.append(order)
        else:
            new_pending = []
            for order in self.pending_orders:
                if order.time < self.time:
                    name = order.tid
                    response = self.traders[name].add_order(order)
                        
                    # issue cancellation if order needs to be deleted
                    if response == "LOB_Cancel": # TODO: use enums
                        cancellations.append(order)
                else:
                    new_pending.append(order)
                    
        return [new_pending, cancellations]
                        
    def run(self, shuffle=True):       
        # populate market with traders
        self.stats = self.populate_market(shuffle)
        
        timestep = 1.0 / float(self.stats['n_buyers'] + self.stats['n_sellers'])
        
        duration = float(self.end_time - self.start_time)
                
        while self.time < self.end_time:
            time_left = (self.end_time - self.time) / duration
            util.log(f"Session ID: {self.session_id}," +
                f"Time: {self.time}," +
                f"Remaining Time: {time_left * 100}%", LOG_LEVEL.INFO)
        
            # Generate new orders
            [self.pending_orders, self.kill_orders] = self.orders()
            
            # if new orders are cancellations, kill them
            if len(self.kill_orders) > 0:
                for kill_order in self.kill_orders:
                    if self.traders[kill_order.tid].lastquote is not None:
                        self.exchange.delete_order(self.time, self.traders[kill_order.tid].lastquote)

            # attempt to choose existing trade to happen
            tid = random.choice(list(self.traders.keys()))#
            # NB: probably the only place in the code where .current_oreders[0] CAN be used #TODO: should still find another way
            attempt = self.traders[tid].current_orders[0] if len(self.traders[tid].current_orders) > 0 else None
            util.log(f"Trader {tid} attempting to act with order {attempt}", LOG_LEVEL.INFO)
            
            if attempt:
                ticker = attempt.ticker
                ticker_data = [data for data in self.tickers_data if data[1]['symbol'] == ticker]

                # clean data so that it only contains the data for the ticker
                order = self.traders[tid].get_order(ticker_data, 
                                                    ticker, 
                                                    self.time,
                                                    self.exchange.publish_lob(self.time, self.lob_file)) # TODO: dump file
                
                util.log(f"Trader {tid} acted with order {order}", LOG_LEVEL.INFO)
                
                if order:
                    current_order = [o for o in self.traders[tid].current_orders if o.ticker == ticker][0]
                    
                    if order.order_type == "Ask" and (current_order.order_type == "Bid" \
                        and (order.price < current_order.price)): # TODO: enums
                        util.log("Ask price less than bid price", LOG_LEVEL.ERROR)
                    
                    if order.order_type == "Bid" and (current_order.order_type == "Ask" \
                        and (order.price > current_order.price)):
                        util.log("Bid price greater than ask price", LOG_LEVEL.ERROR)
                        
                    # send orders to exchange
                    self.traders[tid].n_quotes_live = 1
                    trade = self.exchange.process_order(self.time, order)
                    if trade:
                        # trade occurred, bookkeeping needs to occur
                        self.traders[trade['party1']].bookkeep(trade, order, self.time)
                        self.traders[trade['party2']].bookkeep(trade, order, self.time)
                                                
                        # traders respond to the market
                        lob = self.exchange.publish_lob(self.time, None) # TODO: dump lob to file
                        any_record_frame = False
                        # make traders respond to market info, doesn't alter LOB,
                        # so doing this sequentially is fine
                        for t in self.traders:
                            response = self.traders[t].respond(self.time, lob, trade)
                            if response:
                                any_record_frame = True

                        if any_record_frame: # TODO: add dumping
                            util.log(f"Recorded frame at time {self.time}", LOG_LEVEL.INFO)

            self.time += timestep
            
            # session ends
            
            # TODO: add dumping