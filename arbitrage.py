# -*- coding: utf-8 -*-
'''Fetch cryptocurrencies prices from multiple exchanges.

The objective of this module is to provide a simple to use and unified API
to retrieve information about cryptocurrencies marketplaces around the web.

It also contains some plotting functions that makes it easy to plot different
aspects of the collected data with a one function call.

Examples:
TODO: module examples
'''

import pandas as pd
import ccxt
from ccxt.base.errors import NotSupported, AuthenticationError, ExchangeError
import pickle

import time

__debug = False 

def timeit(method):
    '''Decorator function to help identified bottleneck in the code'''
    def timed(*args, **kw):
        print("Start timing {} ...".format(method.__name__))
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('%r  %2.2f ms' %(method.__name__, (te - ts) * 1000))
        return result
    if not __debug:
        return method
    return timed

__debug_print = lambda message: __debug and print(message)

__exchanges = {}

@timeit
def get_exchanges(exchanges=None):
    '''Load a list of exchanges to get information from.

    Args:
        exchanges (list): List of exchanges' names to load. An empty list or None load all supported exchanges

    Returns:
        dict: returns a list of objects representing all loaded exchanges or an empty dict if none were loaded.

    Examples:
        - loading all supported exchanges:
        $ res = get_exchanges(None)
        $ res = get_exchanges([])

        - loading specific exchanges:
        $ res = get_exchanges(['binance', 'coinmarketcap'])
    '''
    to_str = lambda x: type(x) == str and x or x.id
    exchanges = exchanges or ccxt.exchanges
    exchanges = [exchange for exchange in exchanges if to_str(exchange) not in __exchanges]     
    for exchange_id in exchanges:
        try:
            if type(exchange_id) == str:
                exchange = getattr(ccxt, exchange_id)()
            else:
                exchange = exchange_id
            __exchanges[exchange.id] = exchange
            # Load markets
            exchange.load_markets()
        except:
            # exchange not available, just skip it
            pass
    return __exchanges.copy()

# get the current best price
@timeit
def get_current_best_price(exchange, symbols):
    '''Provides the best bid/ask/spead for a list of symbols on a given exchange.

    Args:
        exchange (object): exchange to get information from.
        symbols (list): list of symbols for which to get the price information.

    Returns:
        dict: returns bid/ask/spread for each symbol.

    Examples:
        $ binance = get_exchange(['binance'])['binance']
        $ res = get_current_best_price(binance, ['BTC/USDT', 'LTC/BTC'])
        $ res
    '''
    res = {}
    for symbol in symbols:
        sym = {}
        orderbook = exchange.fetch_order_book(symbol)
        sym["bid"] = orderbook['bids'][0][0] if len (orderbook['bids']) > 0 else None
        sym["ask"] = orderbook['asks'][0][0] if len (orderbook['asks']) > 0 else None
        sym["spread"] = (sym["ask"] - sym["bid"]) if (sym["ask"] and sym["ask"]) else None
        res[symbol] = sym
    return res 

@timeit
def get_arbitrage_symbols(exchanges=None):
    '''Provides a list of cryptocurrencies traded in at list two different exchanges.

    Args:
        exchanges (object): List of exchanges to get information from.
        symbols (list): 

    Returns:
        list: symbols traded on at least two exchanges.

    Examples:
        $ res = get_exchange(['binance', 'coinmarketcap'])
        $ res
    '''
    # get all symbol from all exchanges available
    exchanges = exchanges is None and get_exchanges() or exchanges
    all_symbols = [symbol for exchange in exchanges for symbol in exchange.symbols]

    # get all unique symbols
    unique_symbols = list(set(all_symbols))

    if len(exchanges) > 1:
        # filter out symbols that are not present on at least two exchanges
        arbitrable_symbols = sorted([symbol for symbol in unique_symbols if all_symbols.count(symbol) > 1])
    else:
        arbitrable_symbols = unique_symbols
    return arbitrable_symbols

@timeit
def get_price_data(symbols, exchanges=None, env={}):
    '''Download and cache data from exchanges.

    Args:
        symbols (list): cryptocurrencies to get information about.
        exchanges (object): List of exchanges to get information from.
        env (dict): additional parameters provided as property/value pairs.

    Returns:
        dict: per exchange dataframes with all currencies prices.

    Examples:
        # for bitcoin and litecoin daily prices from binance and coinmarketcap 
        $ res = get_price_data(['BTC/USDT','LTC/BTC'], ['binance', 'coinmarketcap'], env={'timeframe':'1d'})
        $ res

        # for bitcoin and litecoin weekly prices from binance and coinmarketcap 
        $ res = get_price_data(['BTC/USDT','LTC/BTC'], ['binance', 'coinmarketcap'], env={'timeframe':'1w'})
        $ res
    '''
    exchanges = get_exchanges(exchanges)
    # get a list of ohlcv candles - each ohlcv candle is a list of [ timestamp, open, high, low, close, volume ]
    # use close price from each ohlcv candle
    return dict([(exchange.id, __get_data_from_exchange(symbols, exchange, env)) for exchange in exchanges.values()])

@timeit
def __get_data_from_exchange(symbols, exchange, env={}):
    df = pd.DataFrame({})
    for symbol in symbols:
        df_symbol = __get_symbol_data_from_exchange(symbol, exchange, env)
        if df_symbol is not None:
            df = df.join(df_symbol, how='outer')
    return df

@timeit
def __get_symbol_data_from_exchange(symbol, exchange, env):
    '''Download and cache Quandl dataseries'''
    #TODO: provide an appropriate default for cache_dir
    cache_path = env.get('cache_dir','')+ '/' + '{}-{}.pkl'.format(exchange.id, symbol).replace('/','-')
    timeframe = env.get('timeframe','1d')
    try:
        f = open(cache_path, 'rb')
        df = pickle.load(f)
        __debug_print('Loaded {}-{} from cache'.format(exchange.id, symbol))
    except (OSError, IOError):
        try:
            __debug_print('Downloading {}-{} from {}'.format(exchange.id, symbol, exchange.urls.get('api' ,'')))
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe) # since
            d = dict([(candle[0], candle[-2]) for candle in ohlcv]) #(timestamp, close)
            df = pd.Series(list(d.values()), index=pd.to_datetime(list(d.keys()), unit='ms')) # convert epoch timestamp
            df.name = symbol
            df.to_pickle(cache_path)
            __debug_print('Cached {}-{} at {}'.format(exchange.id, symbol, cache_path))
        except (NotSupported, AuthenticationError, ExchangeError) as e:
            __debug_print('{}: {}'.format(exchange.id, str(e)))
            df = None
    return df

@timeit
def __merge_dfs_on_column(dataframes, labels, col):
    '''Merge a single column of each dataframe into a new combined dataframe'''
    series_dict = {}
    for index in range(len(dataframes)):
        series_dict[labels[index]] = dataframes[index][col]
        
    return pd.DataFrame(series_dict)