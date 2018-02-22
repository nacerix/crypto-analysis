# encoding: utf-8

import pandas as pd
import ccxt
import pickle

import plotly.offline as py
import plotly.graph_objs as go
import plotly.figure_factory as ff
#py.init_notebook_mode(connected=True)

import time

__debug = True 

def timeit(method):
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

__exchanges = {}

@timeit
def get_exchanges(exchanges=None):
    ''' Load available exchange '''
    if not __exchanges:
        exchanges = exchanges or ccxt.exchanges
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
                # exchange not available
                pass
    return __exchanges.copy()

# get the current best price
@timeit
def get_current_best_price(exchange, symbols):
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
    '''Download and cache dataseries'''
    exchanges = get_exchanges(exchanges)
    # get a list of ohlcv candles - each ohlcv candle is a list of [ timestamp, open, high, low, close, volume ]
    # use close price from each ohlcv candle
    return dict([(exchange.id, get_data_from_exchange(symbols, exchange, env)) for exchange in exchanges.values()])

@timeit
def get_data_from_exchange(symbols, exchange, env={}):
    df = None
    for symbol in symbols:
        df_symbol = get_symbol_data_from_exchange(symbol, exchange, env)
        df = df and df.join(df_symbol, how='outer') or pd.DataFrame(df_symbol)
    return df

@timeit
def get_symbol_data_from_exchange(symbol, exchange, env):
    '''Download and cache Quandl dataseries'''
    #TODO: provide an appropriate default for cache_dir
    cache_path = env.get('cache_dir','')+ '/' + '{}-{}.pkl'.format(exchange.id, symbol).replace('/','-')
    timeframe = env.get('timeframe','1d')
    try:
        f = open(cache_path, 'rb')
        df = pickle.load(f)
        print('Loaded {}-{} from cache'.format(exchange.id, symbol))
    except (OSError, IOError):
        print('Downloading {}-{} from {}'.format(exchange.id, symbol, exchange.urls.get('api' ,'')))
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe) # since
        d = dict([(candle[0], candle[4]) for candle in ohlcv]) #(timestamp, close)
        df = pd.Series(list(d.values()), index=pd.to_datetime(list(d.keys()), unit='ms')) # convert epoch timestamp
        df.to_pickle(cache_path)
        print('Cached {}-{} at {}'.format(exchange.id, symbol, cache_path))
    return df

@timeit
def merge_dfs_on_column(dataframes, labels, col):
    '''Merge a single column of each dataframe into a new combined dataframe'''
    series_dict = {}
    for index in range(len(dataframes)):
        series_dict[labels[index]] = dataframes[index][col]
        
    return pd.DataFrame(series_dict)

@timeit
def simple_scatter(x, y):
    btc_trace = go.Scatter(x, y)
    py.iplot([btc_trace])

@timeit
def df_scatter(df, title, seperate_y_axis=False, y_axis_label='', scale='linear', initial_hide=False):
    '''Generate a scatter plot of the entire dataframe'''
    label_arr = list(df)
    series_arr = list(map(lambda col: df[col], label_arr))
    
    layout = go.Layout(
        title=title,
        legend=dict(orientation="h"),
        xaxis=dict(type='date'),
        yaxis=dict(
            title=y_axis_label,
            showticklabels= not seperate_y_axis,
            type=scale
        )
    )
    
    y_axis_config = dict(
        overlaying='y',
        showticklabels=False,
        type=scale )
    
    visibility = 'visible'
    if initial_hide:
        visibility = 'legendonly'
        
    # Form Trace For Each Series
    trace_arr = []
    for index, series in enumerate(series_arr):
        trace = go.Scatter(
            x=series.index, 
            y=series, 
            name=label_arr[index],
            visible=visibility
        )
        
        # Add seperate axis for the series
        if seperate_y_axis:
            trace['yaxis'] = 'y{}'.format(index + 1)
            layout['yaxis{}'.format(index + 1)] = y_axis_config    
        trace_arr.append(trace)

    fig = go.Figure(data=trace_arr, layout=layout)
    py.iplot(fig)

@timeit
def correlation_heatmap(df, title, absolute_bounds=True):
    '''Plot a correlation heatmap for the entire dataframe'''
    heatmap = go.Heatmap(
        z=df.corr(method='pearson').as_matrix(),
        x=df.columns,
        y=df.columns,
        colorbar=dict(title='Pearson Coefficient'),
    )
    
    layout = go.Layout(title=title)
    
    if absolute_bounds:
        heatmap['zmax'] = 1.0
        heatmap['zmin'] = -1.0
        
    fig = go.Figure(data=[heatmap], layout=layout)
    py.iplot(fig)

if __name__ == "__main__":
    cache_dir="/Users/nacer/Documents/workspace/crypto-analysis/data"
    # fetch bitcoin exchange rate from binance & coinmarketcap exchanges and plot them
    exchanges = [ccxt.binance().id]#, ccxt.coinmarketcap]
    res = get_price_data(['BTC/USDT'], exchanges, env={'cache_dir':cache_dir})
    df = res[ccxt.binance().id]
    #df_scatter(df, title="Bitcoin Prices USD")
    print(df.head())
    #symbols = [symbol for exch in exchanges for symbol in exch.symbols if symbol.split('/')[1].find('USD')!= -1]

    # fetch bitcoin exchange rate from all ccxt exchanges available and plot them
    # calculate and plot an average price

    # altcoins = ['ETH','LTC','XRP','ETC','STR','DASH','SC','XMR','XEM'] -> BTC
    # fetch bitcoin, litecoin, ripple exchange rates from all ccxt exchanges available and plot them
    # fetch bitcoin, litecoin, ripple exchange rates from binance & coinmarketcap exchanges and plot them

    # Calculate the pearson correlation coefficients for altcoins in 2016/2017 and plot heatmap