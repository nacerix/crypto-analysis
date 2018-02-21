# encoding: utf-8

import pandas as pd
import ccxt
import pickle

import plotly.offline as py
import plotly.graph_objs as go
import plotly.figure_factory as ff
#py.init_notebook_mode(connected=True)

__exchanges = {}

def get_exchanges():
    ''' Load available exchange '''
    if not __exchanges:
        for exchange_id in ccxt.exchanges:
            try:
                exchange = getattr(ccxt, exchange_id)()
                __exchanges[exchange.id] = exchange
                # Load markets
                exchange.load_markets()
            except:
                # exchange not available
                pass
    return __exchanges.copy()

# get the current best price
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

def get_arbitrage_symbols():
    # get all symbol from all exchanges available
    all_symbols = [symbol for exchange in get_exchanges() for symbol in exchange.symbols]

    # get all unique symbols
    unique_symbols = list(set(all_symbols))

    # filter out symbols that are not present on at least two exchanges
    arbitrable_symbols = sorted([symbol for symbol in unique_symbols if all_symbols.count(symbol) > 1])
    return arbitrable_symbols

def get_price_data(symbols, timeframe, env):
    '''Download and cache dataseries'''
    exchanges = get_exchanges()
    cache_dir = env is None and "" or env.get("cache_dir", "")
    # get a list of ohlcv candles - each ohlcv candle is a list of [ timestamp, open, high, low, close, volume ]
    # use close price from each ohlcv candle
    return dict([(_id, __get_data_from_exchange(_id, symbols, cache_dir, timeframe)) for _id in exchanges])

def __get_data_from_exchange(exchange, symbols, cache_dir, timeframe):
    df = None
    for symbol in symbols:
        df_symbol = __get_symbol_data_from_exchange(exchange, symbol, cache_dir, timeframe)
        df = df.join(df_symbol, how='outer')
    return df

def __get_symbol_data_from_exchange(exchange, symbol, cache_dir, timeframe):
    '''Download and cache Quandl dataseries'''
    cache_path = cache_dir+ '/' '{}-{}.pkl'.format(exchange.id, symbol).replace('/','-')
    try:
        f = open(cache_path, 'rb')
        df = pickle.load(f)
        print('Loaded {}-{} from cache'.format(exchange.id, symbol))
    except (OSError, IOError):
        print('Downloading {}-{} from {}'.format(exchange.id, symbol, exchange.url))
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe) # since
        d = dict([(candle[0], candle[4]) for candle in ohlcv]) #(timestamp, close)
        df = pd.Series(d, index=pd.to_datetime(d.keys(), unit='ms')) # convert epoch timestamp
        df.to_pickle(cache_path)
        print('Cached {}-{} at {}'.format(exchange.id, symbol, cache_path))
    return df

def merge_dfs_on_column(dataframes, labels, col):
    '''Merge a single column of each dataframe into a new combined dataframe'''
    series_dict = {}
    for index in range(len(dataframes)):
        series_dict[labels[index]] = dataframes[index][col]
        
    return pd.DataFrame(series_dict)

def simple_scatter(x, y):
    btc_trace = go.Scatter(x, y)
    py.iplot([btc_trace])

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
    print("Finished")