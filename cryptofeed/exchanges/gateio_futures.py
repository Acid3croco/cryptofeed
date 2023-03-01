'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import time
import asyncio
import logging
from decimal import Decimal
from collections import defaultdict
from typing import Dict, Tuple, Iterable

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, CANDLES, GATEIO_FUTURES, L2_BOOK, TICKER, TRADES, BUY, SELL, PERPETUAL, FUNDING
from cryptofeed.exchanges import Gateio
from cryptofeed.symbols import Symbol
from cryptofeed.types import OrderBook, Trade, Ticker, Funding

LOG = logging.getLogger('feedhandler')


class GateioFutures(Gateio):
    id = GATEIO_FUTURES
    websocket_endpoints = [
        WebsocketEndpoint('wss://fx-ws.gateio.ws/v4/ws/usdt',
                          instrument_filter=('QUOTE', ('USDT',)),
                          options={'compression': None})
    ]
    rest_endpoints = [
        RestEndpoint(
            'https://api.gateio.ws',
            routes=Routes(
                '/api/v4/futures/usdt/contracts',
                l2book='/api/v4/futures/usdt/order_book?contract={}&limit=100&with_id=true',
                funding='/api/v4/futures/usdt/funding_rate?contract={}'
            ),
            instrument_filter=('QUOTE', ('USDT',)))
    ]

    valid_candle_intervals = {
        '10s', '1m', '5m', '15m', '30m', '1h', '4h', '8h', '1d', '7d'
    }
    websocket_channels = {
        FUNDING: 'funding',
        L2_BOOK: 'futures.order_book_update',
        TRADES: 'futures.trades',
        TICKER: 'futures.tickers',
        CANDLES: 'futures.candlesticks'
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._funding_cache = defaultdict(str)

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = {'instrument_type': {}}

        for entry in data:
            if entry["in_delisting"] is True:
                continue
            base, quote = entry['name'].split('_')
            s = Symbol(base, quote, type=PERPETUAL)
            ret[s.normalized] = entry['name']
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {
            "time": 1615366379,
            "channel": "futures.book_ticker",
            "event": "update",
            "error": null,
            "result": {
                "t": 1615366379123,
                "u": 2517661076,
                "s": "BTC_USD",
                "b": "54696.6",
                "B": 37000,
                "a": "54696.7",
                "A": 47061
            }
        }
        """
        t = Ticker(self.id,
                   self.exchange_symbol_to_std_symbol(
                       msg['result']['s']),
                   Decimal(msg['result']['b']),
                   Decimal(msg['result']['a']),
                   float(msg['result']['t'] / 1000),
                   raw=msg)
        await self.callback(TICKER, t, timestamp)

    async def _trades(self, msg: dict, timestamp: float):
        """
        a = {
            'time':
            1667651304,
            'channel':
            'futures.trades',
            'event':
            'update',
            'result': [{
                'contract': 'BTC_USDT',
                'create_time': 1667651304,
                'create_time_ms': 1667651304562,
                'id': 142863637,
                'price': '21307.8',
                'size': -878
            }]
        }
        """
        for trade in msg['result']:
            t = Trade(self.id,
                      self.exchange_symbol_to_std_symbol(trade['contract']),
                      SELL if trade['size'] < 0 else BUY,
                      Decimal(trade['size']),
                      Decimal(trade['price']),
                      float(trade['create_time_ms']) / 1000,
                      id=str(trade['id']),
                      raw=msg)
            await self.callback(TRADES, t, timestamp)

    async def _snapshot(self, symbol: str):
        """
        {
            "id": 2679059670,
            "current": Decimal('1667651105.381'),
            "update": Decimal('1667651105.381'),
            "asks": [{'s' size:, 's': 'price'}, {...}, {...}],
            "bids": [{'s' size:, 's': 'price'}, {...}, {...}]
        }
        """
        ret = await self.http_conn.read(self.rest_endpoints[0].route(
            'l2book', self.sandbox).format(symbol))
        data = json.loads(ret, parse_float=Decimal)

        symbol = self.exchange_symbol_to_std_symbol(symbol)
        self._l2_book[symbol] = OrderBook(self.id,
                                          symbol,
                                          max_depth=self.max_depth)
        self.last_update_id[symbol] = data['id']
        self._l2_book[symbol].book.bids = {
            Decimal(level['p']): Decimal(level['s'])
            for level in data['bids']
        }
        self._l2_book[symbol].book.asks = {
            Decimal(level['p']): Decimal(level['s'])
            for level in data['asks']
        }
        await self.book_callback(L2_BOOK,
                                 self._l2_book[symbol],
                                 time.time(),
                                 raw=data,
                                 sequence_number=data['id'])

    async def _process_l2_book(self, msg: dict, timestamp: float):
        """
        {
            'time': 1667651031,
            'channel': 'futures.order_book_update',
            'event': 'update',
            'result': {
                't': 1667651031501,
                's': 'BTC_USDT',
                'U': 20778286361,
                'u': 20778286365,
                'b': [{
                    'p': '21315.7',
                    's': 1322
                }, {
                    'p': '21307.9',
                    's': 0
                }, {
                    'p': '21293.6',
                    's': 0
                }],
                'a': [{
                    'p': '21339',
                    's': 19691
                }]
            }
        }
        """
        symbol = self.exchange_symbol_to_std_symbol(msg['result']['s'])
        if symbol not in self._l2_book:
            await self._snapshot(msg['result']['s'])

        skip_update = self._check_update_id(symbol, msg['result'])
        if skip_update:
            return

        ts = msg['result']['t'] / 1000
        delta = {BID: [], ASK: []}

        for s, side in (('b', BID), ('a', ASK)):
            for update in msg['result'][s]:
                price = Decimal(update['p'])
                amount = Decimal(update['s'])

                if amount == 0:
                    if price in self._l2_book[symbol].book[side]:
                        del self._l2_book[symbol].book[side][price]
                        delta[side].append((price, amount))
                else:
                    self._l2_book[symbol].book[side][price] = amount
                    delta[side].append((price, amount))

        await self.book_callback(L2_BOOK,
                                 self._l2_book[symbol],
                                 timestamp,
                                 delta=delta,
                                 timestamp=ts,
                                 raw=msg)

    async def _funding(self, pairs: Iterable):
        """
        [
            {
                "name": "BTC_USDT",
                "type": "direct",
                "quanto_multiplier": "0.0001",
                "ref_discount_rate": "0",
                "order_price_deviate": "0.5",
                "maintenance_rate": "0.005",
                "mark_type": "index",
                "last_price": "38026",
                "mark_price": "37985.6",
                "index_price": "37954.92",
                "funding_rate_indicative": "0.000219",
                "mark_price_round": "0.01",
                "funding_offset": 0,
                "in_delisting": false,
                "risk_limit_base": "1000000",
                "interest_rate": "0.0003",
                "order_price_round": "0.1",
                "order_size_min": 1,
                "ref_rebate_rate": "0.2",
                "funding_interval": 28800,
                "risk_limit_step": "1000000",
                "leverage_min": "1",
                "leverage_max": "100",
                "risk_limit_max": "8000000",
                "maker_fee_rate": "-0.00025",
                "taker_fee_rate": "0.00075",
                "funding_rate": "0.002053",
                "order_size_max": 1000000,
                "funding_next_apply": 1610035200,
                "short_users": 977,
                "config_change_time": 1609899548,
                "trade_size": 28530850594,
                "position_size": 5223816,
                "long_users": 455,
                "funding_impact_value": "60000",
                "orders_limit": 50,
                "trade_id": 10851092,
                "orderbook_id": 2129638396
            }
        ]
        """
        while True:
            res = await self.http_conn.read(self.rest_endpoints[0].route('instruments', sandbox=self.sandbox))
            all_data = json.loads(res, parse_float=Decimal)

            for data in all_data:
                # since we fetch contracts, skip in_delisting contracts
                if data['name'] not in pairs:
                    continue
                received = time.time()

                pair = self.exchange_symbol_to_std_symbol(data['name'])
                rate = Decimal(data['funding_rate'])
                predicted_rate = Decimal(data['funding_rate_indicative'])
                rate_cached, predicted_rate_cached = self._funding_cache.get(pair, (None, None))

                if rate == rate_cached and predicted_rate == predicted_rate_cached:
                    continue
                else:
                    self._funding_cache[pair] = (rate, predicted_rate)

                f = Funding(
                    self.id,
                    pair,
                    Decimal(data['mark_price']),
                    rate,
                    data['funding_next_apply'],
                    received,
                    predicted_rate=predicted_rate,
                    raw=data
                )
                await self.callback(FUNDING, f, received)
                await asyncio.sleep(0.1)
            await asyncio.sleep(60)

    async def subscribe(self, conn: AsyncConnection):
        self._reset()
        for chan in self.subscription:
            symbols = self.subscription[chan]

            if chan == FUNDING:
                asyncio.create_task(self._funding(symbols))
                continue

            nchan = self.exchange_channel_to_std(chan)
            if nchan in {L2_BOOK, CANDLES}:
                for symbol in symbols:
                    await conn.write(json.dumps(
                        {
                            "time": int(time.time()),
                            "channel": chan,
                            "event": 'subscribe',
                            "payload": [symbol, '100ms'] if nchan == L2_BOOK else [self.candle_interval, symbol],
                        }
                    ))
            else:
                await conn.write(json.dumps(
                    {
                        "time": int(time.time()),
                        "channel": chan,
                        "event": 'subscribe',
                        "payload": symbols,
                    }
                ))
