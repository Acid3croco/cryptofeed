'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
import time
from uuid import uuid4
from decimal import Decimal
from typing import Dict, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, BUY, L2_BOOK, POLONIEX_FUTURES, SELL, TRADES, PERPETUAL
from cryptofeed.exceptions import MissingSequenceNumber
from cryptofeed.exchanges.poloniex import Poloniex
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.types import OrderBook, Trade

LOG = logging.getLogger('feedhandler')


class PoloniexFutures(Poloniex):
    id = POLONIEX_FUTURES
    websocket_endpoints = [
        WebsocketEndpoint('wss://futures-apiws.poloniex.com/')
    ]
    rest_endpoints = [
        RestEndpoint(
            'https://futures-api.poloniex.com',
            routes=Routes(
                '/api/v1/contracts/active',
                authentication='/api/v1/bullet-public',
                l2book='/api/v1/level2/depth?symbol={}&depth=depth{}'))
    ]
    websocket_channels = {
        L2_BOOK: 'level2',
        TRADES: 'execution',
    }
    valid_depths = [5, 10, 20, 30, 50, 100]
    request_limit = 6

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = {'instrument_type': {}}
        for entry in data['data']:
            symbol = entry['symbol']
            base = entry['baseCurrency']
            quote = entry['quoteCurrency']
            s = Symbol(base, quote, PERPETUAL)
            ret[s.normalized] = symbol
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    def __reset(self):
        self._l2_book = {}
        self.seq_no = {}

    def _address(self):
        self._private = False

        data = self.http_sync.write(self.rest_endpoints[0].route(
            'authentication', sandbox=self.sandbox))
        data = json.loads(data)
        token = data['data']

        params = {
            'connectId': uuid4(),
            'token': token['token'],
            'acceptUserMessage': self._private
        }
        params = [f'{key}={value}' for key, value in params.items()]
        params = '&'.join(params)

        url = token['instanceServers'][0]['endpoint']
        url = f'{url}?{params}'

        return [url]

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            'data': {
                'makerUserId': '14262470',
                'symbol': 'BTCUSDTPERP',
                'sequence': 659294,
                'side': 'buy',
                'size': 1,
                'price': 19155.0,
                'takerOrderId': '6355476b9a81490007ac4c4a',
                'makerOrderId': '635547325562920007e49fc8',
                'takerUserId': '14330687',
                'tradeId': '6355476b244b7900011a4f5f',
                'ts': 1666533227264166601
            },
            'subject': 'match',
            'topic': '/contractMarket/execution:BTCUSDTPERP',
            'type': 'message'
        }
        """
        price = Decimal(msg['data']['price'])
        amount = Decimal(msg['data']['size'])
        # timestamp is in nanoseconds
        trade_timestamp = self.timestamp_normalize(
            msg['data']['ts']) / 1_000_000
        t = Trade(msg['data']['tradeId'],
                  self.exchange_symbol_to_std_symbol(msg['data']['symbol']),
                  SELL if msg['data']['side'] == 'sell' else BUY,
                  amount,
                  price,
                  trade_timestamp,
                  raw=msg)
        await self.callback(TRADES, t, timestamp)

    async def _snapshot(self, symbol: str):
        """
        {
            'code': '200000',
            'data': {
                'symbol':
                'EGLDUSDTPERP',
                'sequence':
                1666242556001,
                'asks': [[Decimal('55.6'), 157], [Decimal('55.61'), 150],
                         [Decimal('55.63'), 400], [Decimal('55.65'), 407],
                         [Decimal('55.66'), 237], [Decimal('55.82'), 2561],
                         [Decimal('55.92'), 815], [Decimal('56.03'), 607],
                         [Decimal('56.36'), 184], [Decimal('56.43'), 190]],
                'bids': [[Decimal('55.51'), 150], [Decimal('55.5'), 90],
                         [Decimal('55.48'), 100], [Decimal('55.46'), 157],
                         [Decimal('55.45'), 300], [Decimal('55.41'), 407],
                         [Decimal('55.4'), 237], [Decimal('55.27'), 1629],
                         [Decimal('55.14'), 1022], [Decimal('55.07'), 796]],
                'ts':
                1666546818854988483
            }
        }
        """
        ret = await self.http_conn.read(self.rest_endpoints[0].route(
            'l2book', self.sandbox).format(symbol, self.max_depth))
        res = json.loads(ret, parse_float=Decimal)
        data = res['data']

        symbol = self.exchange_symbol_to_std_symbol(symbol)
        self._l2_book[symbol] = OrderBook(self.id,
                                          symbol,
                                          max_depth=self.max_depth)
        self.seq_no[symbol] = data['sequence']
        self._l2_book[symbol].book.bids = {
            Decimal(price): Decimal(amount)
            for price, amount in data['bids']
        }
        self._l2_book[symbol].book.asks = {
            Decimal(price): Decimal(amount)
            for price, amount in data['asks']
        }
        await self.book_callback(L2_BOOK,
                                 self._l2_book[symbol],
                                 time.time(),
                                 raw=data,
                                 sequence_number=data['sequence'])

    def _check_update_id(self, symbol: str, msg: dict) -> Tuple[bool, bool]:
        if self._l2_book[symbol].sequence_number is None or msg[
                'sequence'] <= self.seq_no[symbol]:
            return True
        elif msg['sequence'] == self.seq_no[symbol] + 1:
            self.seq_no[symbol] = msg['sequence']
            return False
        else:
            self._reset()
            LOG.warning("%s: Missing book update detected, resetting book",
                        self.id)
            return True

    async def _book(self, msg: dict, timestamp: float):
        """
        {
            'data': {
                'sequence': 1666242551380,
                'change': '55.29,sell,0',
                'timestamp': 1666544751415
            },
            'subject': 'level2',
            'topic': '/contractMarket/level2:EGLDUSDTPERP',
            'type': 'message'
        }
        """
        exchange_symbol = msg['topic'].split(':')[-1]
        symbol = self.exchange_symbol_to_std_symbol(exchange_symbol)
        if symbol not in self._l2_book:
            await self._snapshot(exchange_symbol)

        data = msg['data']
        skip_update = self._check_update_id(symbol, data)
        if skip_update:
            return

        ts = self.timestamp_normalize(data['timestamp'])
        delta = {BID: [], ASK: []}

        price, side, amount = data['change'].split(',')
        price = Decimal(price)
        amount = Decimal(amount)
        side = BID if side == 'buy' else ASK

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
                                 timestamp=ts,
                                 delta=delta,
                                 sequence_number=data['sequence'])

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        event = msg.get('type')
        if event == 'error':
            LOG.error("%s: Error from exchange: %s", self.id, msg)
            return
        elif event == 'welcome':
            return
        elif event == 'ack':
            return
        elif event == 'subscribe':
            return

        channel = msg.get('subject')
        if channel == 'match':
            await self._trade(msg, timestamp)
        elif channel == 'level2':
            await self._book(msg, timestamp)
        else:
            LOG.warning('%s: Invalid message type %s', self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        for chan, symbols in self.subscription.items():
            for sym in symbols:
                d = {
                    'id': int(time.time() * 1000),
                    'type': 'subscribe',
                    'topic': f'/contractMarket/{chan}:{sym}',
                    'privateChannel': False,
                    'response': True
                }
                await conn.write(json.dumps(d))
