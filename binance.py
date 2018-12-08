import os
import time
import asyncio
import aiohttp
import requests
import hmac
import urllib
import base64
import hashlib
from requests.auth import HTTPBasicAuth
from datetime import datetime
from logger import logger
from aiohttp import web
from threading import Thread


os.environ["TZ"] = "Asia/Kolkata"


class Mmaker(object):

    def __init__(self):
        self.app = web.Application()
        self.order_url = "https://api.binance.com/api/v3/order"
        self.candle_url = "https://api.binance.com/api/v1/klines"
        self.api_key = "aWhm2y2HvQUYieRf6G5ywK7ldSR5I6Xy00Ll6iNP3malwOQ5GIc1UkCpUvNVauCU"
        self.secret = "kNqAwffrrypZB7tsHnYx1u8iGm0w6w6pOYNgfVagi9mm6y6xQtq71IoTQadwyg51"
        self.price = 0
        self.side = None
        self.symbol = None
        self.increment = 0
        self.wait = False
        self.decrement = 0
        self.qty = 0
        self.cycle = 0
        self.setup_app()
        t = Thread(target=self.setup_poller)
        t.start()
        logger.info("Binance mmaker running")
        try:
            web.run_app(self.app, host="0.0.0.0", port=7001)
        except:
            pass
        finally:
            self.app.shutdown()

    def setup_poller(self):
        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        asyncio.ensure_future(self.poller())
        try:
            self.event_loop.run_forever()
        except:
            pass
        finally:
            self.event_loop.close()

    async def poller(self):
        while True:
            if self.symbol is None:
                await asyncio.sleep(5)
                continue
            if self.wait is True:
                self.wait_for_entry()
                await asyncio.sleep(60)
                continue
            self.fetch_candle()
            await asyncio.sleep(60)

    def wait_for_entry(self):
        symbol = self.symbol
        symbol = self.get_symbol(symbol)
        body = {"symbol": symbol,
                "interval": "1m"
                }
        try:
            resp = requests.get(self.candle_url, params=body)
        except Exception as e:
            logger.info("Error fetching candles %s" % e)
            return
        data = resp.json()
        data = data[-1]
        close = data[4]
        close = float(close)
        close = round(close, 5)
        _open = data[1]
        _open = float(_open)
        _open = round(_open, 5)
        pdata = resp.json()[-2]
        pclose = pdata[4]
        pclose = float(pclose)
        pclose = round(pclose, 5)
        popen = pdata[1]
        popen = float(popen)
        popen = float(popen, 5)
        if popen >= pclose:
            candle1 = "RED"
        else:
            candle1 = "GREEN"
        if _open >= close:
            candle2 = "RED"
        else:
            candle2 = "GREEN"
        data = {"symbol": self.symbol,
                "side": self.side,
                "qty": self.qty,
                "increment": self.increment,
                "decrement": self.decrement
                }
        if self.side == "BUY" and candle1 == "GREEN" and candle2 == "GREEN":
            self.wait = False
            self.symbol = None
            asyncio.ensure_future(self.recycle_order(data))
        if self.side == "SELL" and candle1 == "RED" and candle2 == "RED":
            self.wait = False
            self.symbol = None
            asyncio.ensure_future(self.recycle_order(data))

    def fetch_candle(self):
        symbol = self.symbol
        symbol = self.get_symbol(symbol)
        body = {"symbol": symbol,
                "interval": "1m"
                }
        try:
            resp = requests.get(self.candle_url, params=body)
        except Exception as e:
            logger.info("Error fetching candles %s" % e)
            return
        data = resp.json()
        data = data[-1]
        close = data[4]
        close = float(close)
        close = round(close, 5)
        logger.info("Cycle %s" % self.cycle)
        logger.info("Entry %s Decrement %s Increment %s Close %s" % (self.price, self.decrement, self.increment, close))
        if self.side == "BUY":
            if close < self.price - self.decrement:
                logger.info("Exiting")
                self.exit("SELL", True)
            if close > self.price + self.increment:
                logger.info("Increasing price")
                self.exit("SELL")
        else:
            if close > self.price + self.decrement:
                logger.info("Exiting")
                self.exit("BUY", True)
            if close < self.price - self.increment:
                logger.info("Increasing price")
                self.exit("BUY")

    def setup_app(self):
        self.app.router.add_routes([web.post("/handle_post", self.handle_post)])

    async def handle_post(self, request):
        data = await request.json()
        logger.info("Handle post %s" % data)
        self.cycle = 0
        body, status_code = self.make_order(data)
        return web.json_response(body, status=status_code)

    def get_header(self):
        headers = {"X-MBX-APIKEY": self.api_key}
        return headers

    def get_symbol(self, symbol):
        symbol = symbol.replace("/", "")
        return symbol

    def exit(self, side, reverse=False):
        symbol = self.get_symbol(self.symbol)
        qty = self.qty
        body = {"symbol": symbol,
                "side": side,
                "type": "market",
                "quantity": qty,
                "newOrderRespType": "FULL"
                }
        headers = self.get_header()
        timestamp = int(time.time()) * 1000
        body.update({"timestamp": timestamp,
                     "recvWindow": 10000
                     })
        _body = urllib.parse.urlencode(body)
        signature = hmac.new(self.secret.encode("utf-8"), _body.encode("utf-8"), hashlib.sha256).hexdigest()
        body.update({"signature": signature})
        try:
            resp = requests.post(self.order_url, headers=headers, params=body)
            status_code = resp.status_code
            if status_code in (200, 201):
                data = resp.json()
                logger.info("Order exit at %s" % data)
                if reverse is True:
                    logger.info("Stop loss was hit")
                    trades = data["fills"]
                    total_price = sum(float(k["price"]) * float(k["qty"]) for k in trades)
                    total_qty = sum(float(k["qty"]) for k in trades)
                    price = total_price / total_qty
                    price = round(price, 5)
                    self.price = price
                    self.wait = True
                    return
                data = {"symbol": self.symbol,
                        "side": self.side,
                        "qty": self.qty,
                        "increment": self.increment,
                        "decrement": self.decrement
                        }
                self.symbol = None
                asyncio.ensure_future(self.recycle_order(data))
            else:
                data = resp.json()
                logger.info("Error exiting %s" % data)
                self.qty -= 1
        except Exception as e:
            logger.info("Unable to exit %s" % e)

    async def recycle_order(self, data):
        while True:
            resp, status_code = self.make_order(data)
            if status_code in (200, 201):
                break
            await asyncio.sleep(60)

    def make_order(self, data):
        symbol = data["symbol"]
        qty = data["qty"]
        side = data["side"]
        symbol = self.get_symbol(symbol)
        body = {"symbol": symbol,
                "side": side,
                "type": "market",
                "quantity": qty,
                "newOrderRespType": "FULL"
                }
        headers = self.get_header()
        timestamp = int(time.time()) * 1000
        body.update({"timestamp": timestamp,
                     "recvWindow": 10000
                     })
        _body = urllib.parse.urlencode(body)
        signature = hmac.new(self.secret.encode("utf-8"), _body.encode("utf-8"), hashlib.sha256).hexdigest()
        body.update({"signature": signature})
        try:
            resp = requests.post(self.order_url, headers=headers, params=body)
            status_code = resp.status_code
        except Exception as e:
            data = {"error": e}
            logger.info(data)
            return data, 400
        if status_code not in (200, 201):
            data = {"error": resp.text}
            logger.info(data)
            return data, status_code
        else:
            resp = resp.json()
            logger.info("Order entry at %s" % resp)
            self.poll_market(data, resp)
            return resp, status_code

    def poll_market(self, data, resp):
        trades = resp["fills"]
        total_price = sum(float(k["price"]) * float(k["qty"]) for k in trades)
        total_qty = sum(float(k["qty"]) for k in trades)
        price = total_price / total_qty
        price = round(price, 5)
        side = data["side"]
        symbol = data["symbol"]
        increment = data["increment"]
        decrement = data["decrement"]
        self.price = price
        self.side = side
        self.symbol = symbol
        self.increment = increment
        self.decrement = decrement
        self.qty = total_qty
        self.cycle += 1


Mmaker()
