import os
import time
import asyncio
import aiohttp
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
from logger import logger
from aiohttp import web
from threading import Thread


os.environ["TZ"] = "Asia/Kolkata"


class Mmaker(object):

    def __init__(self):
        self.app = web.Application()
        self.order_url = "https://api.hitbtc.com/api/2/order"
        self.candle_url = "https://api.hitbtc.com/api/2/public/candles"
        self.api_key = "6170d61976152b42fc71e587ac112bcb"
        self.secret = "8a6161f57ed61df65a036cdd1b629539"
        self.price = 0
        self.side = None
        self.symbol = None
        self.increment = 0
        self.decrement = 0
        self.qty = 0
        self.cycle = 0
        self.setup_app()
        t = Thread(target=self.setup_poller)
        t.start()
        logger.info("HitBTC mmaker running")
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
            self.fetch_candle()
            await asyncio.sleep(300)

    def fetch_candle(self):
        symbol = self.symbol
        symbol = self.get_symbol(symbol)
        url = "%s/%s?limit=1&sort=DESC&period=M5" % (self.candle_url, symbol)
        resp = requests.get(url)
        data = resp.json()
        data = data[0]
        close = data["close"]
        close = float(close)
        logger.info("Cycle %s" % self.cycle)
        logger.info("Entry %s Decrement %s Increment %s Close %s" % (self.price, self.decrement, self.increment, close))
        if self.side == "BUY":
            if close < self.price - self.decrement:
                logger.info("Exiting")
                self.exit("SELL")
            if close > self.price + self.increment:
                logger.info("Increasing price")
                self.price = close
        else:
            if close > self.price + self.decrement:
                logger.info("Exiting")
                self.exit("BUY")
            if close < self.price - self.increment:
                logger.info("Increasing price")
                self.price = close

    def setup_app(self):
        self.app.router.add_routes([web.post("/handle_post", self.handle_post)])

    async def handle_post(self, request):
        data = await request.json()
        logger.info("Handle post %s" % data)
        body, status_code = self.make_order(data)
        return web.json_response(body, status=status_code)

    def get_header(self):
        headers = {"Connection": "Keep-Alive",
                   "Content-Type": "application/json"
                   }
        return headers

    def get_auth(self):
        auth = HTTPBasicAuth(self.api_key, self.secret)
        return auth

    def get_symbol(self, symbol):
        base, quote = symbol.split("/")
        if base != "XRP":
            symbol = "%sUSD" % base
        else:
            symbol = symbol.replace("/", "")
        return symbol

    def exit(self, side):
        symbol = self.get_symbol(self.symbol)
        side = side.lower()
        qty = self.qty
        data = {"symbol": symbol,
                "side": side,
                "type": "market",
                "timeInForce": "GTC",
                "quantity": qty
                }
        headers = self.get_header()
        auth = self.get_auth()
        try:
            resp = requests.post(self.order_url, headers=headers, auth=auth, json=data)
            status_code = resp.status_code
            if status_code in (200, 201):
                data = resp.json()
                logger.info("Order exit at %s" % data)
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
        side = side.lower()
        symbol = self.get_symbol(symbol)
        body = {"symbol": symbol,
                "side": side,
                "type": "market",
                "timeInForce": "GTC",
                "quantity": qty
                }
        headers = self.get_header()
        auth = self.get_auth()
        try:
            resp = requests.post(self.order_url, headers=headers, auth=auth, json=body)
            status_code = resp.status_code
        except Exception as e:
            data = {"error": e}
            return data, 400
        if status_code not in (200, 201):
            data = {"error": resp.text}
            return data, status_code
        else:
            resp = resp.json()
            logger.info("Order entry at %s" % resp)
            self.poll_market(data, resp)
            return resp, status_code

    def poll_market(self, data, resp):
        trades = resp["tradesReport"]
        total_price = sum(float(k["price"]) * float(k["quantity"]) for k in trades)
        total_qty = sum(float(k["quantity"]) for k in trades)
        price = total_price / total_qty
        price = round(price, 4)
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
