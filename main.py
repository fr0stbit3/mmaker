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
        self.state = "waiting_for_init"
        self.decrement = 0
        self.qty = 0
        self.cycle = 0
        self.wins = 0
        self.loss = 0
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
            if self.state == "waiting_for_exit":
                self.check_for_exit()
                await asyncio.sleep(60)
                continue
            if self.state == "waiting_for_entry":
                self.check_for_entry()
                await asyncio.sleep(60)
                continue
            await asyncio.sleep(5)

    def check_for_entry(self):
        candles = self.fetch_candle()
        if not candles:
            return
        data = candles[-1]
        close = data[4]
        close = float(close)
        close = round(close, 5)
        _open = data[1]
        _open = float(_open)
        _open = round(_open, 5)
        pdata = candles[-2]
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
        entry = False
        if self.side == "BUY" and candle1 == "GREEN" and candle2 == "GREEN":
            entry = True
        if self.side == "SELL" and candle1 == "RED" and candle2 == "RED":
            entry = True
        if entry:
            logger.info("Entering cycle %s" % (self.cycle + 1))
            logger.info("Entry %s Decrement %s Increment %s Close %s" % (self.price, self.decrement, self.increment, close))
            data = {"symbol": self.symbol,
                    "side": self.side,
                    "qty": self.qty,
                    "increment": self.increment,
                    "decrement": self.decrement
                    }
            _, status_code = self.make_order(data)
            if status_code in (200, 201):
                self.state = "waiting_for_exit"

    def check_for_exit(self):
        candles = self.fetch_candle()
        if not candles:
            return
        data = candles[-1]
        close = data[4]
        close = float(close)
        close = round(close, 5)
        exit = False
        side = None
        if self.side == "BUY":
            if close <= self.price - self.decrement:
                logger.info("Stop loss hit")
                exit = True
                side = "SELL"
                self.loss += 1
            if close >= self.price + self.increment:
                logger.info("Target profit hit")
                exit = True
                side = "SELL"
                self.wins += 1
        else:
            if close >= self.price + self.decrement:
                logger.info("Stop loss hit")
                exit = True
                side = "BUY"
                self.loss += 1
            if close <= self.price - self.increment:
                logger.info("Target profit hit")
                side = "BUY"
                exit = True
                self.wins += 1
        if exit:
            logger.info("Exiting cycle %s" % self.cycle)
            logger.info("Wins %s loss %s" % (self.wins, self.loss))
            logger.info("Entry %s Decrement %s Increment %s Close %s" % (self.price, self.decrement, self.increment, close))
            data = {"symbol": self.symbol,
                    "side": side,
                    "qty": self.qty
                    }
            _, status_code = self.make_order(data)
            if status_code in (200, 201):
                self.state = "waiting_for_entry"
        
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
        return data

    def get_symbol(self, symbol):
        symbol = symbol.replace("/", "")
        return symbol

    def setup_app(self):
        self.app.router.add_routes([web.post("/order", self.handle_order)])
        self.app.router.add_routes([web.post("/update", self.update_cycle)])
        self.app.router.add_routes([web.post("/exit", self.handle_exit)])
        self.app.router.add_routes([web.post("/resume", self.handle_resume)])

    async def handle_exit(self, request):
        side = "BUY" if self.side == "SELL" else "SELL"
        data = {"symbol": self.symbol,
                "side": side,
                "qty": self.qty
                }
        body, status_code = self.make_order(data)
        if status_code in (200, 201):
            logger.info("Wins %s Loss %s" % (self.wins, self.loss))
            self.wins = 0
            self.loss = 0
            self.state = "waiting_for_init"
        return web.json_response(body, status=status_code)

    async def update_cycle(self, request):
        data = await request.json()
        logger.info("Update exit %s" % data)
        self.increment = data["increment"]
        self.decrement = data["decrement"]
        return web.json_response({"data": "Ok"}, status=200)

    async def handle_order(self, request):
        data = await request.json()
        logger.info("Handle post %s" % data)
        if self.state != "waiting_for_init":
            return web.json_response({"data": "Exit first"}, status=status_code)
        self.cycle = 0
        body, status_code = self.make_order(data)
        return web.json_response(body, status=status_code)

    def get_header(self):
        headers = {"X-MBX-APIKEY": self.api_key}
        return headers

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
        if self.state != "waiting_for_exit":
            self.poll_market(data, resp)
        return self.send_order_request(headers, body)

    def send_order_request(self, data, headers, body):
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
            return resp, status_code

    async def handle_resume(self, request):
        data = await request.json()
        if self.state != "waiting_for_init":
            return web.json_response({"data": "Cannot resume"}, status=400)
        self.price = data["price"]
        self.symbol = data["symbol"]
        self.qty = data["qty"]
        self.increment = data["increment"]
        self.decrement = data["decrement"]
        self.cycle += 1
        self.state = "waiting_for_exit"
        self.side = data["side"]
        return web.json_response({"data": "Ok"}, status=200)

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
        self.state = "waiting_for_exit"


Mmaker()
