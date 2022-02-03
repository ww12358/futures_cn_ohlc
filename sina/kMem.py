import asyncio
import json
import pyarrow as pa
import aioredis
import redis

class kMem:

    # async def _init(self):
    #     self._r = await aioredis.create_redis_pool(
    #         "redis://localhost", minsize=5, maxsize=10, db=1
    #     )

    def __init__(self, symbol, cInfo, loop):
        #         self._svr = redis_svr
        self.symbol = symbol
        self._r = redis.StrictRedis(host='127.0.0.1', port=6379, db=1)
        # loop.creat_task(_init)
        # task = asyncio.create_task(self._init())
        #         self.dfs = [df for df ]
        #         self._r = await aiodis.create_redis_pool(self._svr, minsize=5, maxsize=10, db=1)
        self.cInfo = cInfo
        self.all_contracts = self.get_all_contracts()
        self.contract_1st = self.all_contracts[0]
        self.dfs = {}
        self.load_hfreq()
        # asyncio.run(self.load_hfreq())
        # print(self.all_contract)
        return

    def __del__(self):
        self._r.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._r.close()

        return True
    # async close_redis(self):
    #     self.r.close()
    #     await r.wait_closed()

    # @classmethod
    # async def load_all(cls, symbol, cInfo, r):
    #     self = kMem(symbol, cInfo, r)
    #     await self.load_hfreq()
    #     for k, v in self.dfs:
    #         print(k, v)

        # return self


    def get_all_contracts(self):
        # print(self.cInfo)
        return self.cInfo['all']

    def get_1st_contract(self):
        return self.contract_1st

    def load_hfreq(self):
        # dfs = {}
        try:
            for c in self.all_contracts:
                ptn = self.symbol + '??' + c
                print(ptn)
                k = self._r.keys(ptn)
                buf = self._r.get(k[0])
                self.dfs[c] = pa.deserialize(buf)
                print(self.dfs[c])

        except IndexError:
            print("Data of exist on redis. Pass...")
            pass
        except Exception as e:
            print("Error occured while loading hfreq contracts from redis...\t", str(e))

    # async def load_hfreq(self):
    #     # dfs = {}
    #     try:
    #         for c in self.all_contracts:
    #             ptn = self.symbol + '??' + c
    #             print(ptn)
    #             k = await self._r.keys(ptn)
    #             buf = await self._r.get(k[0])
    #             self.dfs[c] = pa.deserialize(buf)
    #             print(self.dfs[c])
    #     except Exception as e:
    #         print("Error occured while loading hfreq contracts from redis...\t", str(e))

    def getMainContract(self):
        with open("/home/sean/code/utils/main_contracts.json", "r") as f:
            m_con = json.load(f)

        self.mainContract = m_con[self.symbol]['1st']

        return self.mainContract