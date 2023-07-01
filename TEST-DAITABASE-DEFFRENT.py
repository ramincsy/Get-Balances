import pyodbc
import requests
from decimal import Decimal
import time
from datetime import datetime
import json
import abc
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.builder.call_builder import CallBuilder

# مشخصات اتصال به دیتابیس
server = '.'
database = 'Get-Balance-Rebital'
username = 'sa'
password = 'Rebital@123'
driver= '{ODBC Driver 17 for SQL Server}'

class BalanceFetcher(abc.ABC):
    def __init__(self, address):
        self.address = address

    @abc.abstractmethod
    def get_balance(self, currency):
        pass

class EtherscanBalanceFetcher(BalanceFetcher):
    def __init__(self, address, api_key):
        super().__init__(address)
        self.api_key = api_key

    def get_balance(self, currency):
        contract_address = currency['contract']
        if contract_address:
            url = f"https://api.etherscan.io/api?module=account&action=tokenbalance&contractaddress={contract_address}&address={self.address}&tag=latest&apikey={self.api_key}"
        else:
            url = f"https://api.etherscan.io/api?module=account&action=balance&address={self.address}&tag=latest&apikey={self.api_key}"

        response = requests.get(url)
        
        if response.status_code == 200:
            result = json.loads(response.text)['result']
            if result.isdigit():
                return int(result) / (10 ** currency['decimals'])
            else:
                print(f"Error retrieving balance: {result}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None

class PolygonscanBalanceFetcher(BalanceFetcher):
    def __init__(self, address, api_key):
        super().__init__(address)
        self.api_key = api_key

    def get_balance(self, currency):
        url = f"https://api.polygonscan.com/api?module=account&action=balance&address={self.address}&tag=latest&apikey={self.api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            result = json.loads(response.text)['result']
            if result.isdigit():
                return int(result) / (10 ** currency['decimals'])
            else:
                print(f"Error retrieving balance: {result}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None

class MaticBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        api_key = "2VFTJJANHASYH37CPMHR9CUYGGXNYUVY5B"  # Enter your Polygonscan API key here
        response = requests.get(f"https://api.polygonscan.com/api?module=account&action=balance&address={self.address}&tag=latest&apikey={api_key}")
        balance = int(json.loads(response.text)['result']) / 1e18
        return balance
    
class LtcBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        response = requests.get(f"https://api.blockcypher.com/v1/ltc/main/addrs/{self.address}/balance")
        response_dict = json.loads(response.text)
        balance = int(response_dict['final_balance']) / 1e8
        return balance

class TronscanBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        token_symbol = currency['symbol']
        try:
            url = f"https://apilist.tronscan.org/api/account?address={self.address}"
            response = requests.get(url)
            data = response.json()

            # Check TRX balance
            if token_symbol.lower() == 'trx':
                return float(data.get('balance', 0)) / 1e6

            # Check TRC20 token balances
            token_balances = data.get('trc20token_balances', [])
            for token in token_balances:
                if token['tokenAbbr'].lower() == token_symbol.lower():
                    return float(token['balance']) / (10 ** token['tokenDecimal'])

            return None

        except Exception as e:
            print(f"Error retrieving {token_symbol.upper()} balance on TRON: {e}")
            return None

class BitcoinBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        response = requests.get(f"https://blockchain.info/q/addressbalance/{self.address}")
        balance = int(response.text) / 1e8
        return balance

class BitcoinCashBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        response = requests.get(f"https://api.blockchair.com/bitcoin-cash/dashboards/address/{self.address}")
        data = json.loads(response.text)
        balance = float(data['data'][self.address]['address']['balance']) / 1e8
        return balance

class CardanoBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        api_key = 'mainnetmTYW1JdBJql7zY0ZleshUeebEOpVXQq4'  
        try:
            response = requests.get(f"https://cardano-mainnet.blockfrost.io/api/v0/addresses/{self.address}", headers={'project_id': api_key})
            data = json.loads(response.text)
            balance = int(data['amount'][0]['quantity']) / 1e6
            print(f"Cardano Balance: {balance}")
            return balance
        except Exception as e:
            print(f"Error retrieving Cardano balance: {e}")
            return None

class XrpBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        response = requests.get(f"https://data.ripple.com/v2/accounts/{self.address}/balances")
        data = response.json()
        for balance in data["balances"]:
            if balance["currency"] == "XRP":
                return float(balance["value"]) / 1e6  # XRP value is in drops, divide by 1 million to get XRP
        return 0.0  # Return 0 if XRP balance is not found

class NeoBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        try:
            url = f"https://dora.coz.io/api/v1/neo3/mainnet/balance/{self.address}"
            response = requests.get(url)
            data = response.json()

            balances = {}
            for entry in data:
                symbol = entry['symbol']
                balance = entry['balance']
                balances[symbol] = balance

            print(f"NEO Balances: {balances}")
            return balances['NEO']  # only return the NEO balance

        except Exception as e:
            print(f"Error retrieving NEO balance: {e}")
            return None
    
class QtumBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        response = requests.get(f"https://qtum.info/api/address/{self.address}")
        data = response.json()
        balance = Decimal(data['balance']) / Decimal(1e8)  # Divide by 10^8 to convert from satoshis to QTUM
        return balance

class BnbBep2BalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        response = requests.get(f"https://dex.binance.org/api/v1/account/{self.address}")
        data = response.json()
        bnb_balance = next((item for item in data['balances'] if item["symbol"] == "BNB"), None)
        if bnb_balance is not None:
            balance = Decimal(bnb_balance['free'])
        else:
            balance = None
        return balance




class DogeBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        response = requests.get(f"https://dogechain.info/api/v1/address/balance/{self.address}")
        data = response.json()
        balance = Decimal(data['balance']) / Decimal(1e8)
        return balance


class BscBep20BalanceFetcher(BalanceFetcher):
    def __init__(self, address, api_key):
        super().__init__(address)
        self.api_key = api_key

    def get_balance(self, currency):
        contract_address = currency.get('contract')
        # If contract_address is provided, fetch token balance, else fetch BNB balance
        if contract_address:
            url = f"https://api.bscscan.com/api?module=account&action=tokenbalance&contractaddress={contract_address}&address={self.address}&tag=latest&apikey={self.api_key}"
        else:
            url = f"https://api.bscscan.com/api?module=account&action=balance&address={self.address}&tag=latest&apikey={self.api_key}"

        response = requests.get(url)
        
        if response.status_code == 200:
            result = json.loads(response.text)['result']
            if result.isdigit():
                return int(result) / (10 ** currency['decimals'])
            else:
                print(f"Error retrieving balance: {result}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None
        

class ICXBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        url = "https://ctz.solidwallet.io/api/v3"
        headers = {"Content-Type": "application/json"}
        data = {
            "jsonrpc": "2.0",
            "method": "icx_getBalance",
            "params": {"address": self.address},
            "id": 1
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        balance = response.json().get('result', None)
        if balance:
            return int(balance, 16) / (10 ** 18)  # Convert from hex to decimal and adjust for ICX's 18 decimal places
        else:
            return None

class StellarBalanceFetcher(BalanceFetcher):
    def get_balance(self, currency):
        return get_balance_stellar(self.address)

def get_balance_stellar(address):
    response = requests.get(f"https://horizon.stellar.org/accounts/{address}")
    data = json.loads(response.text)
    if 'balances' in data and isinstance(data['balances'], list):
        for balance in data['balances']:
            if balance['asset_type'] == "native":
                return float(balance['balance'])
        return None
    else:
        return None

addresses = {
    "XRP": "r4KR4HraNJYQtufsQCSZNQQuFx2fiNhGxv",
    "ETH": "0x8cB34b21dad081adFAd82E2B1769AB22D1c7cEE8",
    "TRX": "TNjgMYjt3NUZVEvTyYyKetzq4yvjREWoSX",
    "USDT": "TNjgMYjt3NUZVEvTyYyKetzq4yvjREWoSX",
    "MATIC": "0xafed221131d04fa0cc86bdd25ada4017f2933f02",
    "LTC": "Li5ieACvBr3FGF2uKgjHceTMxJTWo2JjHT",
    "BTC": "1HVkuphJS8fTeQPVxK37t3vSw6ZtNi216",
    "BCH": "qrm5f88ugp54404flr2yv03727tv5hq9fvf3zxzscd",
    "ADA": "addr1qyzluw0qqvvkgvjg8mhwuh84p5d6qhrfkpwys2zrpcqx2lsw3y2tkfmhp9jx5ad3uwqs93ea4jgxfk58lss6c5uh8psswl8e84",
    "BNB_BEP2": "bnb1q7an8j8hxrrmh7rw5730qjwer7j6md0q8wsp04",
    "QTUM": "QjHtjATghMycfSm7UwsWhkP5jStGhVH9wW",
    "NEO": "NgWLcYrWH6ebaFjSEVXLjPXPix6oi2zUjq",
    "DOGE": "D9jg93utJig13qZxQwVHoNZQGxpYKeaEQa",
    "ICX": "hxdc2371d9c12bca4ef8c84ec945cdf62a812a4eb6",
    "BNB": "0x264D6643a7d661bE5A14F023370B4e3d61d2Bf87",
    "SHIBA": "0x264D6643a7d661bE5A14F023370B4e3d61d2Bf87",
    "BNB_BEP20": "0x264D6643a7d661bE5A14F023370B4e3d61d2Bf87",
    "XLM": "GDSMCZHDD3OC73YSMURAJYKA5MGTQI726DW7IHLW5SHKLP3CJ4QU474N",
    "LINK": "0xb507FbBaa0da1a39F22B986C2D4Cb3B8084E8b94"
}
# Create fetchers
fetchers = [
    XrpBalanceFetcher(address="XRP"),
    EtherscanBalanceFetcher(address=addresses["ETH"], api_key="RT6JRKER1WVK5JI7XQ8ZIEKNUMNJ93426Q"),
    TronscanBalanceFetcher(address=addresses["TRX"]),
    NeoBalanceFetcher(address=addresses["NEO"]),
    PolygonscanBalanceFetcher(address=addresses["MATIC"], api_key="2VFTJJANHASYH37CPMHR9CUYGGXNYUVY5B"),
    LtcBalanceFetcher(address=addresses["LTC"]),
    MaticBalanceFetcher(address=addresses["MATIC"]),
    BitcoinBalanceFetcher(address=addresses["BTC"]),
    BitcoinCashBalanceFetcher(address=addresses["BCH"]),
    QtumBalanceFetcher(address=addresses["QTUM"]),
    DogeBalanceFetcher(address=addresses["DOGE"]),
    BnbBep2BalanceFetcher(address=addresses["BNB_BEP2"]),
    BscBep20BalanceFetcher(address=addresses["BNB"], api_key="W5IRFIP8Z1EM1Z1CKM4WRB3TDXKAUN9R3R"),
    CardanoBalanceFetcher(address=addresses["ADA"]),
    StellarBalanceFetcher(address=addresses["XLM"])
    ]

# Create fetchers directly in the tokens dictionary
tokens = [
    {"name": "XRP", "symbol": "xrp", "decimals": 6, "fetcher": XrpBalanceFetcher(address=addresses["XRP"])},
    {"name": "ETH", "contract": None, "decimals": 18, "fetcher": EtherscanBalanceFetcher(address=addresses["ETH"], api_key="RT6JRKER1WVK5JI7XQ8ZIEKNUMNJ93426Q")},
    {"name": "LINK", "contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18, "fetcher": EtherscanBalanceFetcher(address=addresses["LINK"], api_key="RT6JRKER1WVK5JI7XQ8ZIEKNUMNJ93426Q")},
    {"name": "TRX", "symbol": "trx", "decimals": 6, "fetcher": TronscanBalanceFetcher(address=addresses["TRX"])},
    {"name": "USDT", "symbol": "usdt", "decimals": 6, "fetcher": TronscanBalanceFetcher(address=addresses["USDT"])},
    {"name": "MATIC", "symbol": "matic", "decimals": 18, "fetcher": MaticBalanceFetcher(address=addresses["MATIC"])},
    {"name": "LTC", "symbol": "ltc", "decimals": 8, "fetcher": LtcBalanceFetcher(address=addresses["LTC"])},
    {"name": "BTC", "symbol": "btc", "decimals": 8, "fetcher": BitcoinBalanceFetcher(address=addresses["BTC"])},
    {"name": "BCH", "symbol": "bch", "decimals": 8, "fetcher": BitcoinCashBalanceFetcher(address=addresses["BCH"])},
    {"name": "ADA", "symbol": "ada", "decimals": 6, "fetcher": CardanoBalanceFetcher(address=addresses["ADA"])},
    {"name": "BNB_BEP2", "symbol": "bnb_bep2", "decimals": 8, "fetcher": BnbBep2BalanceFetcher(address=addresses["BNB_BEP2"])},
    {"name": "QTUM", "symbol": "qtum", "decimals": 8, "fetcher": QtumBalanceFetcher(address=addresses["QTUM"])},
    {"name": "NEO", "symbol": "neo", "decimals": 8, "fetcher": NeoBalanceFetcher(address=addresses["NEO"])},
    {"name": "DOGE", "symbol": "doge", "decimals": 8, "fetcher": DogeBalanceFetcher(address=addresses["DOGE"])},
    {"name": "ICX", "symbol": "icx", "decimals": 18, "fetcher": ICXBalanceFetcher(address=addresses["ICX"])},
    {"name": "BNB", "contract": None, "decimals": 18, "fetcher": BscBep20BalanceFetcher(address=addresses["BNB"], api_key="W5IRFIP8Z1EM1Z1CKM4WRB3TDXKAUN9R3R")},
    {"name": "SHIBA", "contract": "0x2859e4544C4bB03966803b044A93563Bd2D0DD4D", "decimals": 18, "fetcher": BscBep20BalanceFetcher(address=addresses["SHIBA"], api_key="W5IRFIP8Z1EM1Z1CKM4WRB3TDXKAUN9R3R")},
    {"name": "BNB_BEP20", "symbol": "bnb_bep20", "decimals": 18, "fetcher": BscBep20BalanceFetcher(address=addresses["BNB_BEP20"], api_key="W5IRFIP8Z1EM1Z1CKM4WRB3TDXKAUN9R3R")},
    {"name": "XLM", "symbol": "xlm", "decimals": 7, "fetcher": StellarBalanceFetcher(address=addresses["XLM"])}
    ]
# Fetch and print balances
for fetcher in fetchers:
    try:
        balance = fetcher.get_balance('icx') # or replace 'icx' with the correct currency for each fetcher
        print(f"{fetcher.__class__.__name__} Balance: {balance}")
    except Exception as e:
        print(f"Error retrieving balance for {fetcher.__class__.__name__}: {str(e)}")


while True:
    # ایجاد اتصال به دیتابیس
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = connection.cursor()

    for currency in tokens:
        # Get balance
        try:
            new_balance = currency['fetcher'].get_balance(currency)
        except Exception as e:
            print(f"Error retrieving balance for {currency['name']}: {str(e)}")
            continue

        if isinstance(new_balance, dict) and 'balance' in new_balance:
            new_balance = new_balance['balance']

        if new_balance is None:
            print(f"Failed to fetch balance for {currency['name']}")
            continue

        # یافتن موجودی قبلی
        cursor.execute("""
        SELECT TOP 1 balance
        FROM Balances
        WHERE name = ? 
        ORDER BY timestamp DESC
        """, currency['name'])
        row = cursor.fetchone()
        if row:
           previous_balance = Decimal(row[0])
        else:
           previous_balance = Decimal(0)

        # محاسبه اختلاف
        if isinstance(new_balance, (int, float, Decimal)) and isinstance(previous_balance, (int, float, Decimal)):
            difference = Decimal(new_balance) - Decimal(previous_balance)
        else:
            print(f"Failed to calculate difference for token {currency['name']} due to non-numeric balance.")
            continue

        # ذخیره موجودی جدید
        cursor.execute("""
        INSERT INTO Balances (name, balance, previous_balance, difference, address, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """, currency['name'], Decimal(new_balance).quantize(Decimal('0.00000000')), Decimal(previous_balance).quantize(Decimal('0.00000000')), Decimal(difference).quantize(Decimal('0.00000000')), currency['fetcher'].address, datetime.now())

        # Commit the transaction
        connection.commit()

    # Close the connection
    connection.close()

    # انتظار برای 5 دقیقه
    time.sleep(300)
