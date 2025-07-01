#!/usr/bin/python3


import requests
import psycopg2
import psycopg2.extras
from solders.transaction_status import *
import spl.token.constants
import json
from decimal import *
import os
import time
import random
from openai import OpenAI
from inspect import getframeinfo, stack
import datetime
import re
from urllib import parse

# may have to watch clashing names here.
from secret_sdk.client.lcd import LCDClient
from secret_sdk.key.mnemonic import MnemonicKey

from project_include import *

# =====================
# REQUIRED CONFIGURATION
# =====================
# Fill these in for your deployment or development environment.

# --- Secret Network/SSCRT ---
SECRET_CONTRACT_ADDRESS = "<your_secret_contract_address>"  # The contract address for your SSCRT contract on Secret Network
SUPER_DUPER_SECRET_CONTRACT_HASH = "<your_contract_hash>"    # The code hash for the SSCRT contract (get from Secret Network)
SECRET_NETWORK_DECIMALS = 6  # Number of decimals for SCRT (usually 6)
SUPER_SECRET_DECIMALS = 6    # Number of decimals for SSCRT (usually 6)
SUPER_DUPER_SECRET_SEQUENCE_FILENAME = "secret_sequence.txt"  # File to store processed sequence numbers for SSCRT transfers

# --- Telegram ---
TELEGRAM_BOT_API_KEY = "<your_telegram_bot_api_key>"  # Telegram bot API key for notifications

# --- OpenAI (optional, for joke generation) ---
OPEN_AI_API_KEY = "<your_openai_api_key>"  # OpenAI API key for joke generation (optional)

# --- Token Price Caching ---
TOKEN_PRICE_CACHE_SECONDS = 60  # How long to cache token prices (in seconds)

# --- Secret Wallet Credentials (DO NOT SHARE REAL VALUES) ---
SECRET_MNEMONIC = "<your_secret_wallet_mnemonic>"  # The mnemonic phrase for your Secret Network wallet (keep this safe!)
SECRET_VIEWING_KEY = "<your_secret_viewing_key>"    # The viewing key for your Secret Network wallet (keep this safe!)
SECRET_WALLET_ADDRESS = "<your_secret_wallet_address>"  # The wallet address for your Secret Network wallet

# =====================
# END CONFIGURATION
# =====================



db_handle = None

def db_connect():

    global db_handle

    if (db_handle is not None):
        return db_handle

    db_handle = psycopg2.connect(
            dbname="<dbname>",
            user="<dbuser>",
            password="<dbpassword>",
            host="<hostname>"
    )
        
    return db_handle


def crush_down_decimals(amount):
    # Crush down trailing zeros to one.
    amount_as_str = str(Decimal(amount))
    amount_as_str = re.sub('0{,19}$', "", amount_as_str)
    amount_as_str = amount_as_str + "0"
    return amount_as_str



def get_tweet_emoji():

    allowable=('👉','🔥','💥','🎯','👊','🔥','⚔️','👇','💣','🚨','🍻','⚽️','💥','🍻','🥴','👊','🍺','⚽️','🍻','😩','🫠','🍻','🥴','😵','💀','🥃','😭','🫠','🍞','⚔️','🔥','💥','😤','🚨','💣','⚡️','🎯','🫡','🔥')
    item = random.randint(0, len(allowable)-1)
    return allowable[item] # we're returning it 'raw' because the lower-down url-encoding takes care of stuff for us.
    #return allowable[item].encode('unicode-escape').decode('ASCII')



def get_tweet_template(wager_offer_id: int):

    with open("tweet_templates.txt", "r") as fh:
        (lines) = fh.readlines()
        
        
    choice = random.randint(0, len(lines)-1)
    firstline = lines[choice]
    emoji = get_tweet_emoji()
    secondline = emoji + " " + get_wagerlink(wager_offer_id)
    
    return firstline + "\n" + secondline


    

def get_tweet_text(wager_offer_id: int, amount: str, currency: str, prediction: str):


    amount_as_str = crush_down_decimals(amount)

    template = get_tweet_template(wager_offer_id)
    tweet_body = template.format(amount=amount_as_str, currency=currency, prediction=f"'{prediction}'")

    tweet_body = parse.quote(tweet_body)

    return f"<a href='https://x.com/intent/tweet?text={tweet_body}'>Click here to Tweet it!</a>"

def get_wagerlink(wager_offer_id: int):
    
    return f"https://t.me/WagerLinkBot?start=wagerlink_{wager_offer_id}"


def get_joke(home_team: str, away_team: str, prediction_equation: str):

    payload = {}
    payload["home_team_formal"] = home_team
    payload["away_team_formal"] = away_team
    payload["prediction_equation"] = prediction_equation

    system_prompt = ""
    with open("gpt_system_prompt_joke.txt", "r") as fh:
        system_prompt = fh.read()

    fh.close()

    open_ai_client = OpenAI(api_key=OPEN_AI_API_KEY)

    completion = open_ai_client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=1.0,
        max_tokens = 400,
        messages=[
            { "role": "system", "content": system_prompt},
            { "role": "user", "content": json.dumps(payload) }
        ]
    )
    
    return completion.choices[0].message.content
                    


def get_token_price(token_code: str):


    if (token_code == "SCRT" or token_code == "SSCRT"):
        token_name = "secret"


    # Hit cache first
    try:
        with open(f"last_{token_name}_price.txt","r") as fh:
            data = fh.read()
    except:
        data = "0:0"
        
    (last_fetch_time, price) = data.split(':')

    nowtime = int(time.time())
    last_time_int = int(last_fetch_time)
    
    
    if ((nowtime - last_time_int) < TOKEN_PRICE_CACHE_SECONDS):
        
        return price
    
    
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={token_name}&vs_currencies=usd"
    response = requests.get(url)
    
    if (response.ok):
        data = response.json()
        
        filetime = str(int(time.time()))
        price = data[token_name]['usd']
        try:
            tmp_filename = f"last_{token_name}_price.txt.{os.getpid()}"
            with open(tmp_filename,'w') as fh:
                fh.write(f"{filetime}:{price}")
                fh.close()
                os.rename(tmp_filename, f"last_{token_name}_price.txt")
        except:
            pass
    else:
        print(f"Error getting API price for {token_name}. Using last value of {price}")


    return price
    


def save_ssecret_sequence(address: str, highest_used: int):

    tmp_filename = "tmp_sequence_" + str(os.getpid())
    an_address = ""
    got_hit = False
    with open(tmp_filename, "w") as write_fh:
        with open(SUPER_DUPER_SECRET_SEQUENCE_FILENAME, "r") as read_fh:
            for line in read_fh.readlines():
                #print(f"save : {line}")
                try:
                    (an_address, a_sequence) = line.split(':')
                except:
                    pass
                
                if (an_address == address):
                    new_line = str(address + ":" + str(highest_used) + "\n")
                    write_fh.write(new_line)
                    got_hit = True
                else:
                    write_fh.write(str(line))


        if (got_hit is False):
            new_line = str(address + ":" + str(highest_used) + "\n")
            write_fh.write(new_line)

        
    read_fh.close()
    write_fh.close()

    os.rename(tmp_filename, SUPER_DUPER_SECRET_SEQUENCE_FILENAME)


def get_ssecret_sequence(query_address: str):
    try:
        with open(SUPER_DUPER_SECRET_SEQUENCE_FILENAME, "r") as fh:
            for line in fh.readlines():
                #print(f"get : {line}")
                
                (address, sequence) = line.split(':')
                #print(address, sequence)
                if (address == query_address):
                    return(int(sequence))
    except:
        pass

    print("Warning: sequence not found. Starting from 0")
    return 0
    

def uscrt_to_scrt(amount):

    uscrt = float(amount)
    converted = uscrt * 1.0
    as_float = round(converted * 10**-SECRET_NETWORK_DECIMALS, SECRET_NETWORK_DECIMALS)
    
    decimal_value = Decimal(str(as_float))
    return decimal_value

def parse_secret_transaction(txn_ref: str):

    # This is the receiver wallet.
    mnemonic = SECRET_MNEMONIC
    our_wallet_address = SECRET_WALLET_ADDRESS
    viewing_key = SECRET_VIEWING_KEY

    # Provider (free, unknown quality)
    chain_id = "secret-4"
    url = "https://rpc.ankr.com/http/scrt_cosmos"
    client = LCDClient(url=url, chain_id=chain_id)
   
    mk = MnemonicKey(mnemonic=mnemonic)
    wallet = client.wallet(mk)

    try:
        tx_response = wallet.lcd.tx.tx_info(txn_ref.lower())
        transaction = tx_response.tx.to_data()
    except Exception as error:
        print("Error calling SECRET", error)
        return (False, '',0,0,'','', None)
    #print(transaction)

    if tx_response.code == 0:
        print("Transaction was successful")
    else:
        print(f"Error: transaction was not successful. tx_response.code = {tx_response.code}")
        return (False, '',0,0,'','', None)
    

    contract_address = None


    try:
        contract_address = transaction["body"]["messages"][0]["contract"]
        sending_wallet_address = transaction["body"]["messages"][0]["sender"]
    except:
        sending_wallet_address = transaction["body"]["messages"][0]["from_address"]


    #print(f"Sending wallet was {sending_wallet_address}")

    if (contract_address is not None):
        if (contract_address == SECRET_CONTRACT_ADDRESS):

            
            currency_code = "SSCRT"
            wallet_in = "" # we don't know yet
            wallet_out = sending_wallet_address
            amount = -1 # we don't know yet
            
            query = {}
            query["address"] = our_wallet_address
            query["key"] = viewing_key
            query["page_size"] = 10
            fq = {}
            fq["transaction_history"] = query
            response = client.wasm.contract_query(SECRET_CONTRACT_ADDRESS, fq, SUPER_DUPER_SECRET_CONTRACT_HASH)
            #print(f"contract query response = {response}")
            minimum_sequence = get_ssecret_sequence(sending_wallet_address)

            is_us = False;

            if (response.get("transaction_history") is None):
                print("Not our our wallet so we can't see inside.")
                return (False, '', 0, SECRET_NETWORK_DECIMALS, '', '', '')


            for txn in response["transaction_history"]["txs"]:
                
                transfer = txn["action"]["transfer"]
                if (transfer["recipient"] == our_wallet_address and transfer["sender"] == sending_wallet_address):
                    is_us = True
                else:
                    is_us = False

                if (is_us and txn["id"] > minimum_sequence):
                    wallet_in = transfer["recipient"]
                    wallet_out = transfer["sender"]
                    amount = txn["coins"]["amount"]  # unlike regular secret, this is a raw number, not 'uscrt'
                    # so we need to divide it down
                    divisor = "1" + "".zfill(SUPER_SECRET_DECIMALS)
                    value = Decimal(str(amount)) / Decimal(divisor)

                    
                    save_ssecret_sequence(sending_wallet_address, txn["id"])
                    
                    return (True, 'SSCRT', value, SUPER_SECRET_DECIMALS, sending_wallet_address, our_wallet_address, transaction)
                else:
                    # Not for us, not a new txn or something else.
                    print(f"{txn_ref} : is_us={is_us} sequence > seen = {txn['id'] > minimum_sequence}")
                    return (False, 'SSCRT', Decimal(0), SUPER_SECRET_DECIMALS, '', our_wallet_address, transaction)
                

        else:
            print("Some kind of contract invoke, but not super duper")
            return (False, '', 0, SECRET_NETWORK_DECIMALS, '', '', transaction)
    else:
        # regular txn
        print("was regular txn")
        amount = transaction["body"]["messages"][0]["amount"][0]
        currency = "SCRT"
        if (amount["denom"] == "uscrt"):
            # not the best interface design by those lads.
            print(f"Converting {amount} {amount['amount']}")
            value = uscrt_to_scrt(amount["amount"])
            return (True, 'SCRT', value, SECRET_NETWORK_DECIMALS, sending_wallet_address, our_wallet_address, transaction)
        else:
            print("Warning: unparsable amount in SECRET transcation")
            return (False, '', 0, SECRET_NETWORK_DECIMALS, '', '', transaction)
        
        
       
    


def hex_to_int(value):
    return int(value, 16) if value else 0



def get_wager_balance(wager_id: int):

    getcontext().prec = 20

    dbh = db_connect()
    cursor = dbh.cursor()
    
    psycopg2.extras.register_composite('t_wager_amount', cursor)

    
    query = "SELECT wager_amount FROM user_wagers_offered WHERE int_wager_offer_id = %s"
    cursor.execute(query, (wager_id,))
    row = cursor.fetchone()
    
    starting_balance = row[0]

    
    sum_so_far = Decimal(str(starting_balance.c_value))
    
    query = "SELECT match_amount FROM user_wagers_matched WHERE int_wager_offer_id = %s and dtm_payment_recieved is not null"
    cursor.execute(query, (wager_id,))
    
    dbh.commit()
    
    for record in cursor.fetchall():
        sum_so_far = sum_so_far - Decimal(str(record[0].c_value))
        
        
    print(f"{wager_id} has balance {sum_so_far}")
 
    return  sum_so_far.quantize(Decimal('1.00'))
        
                                                                                    


def send_wagerlink(wager_offer_id: int):

    dbh = db_connect()
    cursor = dbh.cursor()
    
    psycopg2.extras.register_composite('t_wager_amount', cursor)
    
    #query = "SELECT int_user_id FROM user_wagers_offered WHERE int_wager_offer_id = %s"
    query = "SELECT user_wagers_offered.int_user_id, user_wagers_offered.txt_prediction_equation, fixtures.txt_team1, fixtures.txt_team2, user_wagers_offered.txt_prediction_text, user_wagers_offered.wager_amount \
             FROM user_wagers_offered, fixtures \
             WHERE user_wagers_offered.int_fixture_id = fixtures.int_fixture_id \
               AND user_wagers_offered.int_wager_offer_id = %s"
    
    

    cursor.execute(query, (wager_offer_id,))
    row = cursor.fetchone()

    currency = row[5].c_code
    amount = row[5].c_value
    prediction = row[4]


    lpo = {}
    lpo["is_disabled"] = True

    payload = {}
    payload["chat_id"] = row[0]
    payload["link_preview_options"] = lpo
    payload["text"] = f"Payment is complete! Here is your wagerlink: \n{get_wagerlink(wager_offer_id)}"

    send_tg_message(payload)


    payload["parse_mode"] = "HTML"
    payload["text"] = get_tweet_text(wager_offer_id, amount, currency, prediction)
    send_tg_message(payload)
    payload.pop("parse_mode", None)



def send_wager_match(wager_match_id: int):
    
    dbh = db_connect()
    cursor = dbh.cursor()
    psycopg2.extras.register_composite('t_wager_amount', cursor)

    
    query = "SELECT int_user_id, int_wager_offer_id, match_amount FROM user_wagers_matched WHERE int_wager_match_id = %s"

    cursor.execute(query, (wager_match_id,))
    row = cursor.fetchone()

    offerer_user_id = row[0]
    wager_offer_id = row[1]
    wager_match_amount = row[2]
    wager_match_value = row[2].c_value
    currency_code = row[2].c_code

    # Tell the player who matched
    payload = {}
    payload["chat_id"] = offerer_user_id
    payload["text"] = "Your wager match payment is complete! Thanks and good luck to you!"

    send_tg_message(payload)


    # Also, tell the offering party.
    query = "SELECT int_user_id, wager_amount FROM user_wagers_offered WHERE int_wager_offer_id = %s"

    cursor.execute(query, (wager_offer_id,))
    row = cursor.fetchone()

    matcher_user_id = row[0]
    wager_offer_amount = row[1]
    wager_offer_value = row[1].c_value

    full_match = False
    balance_remaining = 0.0
    

    if (wager_offer_value - wager_match_value < 0.001):
        full_match = True


    if (full_match is False):
        balance_remaining = get_wager_balance(wager_offer_id)
            

            
    offer_amount_str = crush_down_decimals(wager_offer_value)
    match_amount_str = crush_down_decimals(wager_match_value)
    remainder_str = crush_down_decimals(balance_remaining)


    msg_text = f"Hi. I'm just letting you know that your wager of {offer_amount_str} {currency_code} has been fully matched by a single player. Thanks and good luck to you!"
    if (full_match is False):
        if (balance_remaining == 0.0):
            msg_text = f"Hi. I'm just letting you know that your wager of {offer_amount_str} {currency_code} has been partially mached by {match_amount_str} {currency_code}, leaving {remainder_str} to be matched. Good luck to you!"
        else:
            msg_text = f"Hi. I'm just letting you know that your wager of {offer_amount_str} {currency_code} has been partially mached by {match_amount_str} {currency_code}, making it fully matched now. Good luck to you!"
    
            

    payload = {}
    payload["chat_id"] = matcher_user_id
    payload["text"] = msg_text

    send_tg_message(payload)

    
    
    
def send_tg_message(payload: dict):

    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_API_KEY}/sendMessage"
    response = requests.post(url, json=payload)
                

def die(message: str):
    caller = getframeinfo(stack()[1][0])
    print(f"Exiting early from {caller}: {message}\n")
    exit()


def process_secret():
    print("Start SECRET processing (SSCRT and SCRT)")

    dbh = db_connect()
    cursor = dbh.cursor()
    
    psycopg2.extras.register_composite('t_wager_amount', cursor)
    
    query = """
    SELECT COALESCE(int_wager_offer_id, -1) as offer, COALESCE(int_wager_match_id,-1) as match, txt_transaction_ref 
    FROM user_payment_sessions 
    WHERE txt_transaction_ref IS NOT NULL 
    AND dtm_record_completed IS NULL  
    AND length(txt_transaction_ref) = 64 
    AND txt_real_token NOT LIKE '%_PAID'
    ORDER BY dtm_record_created
    """

    cursor.execute(query)
    if (cursor.rowcount > 0):
        rows = cursor.fetchall()
        for row in rows:
            if (row[2] is None):
                print("Skip row")
                continue

            
            transaction_reference = row[2]
            (success, currency_code, value_decimal, decimals, wallet_out, wallet_in, txn) = parse_secret_transaction(transaction_reference)
                
            if (success is False):
                print("Unparsable transaction. Skipping")
                continue
            
            print(f"{row[2]} -> {wallet_out} {wallet_in} {value_decimal} {currency_code}")
                
                    
            if (row[0] > 0):

                # It's a wager offer
                            
                wager_offer_id = row[0]
                # Get the offer and make sure it's for the same value
                query = "SELECT wager_amount FROM user_wagers_offered WHERE int_wager_offer_id = %s"
                cursor.execute(query, (wager_offer_id,))
                offer_row = cursor.fetchone()[0]
                
                if (offer_row[0] != currency_code):
                    print("Abort. The currency code and the transaction currency do not match")
                    continue
                if (Decimal(str(offer_row[1])[:11]) != value_decimal):
                    """
                    print (float(str(offer_row[1])[:11]))
                    print(offer_row[1])
                    a = Decimal(float(str(offer_row[1])[:11]))
                    b = a - offer_row[1]
                    print(a-b)
                    """
                    difference = abs((Decimal(str(offer_row[1])[:11]) - value_decimal))
                    if (difference > Decimal("0.000001")):
                        print(f"Abort. The amount of the transaction {value_decimal} does not match the offer {offer_row[1]}")
                        continue
                    else:
                        print(f"Allowing small difference of {difference} to be accepted")
                                
                query = "UPDATE user_wagers_offered SET dtm_confirmed = NOW(), b_paid = TRUE, txt_source_address = %s WHERE int_wager_offer_id = %s"
                cursor.execute(query, (wallet_out, wager_offer_id,))

                query = "UPDATE user_payment_sessions SET txt_real_token = CONCAT(txt_real_token, '_PAID'), \
                dtm_record_completed = NOW(), json_transaction = %s \
                WHERE int_wager_offer_id = %s"
                
                cursor.execute(query, (json.dumps(str(txn)), wager_offer_id,))
                dbh.commit()

                                
                # Tell the user.
                send_wagerlink(wager_offer_id)

                            
            else:
                # it's a wager match.
                        
                wager_match_id = row[1]
                
                query = "SELECT match_amount FROM user_wagers_matched WHERE int_wager_match_id = %s"
                cursor.execute(query, (wager_match_id,))
                match_row = cursor.fetchone()[0]

                if (match_row[0] != currency_code):
                    print("Abort. The currency code and the transaction currency do not match")
                    continue
                        
                query = "UPDATE user_wagers_matched SET match_amount = %s::t_wager_amount, dtm_payment_recieved = NOW(), txt_source_address = %s WHERE int_wager_match_id = %s"
                amount_compound = (currency_code, match_row[1], get_token_price(currency_code))
                            
                cursor.execute(query, (amount_compound, wallet_out, wager_match_id,))
                dbh.commit()
                            
                        
                       
                # Now update the payment sessions table with a record of the transaction
                query = "UPDATE user_payment_sessions SET txt_real_token = CONCAT(txt_real_token, '_PAID'), \
                dtm_record_completed = NOW(), json_transaction = %s \
                WHERE txt_transaction_ref = %s"
                
                cursor.execute(query, (json.dumps(str(txn)), transaction_reference,))
                dbh.commit()

                # Tell the user
                send_wager_match(wager_match_id)

    

if (__name__ == "__main__"):
    

    while(True):
        process_secret()

        nowtime = str(datetime.datetime.now())
        print(f"[{nowtime}] : Finished processing batch. Sleeping for 15 seconds")
        time.sleep(15)
