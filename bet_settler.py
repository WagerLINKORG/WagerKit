#!/usr/bin/python3


import requests
import psycopg2
import psycopg2.extras
from solders.transaction_status import *
import spl.token.constants
from decimal import *
import re
from inspect import getframeinfo, stack
from enum import Enum

# =====================
# REQUIRED CONFIGURATION
# =====================
# Fill these in for your deployment or development environment.

# --- Telegram ---
TELEGRAM_BOT_API_KEY = "<your_telegram_bot_api_key>"  # Telegram bot API key for notifications

# --- File Output ---
TRANSACTION_BATCH_FILENAME = "transaction_batch.txt"  # File to store payment records

# --- Commission Wallet ---
COMMISSION_WALLET_ADDRESS = "<your_commission_wallet_address>"  # Wallet address to receive commissions

# --- Bet Outcome Types ---
class BetOutcomeType(Enum):
    UNDETERMINED = 1,
    OFFER_WINS_NO_MATCH = 2,
    OFFER_LOSES_NO_MATCH =3,
    OFFER_WINS_PARTIAL_MATCH = 4,
    OFFER_LOSES_PARTIAL_MATCH = 5,
    OFFER_WINS_FULL_MATCH = 6,
    OFFER_LOSES_FULL_MATCH = 7,
    MATCH_LOSES = 8,
    MATCH_WINS = 9,
    COMMISSION = 10,
    REFUND = 11

# =====================
# END CONFIGURATION
# =====================

db_handle = None

def db_connect():

    global db_handle

    if (db_handle is not None):
        #print("Returning cached handle")
        return db_handle
        
    
    db_handle = psycopg2.connect(
            dbname="<dbname>",
            user="<dbuser>",
            password="<dbpassword>",
            host="<hostname>"
    )
        
    return db_handle
                                                                                                                                               

    

def send_tg_message_batch(user_id: int, messages: list[str]):
    payload = {}
    payload["chat_id"] = user_id
    for message in messages:
        payload["text"] = message
        send_tg_message(payload)
    
    
def send_tg_message(payload: dict):

    #payload["chat_id"] = 7365331834 # my ID. All messages go to me.

    #print(f"not sending: {payload}")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_API_KEY}/sendMessage"
    response = requests.post(url, json=payload)
                

def die(message: str):
    caller = getframeinfo(stack()[1][0])
    print(f"Exiting early from {caller}:\n{message}\n")
    exit()


def get_match_result(fixture_id: int):

    dbh = db_connect()
    cursor = dbh.cursor()
    query = "SELECT txt_team1, txt_team2, dtm_match_date, txt_ht_score, txt_ft_score FROM fixtures WHERE int_fixture_id = %s"

    cursor.execute(query, (fixture_id,))

    
    try:
        if (cursor.rowcount == 1):
            row = cursor.fetchone()
            (ht_home, ht_away) = row[3].split(',')
            (ft_home, ft_away) = row[4].split(',')
            
            return(True, ht_home, ht_away, ft_home, ft_away)

    except:
        pass
    
    return (False, None, None, None, None)


def get_wager_offer_amount(offer_id: int):
    dbh = db_connect()
    cursor = dbh.cursor()

    psycopg2.extras.register_composite('t_wager_amount', cursor)

    query = "SELECT wager_amount FROM user_wagers_offered WHERE int_wager_offer_id = %s"

    cursor.execute(query, (offer_id,))
    row = cursor.fetchone()
    return (row[0].c_value, row[0].c_code)


def get_matches_for_offer(offer_id: int):

    dbh = db_connect()
    cursor = dbh.cursor()

    psycopg2.extras.register_composite('t_wager_amount', cursor)

    query = "SELECT int_wager_match_id, int_user_id, match_amount FROM user_wagers_matched WHERE int_wager_offer_id = %s AND dtm_payment_recieved IS NOT NULL ORDER BY int_user_id"

    cursor.execute(query, (offer_id,))
    records = cursor.fetchall()

    ret = []
    for record in records:
        item = (record[0], record[1], record[2].c_value)
        ret.append(item)


    return ret
    
def get_wager_amount(offer_id: int):
    dbh = db_connect()
    cursor = dbh.cursor()

    psycopg2.extras.register_composite('t_wager_amount', cursor)

    query = "SELECT wager_amount FROM user_wagers_offered WHERE int_wager_offer_id = %s"
    cursor.execute(query, (offer_id,))
    row = cursor.fetchone()

    
    return (format_decimal_to_minimum(row[0].c_value), row[0].c_value, row[0].c_code)

def format_decimal_to_minimum(inval: Decimal):
    as_str = str(inval)
    as_str = re.sub('0{,19}$', "", as_str)
    as_str = as_str + "0"
    return as_str


def get_fixture_data(fixture_id: int):

    dbh = db_connect()
    cursor = dbh.cursor()

    query = "SELECT int_fixture_id, txt_league_name, to_char(dtm_match_date, 'YYYY-MM-DD HH24:MI'), txt_team1, txt_team2, txt_ht_score, txt_ft_score \
             FROM fixtures, leagues \
             WHERE int_fixture_id = %s \
             AND fixtures.int_league_id = leagues.int_league_id"

    cursor.execute(query, (fixture_id, ))
    dbh.commit()
    if (cursor.rowcount != 1):
        return(None, None, None, None, None, None)
    else:
        row = cursor.fetchone()
        #(matchdate, leaguename, home_team, away_team, ht score, ft score)
        
        return (row[2], row[1], row[3], row[4], row[5], row[6] )
    
                                                                                        

def get_match_description(fixture_id: str):
    (matchdate, leaguename, home_team, away_team, ht_text, ft_text) = get_fixture_data(fixture_id)

    matchdate = re.sub('(\d\d):(\d\d):(\d\d)', r'\1:\2', str(matchdate))
    ht_text = re.sub(',','-', ht_text)
    ft_text = re.sub(',','-', ft_text)
   
    return (f"{home_team} vs {away_team} on {matchdate}. {leaguename}", ht_text, ft_text)


def tax_bet(winnings_sum, offer_currency):
    
    commission = winnings_sum * Decimal("0.05")
    winnings_final = winnings_sum - commission

    return (winnings_final, commission)
    

def write_payment_records(records: list[str]):
    
    with open(TRANSACTION_BATCH_FILENAME, "a") as fh:
        for record in records:
            is_first_col = True
            for item in record:
                if (is_first_col):
                    fh.write(str(item.name))
                    is_first_col = False
                else:
                    fh.write(str(item))
                fh.write("\t")
            fh.write("\n")
            
def get_offer_wallet_address(offer_record_id: int):

    dbh = db_connect()
    cursor = dbh.cursor()
    query = "SELECT txt_source_address FROM user_wagers_offered WHERE int_wager_offer_id = %s"
    cursor.execute(query, (offer_record_id,))

    if (cursor.rowcount != 1):
        die(f"Error getting wallet address for wager offer {offer_record_id}")

    return cursor.fetchone()[0]
        




def get_matcher_wallet_address(matcher_record_id: int):

    dbh = db_connect()
    cursor = dbh.cursor()
    query = "SELECT txt_source_address FROM user_wagers_matched WHERE int_wager_match_id = %s"
    cursor.execute(query, (matcher_record_id,))

    if (cursor.rowcount != 1):
        die(f"Error getting wallet address for wager match {matcher_record_id}")

    return cursor.fetchone()[0]

            
def get_destination_wallet_address(record_id: int, outcome_type: BetOutcomeType):

    if (outcome_type == BetOutcomeType.COMMISSION):
        # Easy first one, it's us.
        return COMMISSION_WALLET_ADDRESS
    if (outcome_type == BetOutcomeType.OFFER_WINS_NO_MATCH or
        outcome_type == BetOutcomeType.OFFER_LOSES_NO_MATCH or
        outcome_type == BetOutcomeType.OFFER_WINS_FULL_MATCH or
        outcome_type == BetOutcomeType.OFFER_WINS_PARTIAL_MATCH or
        outcome_type == BetOutcomeType.MATCH_LOSES or
        outcome_type == BetOutcomeType.REFUND):
        return get_offer_wallet_address(record_id)
    if (outcome_type == BetOutcomeType.OFFER_LOSES_FULL_MATCH or
        outcome_type == BetOutcomeType.OFFER_LOSES_PARTIAL_MATCH or
        outcome_type == BetOutcomeType.MATCH_WINS):
        return get_matcher_wallet_address(record_id)
    
    

    die(f"outcome type {outcome_type} not dealt with")
    
    

def emit_payment_records(outcome, offer_user_id, offer_id, offer_amount_decimal, offer_currency, match_records):


    print(f"Offer : {offer_amount_decimal} {offer_currency}")

    records_returned = []


    # First let's deal with the wager offerer. He has win/lose and full/partial outcomes, so 4
    if (outcome == BetOutcomeType.OFFER_WINS_NO_MATCH):
        print(f"OFFER_WINS_NO_MATCH - refund {offer_user_id} {offer_amount_decimal}")
        records_returned.append((outcome, get_destination_wallet_address(offer_id, outcome), offer_amount_decimal, offer_currency))
        
    elif (outcome == BetOutcomeType.OFFER_LOSES_NO_MATCH):
        print(f"OFFER_LOSES_NO_MATCH - refund {offer_user_id} {offer_amount_decimal}")
        records_returned.append((outcome, get_destination_wallet_address(offer_id, outcome), offer_amount_decimal, offer_currency))
        
    elif (outcome == BetOutcomeType.OFFER_WINS_FULL_MATCH):
        print("OFFER_WINS_FULL_MATCH - give DOUBLE amount to OFFER user")

        # Give the offerer all of the money, minus commission

        winnings_intermediate = offer_amount_decimal * Decimal(2.0)
        (winnings_final, commission_amount) = tax_bet(winnings_intermediate, offer_currency)
        records_returned.append((BetOutcomeType.COMMISSION, get_destination_wallet_address(offer_id, BetOutcomeType.COMMISSION), commission_amount , offer_currency))
        records_returned.append((outcome, get_destination_wallet_address(offer_id, outcome), winnings_final, offer_currency))
        """
        # No payment records needed for matchers, as all their money is going to the offerer.
        for record in match_records:
            # Tell them they've lost
            (wager_match_id, matcher_user_id, value) = record
            records_returned.append((BetOutcomeType.MATCH_LOSES, get_destination_wallet_address(offer_id, BetOutcomeType.MATCH_LOSES), value, offer_currency))
        """
        print(f"Pay offer user {offer_user_id} the amount {winnings_final} in {offer_currency}")
        print(f"We take {commission_amount} as commmission from the winner")
        
    elif (outcome == BetOutcomeType.OFFER_WINS_PARTIAL_MATCH):
        print("OFFER_WINS_PARTIAL_MATCH - give DOUBLE matched amounts to OFFER user and refund the rest")
        # Give the offerer the matched amounts from each of the offers
        winnings_sum = Decimal("0")
        for record in match_records:
            (wager_match_id, matcher_user_id, value) = record
            winnings_sum = winnings_sum + value
            """
            # No payment records needed for matchers, as all their money is going to the offerer, plus he get's a bit of a refund for the unmached amount
            records_returned.append((BetOutcomeType.MATCH_LOSES, get_destination_wallet_address(offer_id, BetOutcomeType.MATCH_LOSES), value, offer_currency))
            """
        refund_amount = offer_amount_decimal - winnings_sum
                                    
        # These are matched bets, so the winning amount is offer * 2. But we take a commission, so calculate that.
        winnings_intermediate = winnings_sum * Decimal(2.0)
        (winnings_final, commission_amount) = tax_bet(winnings_intermediate, offer_currency)
        
        records_returned.append((BetOutcomeType.COMMISSION, get_destination_wallet_address(offer_id, BetOutcomeType.COMMISSION), commission_amount , offer_currency))
        records_returned.append((BetOutcomeType.REFUND, get_destination_wallet_address(offer_id, BetOutcomeType.REFUND), refund_amount, offer_currency))
        records_returned.append((outcome, get_destination_wallet_address(offer_id, outcome), winnings_final , offer_currency))
  
                                    
        print(f"Pay the offer user {offer_user_id} the amount {winnings_final} in {offer_currency}")
        print(f"We take {commission_amount} as commmission from the winner")
        print(f"Also refund the same user {offer_user_id} a refund of non-matched {refund_amount}")

    elif (outcome == BetOutcomeType.OFFER_LOSES_FULL_MATCH):
        print("OFFER_LOSES_FULL_MATCH - give all the MATCH users DOUBLE their money")
        # Give the offerers all of the players's money, apportioned by the amount they wagered to match

        #records_returned.append((outcome, offer_id, offer_user_id, offer_amount_decimal, offer_currency))
        
        for record in match_records:
            (wager_match_id, matcher_user_id, value) = record
            winnings_sum = winnings_sum + value

            winnings_intermediate = value * Decimal(2.0)
            (winnings_final, commission_amount) = tax_bet(winnings_intermediate, offer_currency)

            records_returned.append((BetOutcomeType.COMMISSION, get_destination_wallet_address(offer_id, BetOutcomeType.COMMISSION), commission_amount , offer_currency))
            records_returned.append((BetOutcomeType.MATCH_WINS, get_destination_wallet_address(wager_match_id, BetOutcomeType.MATCH_WINS), winnings_final , offer_currency))
            
            print(f"Pay the match  user {matcher_user_id} the amount {winnings_final} in {offer_currency}")
            print(f"We take {commission_amount} as commmission from the winner")
            
       
    elif (outcome == BetOutcomeType.OFFER_LOSES_PARTIAL_MATCH):
        print("OFFER_LOSES_PARTIAL_MATCH - give all the MATCH users DOUBLE their money, and refund the un-matched amount")
        # Give the offerers some of the player's money, apportioned by the amount they wagered to match, and refund the rest

        #records_returned.append((outcome, get_destination_wallet_address(offer_id), winnings_final , offer_currency))

        
        amount_lost_sum = Decimal("0")
        for record in match_records:
            (wager_match_id, matcher_user_id, value) = record
            print(f"{matcher_user_id} matched for {value}")
            amount_lost_sum = amount_lost_sum + value

            winnings_intermediate = value * Decimal(2.0)
            (winnings_final, commission_amount) = tax_bet(winnings_intermediate, offer_currency)

            records_returned.append((BetOutcomeType.COMMISSION, get_destination_wallet_address(offer_id, BetOutcomeType.COMMISSION), commission_amount , offer_currency))
            records_returned.append((BetOutcomeType.MATCH_WINS, get_destination_wallet_address(wager_match_id, BetOutcomeType.MATCH_WINS), winnings_final , offer_currency))
            
            
            print(f"Pay the match  user {matcher_user_id} the amount {winnings_final} in {offer_currency}")
            print(f"We take {commission_amount} as commmission from the winner")
      
        refund_amount = offer_amount_decimal - amount_lost_sum

        records_returned.append((BetOutcomeType.REFUND, get_destination_wallet_address(offer_id, BetOutcomeType.REFUND), refund_amount, offer_currency))
        
        print(f"Also refund the OFFER user {offer_user_id} a refund of non-matched {refund_amount}")
        
    else:
        die("Error: oucome of bet not recognised")


    write_payment_records(records_returned)
    return records_returned
        
    print("\n")
    

def send_settlement_messages(outcome, offer_user_id, offer_amount_decimal, offer_currency, description, offer_prediction, ht_score, ft_score, payment_records):
    
    # Offer party first.
    messages = []
    
    if (outcome == BetOutcomeType.OFFER_WINS_NO_MATCH):

        messages.append(f"Hi. You made this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        messages.append(f"The game info was: {description}")
        messages.append(f"You won.")
        messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
        messages.append(f"Unfortunately, no-one took up your offer, so we're refunding you your stake of {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        
        send_tg_message_batch(offer_user_id, messages)

    if (outcome == BetOutcomeType.OFFER_LOSES_NO_MATCH):

        messages.append(f"Hi. You made this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        messages.append(f"The game info was: {description}")
        messages.append(f"You lost the wager.")
        messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
        messages.append(f"However, no-one took up your offer, so we're refunding you your stake of {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        
        send_tg_message_batch(offer_user_id, messages)


    if (outcome == BetOutcomeType.OFFER_WINS_FULL_MATCH):

        # Tell the winner he's rich
        (outcome_type, _, offer_user_id, winnings_amount, offer_currency) = payment_records[0]
        messages.append(f"Hi. You made this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        messages.append(f"The game info was: {description}")
        messages.append(f"You won.")
        messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
        messages.append(f"Your wager was fully matched. Your winnings are {format_decimal_to_minimum(winnings_amount)} {offer_currency}")
        
        send_tg_message_batch(offer_user_id, messages)
        messages.clear()

        # Now tell the losers
        for record in payment_records:
            (outcome_type, _, matcher_user_id, winnings_final, offer_currency) = record
            if (outcome_type == BetOutcomeType.MATCH_LOSES):
                messages.append(f"Hi. You matched this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
                messages.append(f"The game info was: {description}")
                messages.append(f"You lost, because their wager came true")
                messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")

            
                send_tg_message_batch(matcher_user_id, messages)
                messages.clear()
            else:
                print("Ignoring record")
        
        
    if (outcome == BetOutcomeType.OFFER_LOSES_FULL_MATCH):

        if (len(payment_records) == 1):
            # A single person matched this
            (outcome_type, _, matcher_user_id, winnings_final, offer_currency) = payment_records[0]
            
            messages.append(f"Hi. You matched this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
            messages.append(f"The game info was: {description}")
            messages.append(f"You won, because the wager did not come true.")
            messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
            messages.append(f"Your winnings are {format_decimal_to_minimum(winnings_final)} {offer_currency}")
            
            send_tg_message_batch(matcher_user_id, messages)
            messages.clear()


        else:
            for record in payment_records:
                (outcome_type, _, matcher_user_id, winnings_final, offer_currency) = record
                if (outcome_type == BetOutcomeType.MATCH_WINS):
                    messages.append(f"Hi. You matched this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
                    messages.append(f"The game info was: {description}")
                    messages.append(f"You won, because the wager did not come true.")
                    messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
                    messages.append(f"However, you only matched part of the wager, so your winnings are {format_decimal_to_minimum(winnings_final)} {offer_currency}")

                    send_tg_message_batch(matcher_user_id, messages)
                    messages.clear()
                else:
                    print(f"Ignoring winnner record {outcome_type} for {matcher_user_id}")
                   

        # now tell the loser (offering party)
        messages.append(f"Hi. You made this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        messages.append(f"The game info was: {description}")
        messages.append(f"You lost.")
        messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
        
        send_tg_message_batch(offer_user_id, messages)


                
                
    if (outcome == BetOutcomeType.OFFER_LOSES_PARTIAL_MATCH):

        # Do the winners first, so we have the amount to tell the loser
        amount_to_refund = offer_amount_decimal

        for record in payment_records:
            
            (outcome_type, _, matcher_user_id, winnings_final, offer_currency) = record
            if (outcome_type == BetOutcomeType.MATCH_WINS):
                amount_to_refund = amount_to_refund - winnings_final
            
                messages.append(f"Hi. You matched this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
                messages.append(f"The game info was: {description}")
                messages.append(f"You won, because the wager did not come true.")
                messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
                messages.append(f"However, you only matched part of the wager, so your winnings are {format_decimal_to_minimum(winnings_final)} {offer_currency}")
            
                send_tg_message_batch(matcher_user_id, messages)
                messages.clear()
            else:
                print(f"Ignoring winnner record {outcome_type} for {matcher_user_id}")
                   
        # Ok. Now we have told all the winners, let's tell the loser.
        messages.append(f"Hi. You made this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        messages.append(f"The game info was: {description}")
        messages.append(f"You lost the wager.")
        messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
        messages.append(f"However, not all of your wager was matched, so we're refunding part of your stake, {format_decimal_to_minimum(amount_to_refund)} {offer_currency}")
        
        send_tg_message_batch(offer_user_id, messages)


    if (outcome == BetOutcomeType.OFFER_WINS_PARTIAL_MATCH):
                
        # Do the losers first, so we have the amount to tell the winner that's unmached so he gets it back
        amount_to_refund = offer_amount_decimal

        for record in payment_records:
            (outcome_type, _, matcher_user_id, winnings_final, offer_currency) = record
            amount_to_refund = amount_to_refund - winnings_final
            
            messages.append(f"Hi. You matched this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
            messages.append(f"The game info was: {description}")
            messages.append(f"You lost because it did come true.")
            messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
            
            
            send_tg_message_batch(matcher_user_id, messages)
            messages.clear()
                   
        # Ok. Now we have told all the losers, let's tell the winner.
        messages.append(f"Hi. You made this wager: {offer_prediction} for {format_decimal_to_minimum(offer_amount_decimal)} {offer_currency}")
        messages.append(f"The game info was: {description}")
        messages.append(f"You won.")
        messages.append(f"Result was: half-time score {ht_score}, full-time score {ft_score}")
        messages.append(f"However, not all of your wager was matched, so we're refunding part of your stake, {format_decimal_to_minimum(amount_to_refund)} {offer_currency}")
        messages.append(f"Your winnings are {format_decimal_to_minimum(winnings_final - amount_to_refund)} {offer_currency}")
        send_tg_message_batch(offer_user_id, messages)


def mark_as_settled(offer_id: int, match_records: list[str]):
    
    dbh = db_connect()
    cursor = dbh.cursor()

    query = "UPDATE user_wagers_offered SET dtm_settled = now() WHERE int_wager_offer_id = %s"
    cursor.execute(query, (offer_id,))


    query = "UPDATE user_wagers_matched SET dtm_settled = now() WHERE int_wager_match_id = %s"
    for record in match_records:
        (wager_match_id, matcher_user_id, value) = record
        cursor.execute(query, (wager_match_id,))


    dbh.commit()
        
    
    
    
        

def process_resolution(is_winner: bool, offer_id: int, fixture_id:int, pred_eng:str, offer_user_id:int):

    (match_description, ht_score, ft_score) = get_match_description(fixture_id)
    (offer_amount, offer_amount_decimal, offer_currency) = get_wager_amount(offer_id)
    match_records = get_matches_for_offer(offer_id)
    messages = []
    outcome = BetOutcomeType.UNDETERMINED

    

    
    # Deal with the simplest outcome first. If there were no matches then we refund stake and message
    # the user slightly different wordings depending on win/lose

    if (len(match_records) == 0):

        # No matches
        if (is_winner):
            outcome = BetOutcomeType.OFFER_WINS_NO_MATCH
        else:
            outcome = BetOutcomeType.OFFER_LOSES_NO_MATCH
            

    else:

        # Some matches.

        # Sum them so we can decide if it's partial or full match.
        winnings_amount_decimal = Decimal("0")
        for record in match_records:
            (wager_match_id, matcher_user_id, value) = record
            print(f"{matcher_user_id} matched {value} on match ID {wager_match_id}")
            winnings_amount_decimal = winnings_amount_decimal + value
            
                
        print(f"winnings amount {winnings_amount_decimal}")


        if (winnings_amount_decimal == offer_amount_decimal):
            # Full amount goes to either the offerer or the matcher(s)
            if (is_winner):
                outcome = BetOutcomeType.OFFER_WINS_FULL_MATCH
            else:
                outcome = BetOutcomeType.OFFER_LOSES_FULL_MATCH
                
            
        else:
            if (is_winner):
                outcome = BetOutcomeType.OFFER_WINS_PARTIAL_MATCH
            else:
                outcome = BetOutcomeType.OFFER_LOSES_PARTIAL_MATCH


    # Now, pay the right people.
    payment_records = emit_payment_records(outcome, offer_user_id, offer_id, offer_amount_decimal, offer_currency, match_records)

    # Now mark the bet and any matchers as settled
    mark_as_settled(offer_id, match_records)
    
    # Now everyone has been paid, do the messaging
    send_settlement_messages(outcome, offer_user_id, offer_amount_decimal, offer_currency, match_description, pred_eng, ht_score, ft_score, payment_records)



             


if (__name__ == "__main__"):


    dbh = db_connect()
    cursor = dbh.cursor()

    psycopg2.extras.register_composite('t_wager_amount', cursor)
    
    query = "SELECT int_wager_offer_id, int_fixture_id, txt_prediction_equation, txt_prediction_text, wager_amount, int_user_id \
             FROM user_wagers_offered \
             WHERE b_paid = TRUE AND dtm_settled IS NULL \
             ORDER BY dtm_timestamp DESC"

    cursor.execute(query)
    
    for row in cursor.fetchall():
        offer_id = row[0]
        fixture_id = row[1]
        user_id = row[5]
        
        pred_eq = row[2]
        pred_eng = row[3]
        
        amount_val = row[4].c_value
        amount_ccy = row[4].c_code
        amount_rate = row[4].c_rate

        
        (success, ht_home, ht_away, ft_home, ft_away) = get_match_result(fixture_id)
        
        if (success):
            testable =  pred_eq.replace("SCORE_HT_HOME", ht_home)
            testable = testable.replace("SCORE_HT_AWAY", ht_away)
            testable = testable.replace("SCORE_FT_HOME", ft_home)
            testable = testable.replace("SCORE_FT_AWAY", ft_away)
            #print(testable)
            #print(eval(testable))

            is_winner = eval(testable)
            process_resolution(is_winner, offer_id, fixture_id, pred_eng, user_id)
                                
        else:
            #print(f"No results for wager {offer_id}, fixture {fixture_id}")
            pass
                
            
    exit()
            
