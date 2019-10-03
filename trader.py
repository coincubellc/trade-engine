#!/usr/bin/env python3
from pprint import pformat
from threading import Event
from celery import Celery, group, chain
from celery.exceptions import SoftTimeLimitExceeded

from tools import (sanity_check, calc_indiv, calc_comb, update_cube_cache)
from utils.api import api_request, get_api_creds
from utils.order import cancel_order, place_order, target_orders
from utils.reconcile import reconcile_balances, reconcile_order
from utils.regression import regression
from database import *
import numpy as np
# Replacing datetime.time (Do not move)
from time import time, sleep

CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
CELERY_RESULT_BACKEND = CELERY_BROKER_URL

celery = Celery('trader', backend=CELERY_RESULT_BACKEND, broker=CELERY_BROKER_URL)
celery.conf.broker_transport_options = {'fanout_prefix': True}
celery.conf.broker_transport_options = {'fanout_patterns': True}
celery.conf.worker_prefetch_multiplier = 1
celery.conf.task_time_limit = 1800
celery.conf.task_soft_time_limit = 12000

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
stopevent = Event()


class SqlAlchemyTask(celery.Task):
    """An abstract Celery Task that ensures that the connection the the
    database is closed on task completion"""
    abstract = True

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        db_session.close()
        db_session.remove()   

@celery.task(base=SqlAlchemyTask)
def place_orders(cube_id, orders):
    try:
        cube = Cube.query.get(cube_id)
        log.debug(f'{cube} Placing Orders')
        for order in orders:
            # Arguments: cube_id, ex_pair_id, side, amount, price
            place_order(cube_id, order[2], order[3], order[0], order[1])
        update_cube_cache(cube_id, False) 
    except SoftTimeLimitExceeded:
        update_cube_cache(cube_id, False)  
        ## To do: error handling  

@celery.task(base=SqlAlchemyTask)
def cancel_orders(cube_id, ex, orders):
    try:
        cube = Cube.query.get(cube_id)
        log.debug(f'{cube} Canceling Orders')
        for order in orders:
            cancel_order(cube_id, 
                        ex.id, 
                        order['order_id'], 
                        order['base'],
                        order['quote']
                        )
        update_cube_cache(cube_id, False) 
    except SoftTimeLimitExceeded:
        update_cube_cache(cube_id, False)  
        ## To do: error handling  


#### Not implemented yet
# @celery.task(base=SqlAlchemyTask)
# def unrecognized_activity(cube_id):
#     try:
#         # Possible theft of key and malicious trading activity
#         # Delete all open orders and force update
#         cube = Cube.query.get(cube_id)
#         log.debug(f'{cube} Unrecognized Activity')
#         for conn in cube.connections.values():
#             ex = conn.exchange
#             creds = get_api_creds(cube, ex)
#             args = {**creds, **{'type': 'open'}}
#             orders = api_request(cube, 'GET', ex.name, '/orders', args)
#             if orders:
#                 cancel_orders.delay(cube_id, ex, orders)
#         db_session.add(cube)
#         db_session.commit()
#     except SoftTimeLimitExceeded:
#         update_cube_cache(cube_id, False)
#         ## To do: error handling

@celery.task(base=SqlAlchemyTask)
def trigger_rebalance(cube_id):
    try:
        cube = Cube.query.get(cube_id)
        log.debug(f'{cube} Rebalance triggered')
        cube.reallocated_at = datetime.utcnow()
        for b in cube.balances:
            b.target = None
        db_session.add(cube)
        db_session.commit()
    except SoftTimeLimitExceeded:
        update_cube_cache(cube_id, False)  
        ## To do: error handling

def get_start_timestamp(cube, db_tx):
    if db_tx:
        log.debug(f'{cube} Tranasactions exist, updating from {db_tx.timestamp}')
        return int(float(db_tx.timestamp)) + 1
    else:
        log.debug(f'{cube} Transactions do not exist, updating from account start')
        return int(datetime.timestamp(cube.created_at) * 1000) # Convert to milliseconds


def import_trades(cube, ex, creds, since):
    now = time() * 1000
    days = 10
    trades = pd.DataFrame()
    url = '/trades'

    if ex.name in ['Binance', 'Liquid']:
        while since < now:
            new_trades = pd.DataFrame()
            for bal in cube.balances:
                ex_pairs = ExPair.query.filter_by(
                            exchange_id=ex.id, active=True
                            ).filter(or_(
                                ExPair.base_currency_id == bal.currency_id,
                                ExPair.quote_currency_id == bal.currency_id,
                            )).all()
                for ex_pair in ex_pairs:
                    args = {**creds, 
                        **{
                            'base': ex_pair.base_symbol,
                            'quote': ex_pair.quote_symbol,
                            'limit': 1000,
                            'since': since
                        }
                    }
                    binance_trades = api_request(cube, 'GET', ex.name, url, args)
                    binance_trades = pd.read_json(binance_trades)
                    new_trades = new_trades.append(binance_trades)
                    sleep(1)
            if not new_trades.empty:
                new_trades = new_trades.sort_index()
                new_trades.timestamp = new_trades.timestamp.astype(np.int64)//10**6
                since = int(new_trades.iloc[-1].timestamp) + 1
                trades = trades.append(new_trades)
            elif since < now:
                # 10 days in milliseconds
                since = since + 24 * 60 * 60 * days * 1000
            else:
                break

    else:
        while since < now:
            args = {**creds, **{'since': since}}
            new_trades = api_request(cube, 'GET', ex.name, url, args)
            new_trades = pd.read_json(new_trades)
            if not new_trades.empty:
                new_trades.timestamp = new_trades.timestamp.astype(np.int64)//10**6
                since = new_trades.iloc[-1].timestamp + 1
                trades = trades.append(new_trades)
            elif since < now:
                # 10 days in milliseconds
                since = since + 24 * 60 * 60 * days * 1000
            else:
                break

    if not trades.empty:
        # Adjustments to dataframe to match table structure
        fee = trades['fee'].apply(pd.Series)
        try:
            fee = fee.drop(['type'], axis=1)
        except:
            pass
        try:
            fee = fee.rename(index=str, columns={'rate': 'fee_rate', 'cost': 'fee_amount', 'currency': 'fee_currency'})
        except:
            pass
        trades = pd.concat([trades, fee], axis=1)
        trades = trades.rename(index=str, columns={'id': 'tx_id', 'order': 'order_id', 'amount': 'base_amount', 'cost': 'quote_amount'})
        symbol = trades['symbol'].str.split('/', n=1, expand=True)
        trades['base_symbol'] = symbol[0]
        trades['quote_symbol'] = symbol[1]
        trades['trade_type'] = trades['type']
        trades['type'] = trades['side']
        trades.drop(['side', 'symbol', 'fee'], axis=1, inplace=True)
        _, i = np.unique(trades.columns, return_index=True)
        trades = trades.iloc[:, i]
        trades = trades.fillna(value=0)

        # Add trades to database
        log.debug(f'{cube} Writing trades to database')
        for index, row in trades.iterrows():
            if row.trade_type != 0:
                trade_type = row.trade_type
            else:
                trade_type = None
            ex_pair = ExPair.query.filter_by(
                        exchange_id=ex.id,
                        quote_symbol=row.quote_symbol,
                        base_symbol=row.base_symbol,
                        ).first()
            if not ex_pair:
                continue
            if not row.base_amount:
                continue
            trade = Transaction(
                    tx_id=row.tx_id,
                    datetime=index[0:19],
                    order_id=row.order_id,
                    type=row.type,
                    trade_type=trade_type,
                    price=row.price,
                    base_symbol=row.base_symbol,
                    quote_symbol=row.quote_symbol,
                    exchange=ex,
                    cube=cube,
                    user=cube.user,
                )
            db_session.add(trade)
            db_session.commit()
            db_session.refresh(trade)
            if row.type == 'buy':
                trade.base_amount = row.base_amount
                trade.quote_amount = -row.quote_amount
            elif row.type == 'sell':
                trade.base_amount = -row.base_amount
                trade.quote_amount = row.quote_amount                
            try:
                trade.fee_rate = row.fee_rate
                trade.fee_amount = row.fee_amount
                trade.fee_currency = row.fee_currency
            except Exception:
                pass
            db_session.add(trade)
            db_session.commit()


def import_transactions(cube, ex, creds, since):
    now = time() * 1000
    trans = pd.DataFrame()
    url = '/transactions'
    old_since = 0

    while since < now:
        args = {**creds, **{'since': since}}
        new_trans = api_request(cube, 'GET', ex.name, url, args)
        if not new_trans:
            break
        new_trans = pd.read_json(new_trans)
        if not new_trans.empty:
            new_trans.timestamp = new_trans.timestamp.astype(np.int64)//10**6
            since = new_trans.iloc[-1].timestamp + 1
            if old_since == since:
                break
            old_since = since
            trans = trans.append(new_trans)
        elif since < now:
            # 10 days in milliseconds
            since = since + 24 * 60 * 60 * days * 1000
        else:
            break   

    if not trans.empty:
        log.debug(f'{cube} Adjusting dataframe to match table structure')
        # Adjustments to dataframe to match table structure
        if trans['fee'].any():
            fee = trans['fee'].apply(pd.Series)
            try:
                fee = fee.rename(index=str, columns={'rate': 'fee_rate', 'cost': 'fee_amount'})
                trans = pd.concat([trans, fee], axis=1)
            except:
                log.debug(f'{cube} missing transaction fee information, skipping...')
            trans.drop(['fee'], axis=1, inplace=True)
        trans = trans.rename(index=str, columns={'id': 'tx_id', 'txid': 'order_id'})
        trans.drop(['status', 'updated', 'timestamp'], axis=1, inplace=True)
        _, i = np.unique(trans.columns, return_index=True)
        trans = trans.iloc[:, i]
        trans = trans.fillna(value=0)

        # Add transactions to database
        log.debug(f'{cube} Writing transactions to database')
        for index, row in trans.iterrows():
            if not row.amount:
                continue
            cur = Currency.query.filter_by(symbol=row.currency).first()
            if not cur:
                continue
            if row.type not in ['deposit', 'withdrawal', 'withdraw']:
                continue
            if row.type == 'withdraw':
                t_type = 'withdrawal'
            else:
                t_type = row.type

            ex_pair = ExPair.query.filter_by(
                        exchange_id=ex.id,
                        base_currency_id=cur.id,
                        ).first()
            if t_type == 'deposit':
                base_amount = row.amount
            elif t_type == 'withdrawal':
                base_amount = -row.amount
            quote_amount = 0
            if not ex_pair:
                ex_pair = ExPair.query.filter_by(
                        exchange_id=ex.id,
                        quote_currency_id=cur.id,
                        ).first()
                base_amount = 0
                if t_type == 'deposit':
                    quote_amount = row.amount
                elif t_type == 'withdrawal':
                    quote_amount = -row.amount

            tx = Transaction(
                    tx_id=row.tx_id,
                    datetime=index[0:19],
                    order_id=row.order_id,
                    tag=row.tag,
                    base_amount=base_amount,
                    quote_amount=quote_amount,
                    type=t_type,
                    trade_type=None,
                    base_symbol=ex_pair.base_currency.symbol,
                    quote_symbol=ex_pair.quote_currency.symbol,
                    exchange=ex,
                    cube=cube,
                    user=cube.user,
                )
            db_session.add(tx)
            db_session.commit()


def update_transactions(cube, creds):
    log.debug(f'{cube} Updating transactions')
    db_tx = Transaction.query.filter_by(
                    cube_id=cube.id
                    ).order_by(
                    Transaction.id.desc()
                    ).first()

    try:
        ts = get_start_timestamp(cube, db_tx)
        log.debug(f'{cube} Get Transactions from {ts}')
        import_transactions(cube, cube.exchange, creds, ts)
    except Exception as e:
        log.debug(e)
    try:
        log.debug(f'{cube} Get Trades from {ts}')
        import_trades(cube, cube.exchange, creds, ts)
    except Exception as e:
        log.debug(e)
        return False
        
    return True


def order_reconciliation(cube, ex, creds, bals):
    # Reconcile db orders
    log.debug(f'{cube} Reconciling database orders (API)')
    if cube.orders:
        cube_orders = cube.orders.copy()
        for order in cube_orders:
            if order.ex_pair.exchange.name != ex.name:
                # order for different exchange
                continue
            # Get order from exchange
            url = f'/order/{order.order_id}'
            quote_symbol = order.ex_pair.quote_currency.symbol
            base_symbol = order.ex_pair.base_currency.symbol
            args = {**creds, **{'base': base_symbol, 'quote': quote_symbol}}
            ex_order = api_request(cube, 'GET', ex.name, url, args)
            if ex_order and ex_order != 'InvalidOrder':
                # Reconcile order
                reconcile_order(cube, ex, order.order_id, ex_order, bals)
            # Cancel oustanding order
            cancel_order(
                cube.id, 
                ex.id, 
                order.order_id, 
                base_symbol, 
                quote_symbol
                ) 

    if ex.name != 'Binance':
        # Get api orders
        log.debug(f'{cube} Checking for rogue orders (API)')
        args = {**creds, **{'type': 'open'}}
        orders = api_request(cube, 'GET', ex.name, '/orders', args)
        # Cancel outstanding rogue orders
        if orders:
            log.debug(f'{cube} Rogue orders {orders}')
            # Reconcile exchange orders to known orders
            for order_id in orders:
                log.debug(f'Order ID {order_id}')
                # Cancel rogue orders and set unrecognized activity flag
                if order_id not in cube.all_orders:
                    log.info(f'{cube} Canceling order: {order_id} (rogue)')
                    cancel_order(cube.id, ex.id, order_id)
            cube.unrecognized_activity = True
            db_session.add(cube)
            db_session.commit()

    if ex.name == 'Binance':
        # Check for balance available/total mismatches
        for bal in cube.balances:
            if bal.total > bal.available:
                # Part of balance has been reserved due to an open trade
                ex_pairs = ExPair.query.filter_by(
                        exchange_id=ex.id,
                        active=True
                    ).filter(or_(
                        ExPair.base_currency_id == bal.currency_id,
                        ExPair.quote_currency_id == bal.currency_id,
                    )).all()
                # Check all possible pairs
                log.debug(f'{cube} Checking for rogue orders (API)')
                for ex_pair in ex_pairs:
                    quote_symbol = ex_pair.quote_currency.symbol
                    base_symbol = ex_pair.base_currency.symbol
                    args = {**creds, **{'base': base_symbol, 'quote': quote_symbol, 'type': 'open'}}
                    orders = api_request(cube, 'GET', ex.name, '/orders', args)
                    if orders:
                        for order_id in orders:
                            log.info(f'{cube} Canceling order: {order_id} (rogue)')
                            cancel_order(cube.id, ex.id, order_id, base_symbol, quote_symbol)
                        cube.unrecognized_activity = True
                        db_session.add(cube)
                        db_session.commit()                          


def set_last(cube):
    for bal in cube.balances:
        # Set last balance to current total
        bal.last = bal.total
        db_session.add(bal)
    db_session.commit()


@celery.task(base=SqlAlchemyTask)
def reconcile_cube(cube_id):
    try:
        cube = Cube.query.get(cube_id)
        # Reconcile cube
        for conn in cube.connections.values():
            ex = conn.exchange
            creds = get_api_creds(cube, ex)
            log.info(f'{cube} Reconciling {ex}')

            # Set last balance to total
            set_last(cube)

            # Get api balances
            log.debug(f'{cube} Getting balances (API)')
            bals = api_request(cube, 'GET', ex.name, '/balances', creds)

            if bals:
                # Reconcile orders
                order_reconciliation(cube, ex, creds, bals)
                # Reconcile exchange balances with db balances
                # Covers rogue orders, deposits, etc.
                log.debug(f'{cube} Reconciling Balances')
                reconcile_balances(cube, ex, bals)

            update_transactions(cube, creds)

    except SoftTimeLimitExceeded:
        update_cube_cache(cube_id, False)
        ## To do: error handling

def rebalance(cube, indiv, comb):
    log.debug(f'{cube} Rebalancing')
    r = False
    # Run optimization if needed
    if cube.trading_status == 'off':
        log.debug(f'{cube} Trading off (skipping)')
        r = False
    else:  
        if not cube.balanced_at:
            # Not previously balanced
            log.debug(f'{cube} Not previously balanced (running optimization)')
            cube.reallocated_at = datetime.utcnow()
            r = True
        elif (cube.reallocated_at and
            cube.auto_rebalance and
            (cube.reallocated_at >= cube.balanced_at)):
            # Recently reallocated. Need to rebalance
            log.debug(f'{cube} Recently reallocated (running optimization)')
            r = True
        elif cube.rebalance_interval and cube.auto_rebalance:
            if not cube.reallocated_at:
                cube.reallocated_at = datetime.utcnow()
            if ((cube.reallocated_at +
                timedelta(seconds=(cube.rebalance_interval))) <= datetime.utcnow()):
                log.debug(f'{cube} New rebalance interval (running optimization)')
                cube.reallocated_at = datetime.utcnow()
                r = True
        else:
            log.info(f'{cube} Optimization complete')
    if r:
        log.info(f'{cube} Running Optimization')
        try:
            indiv, comb = regression(cube, indiv, comb)
            print(indiv)
            print(comb)
            if indiv is None:  # just to be safe, should never happen
                log.info('No valid solution from regression.')
            return indiv, comb
        except:
            log.exception('Exception from regression function')


@celery.task(base=SqlAlchemyTask)
def new_orders(cube_id):  
    try:
        cube = Cube.query.get(cube_id)
        log.info(f'{cube} Generating Orders')
        #### Sanity Check ####
        if not sanity_check(cube):
            log.warning(f'{cube} failed sanity check')
            update_cube_cache(cube_id, False) 
            return    
            
        #### Individual Valuations ####
        indiv = calc_indiv(cube)
        log.debug(f'{cube} Individual valuations:\n{indiv}')
        comb = calc_comb(cube, indiv)
        log.debug(f'{cube} Combined valuations:\n{comb}')

        # Rebalance cubes
        if cube.algorithm.name in ['Centaur']:
            rebalance(cube, indiv, comb)

        if cube.trading_status == 'live':
            #### Generate Target Allocation Orders ####
            print(indiv, comb)
            orders = target_orders(cube, indiv, comb, orders=[])
            log.debug(f'{cube} Individual Orders:\n{pformat(orders)}')
            if orders:
                place_orders.delay(cube_id, orders)
            else:
                update_cube_cache(cube_id, False)
                cube.balanced_at = datetime.utcnow()  
                db_session.add(cube)
                db_session.commit()
                log.debug(f'{cube} No Orders')
            cube.suspended_at = datetime.utcnow()
            db_session.add(cube)
            db_session.commit()
        update_cube_cache(cube_id, False)
    except SoftTimeLimitExceeded:
        update_cube_cache(cube_id, False)
        ## To do: error handling

@celery.task(base=SqlAlchemyTask)
def process_cube(cube_id):
    try:
        cube = Cube.query.get(cube_id)
        cache = CubeCache.query.filter_by(cube_id=cube_id).first()
        log.debug(f'{cube} Processing')

        if cache and cache.processing == True:
            log.debug(f'{cube} already in cache')
            return
        else:
            log.debug(f'{cube} adding to cache')
            # Add to CubeCache
            update_cube_cache(cube_id, True)     

        log.debug(f'{cube} reconcile/generate new orders')
        #### Reconcile Cube/Generate New Orders ####
        chain(reconcile_cube.si(cube_id), new_orders.si(cube_id))()

    except SoftTimeLimitExceeded:
        update_cube_cache(cube_id, False)

def run_trader():
    # Find active Cubes
    active = and_(
        Cube.closed_at == None,
        Cube.trading_status == 'live',
        Cube.algorithm.has(Algorithm.name.in_(['Centaur'])),
        not_(Cube.connections.any(Connection.failed_at != None)),
        Cube.connections.any()    
        )

    try:
        cubes = Cube.query.filter(
            active,
            ).order_by(
            Cube.suspended_at
            ).all()

        log.debug(cubes)
        for cube in cubes:
            try:   
                log.debug(f'{cube} Starting')
                cache = CubeCache.query.filter_by(cube_id=cube.id).first()
                if cache and cache.processing == True:
                    if (cache.updated_at and
                        (cube.updated_at +
                        timedelta(hours=1)) <= datetime.utcnow()):
                        update_cube_cache(cube.id, False)  
                        log.debug(f'{cube} 1 hours past last update (processing)')
                        process_cube.delay(cube.id)
                    else:
                        log.debug(f'{cube} Already processing (skipping)')
                        continue
                elif not cube.balanced_at:
                    log.debug(f'{cube} Never balanced (processing)')
                    process_cube.delay(cube.id)
                elif (cube.reallocated_at and
                    (cube.reallocated_at >= cube.balanced_at)):
                    log.debug(f'{cube} Recently reallocated (processing)')
                    process_cube.delay(cube.id)
                elif cube.orders:
                    log.debug(f'{cube} Open orders (processing)') 
                    process_cube.delay(cube.id)                   
                elif (cube.suspended_at and
                    (cube.suspended_at + timedelta(minutes=10)) > datetime.utcnow()):
                    log.debug(f'{cube} less than 10 minutes since last update (skipping)')
                    continue
                else:
                    log.debug(f'{cube} Scheduled run (processing)')
                    process_cube.delay(cube.id)
                
            except Exception as e:
                log.exception('Unhandled exception')

    except Exception as e:
        log.exception('Main thread exception')
    finally:
        db_session.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_trader()
