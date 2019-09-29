from decimal import Decimal as dec

from tools import (trunc, add_new_balance, cur_ids_from_balances)
from database import *

FEE_THRESH = dec('0.01')


def remove_delisted(cube, ex):
    log.debug(f'{cube} Removing delisted')
    for bal in cube.balances:
        ex_pair = ExPair.query.filter_by(
            exchange_id=ex.id,
            active=True
        ).filter(or_(
            ExPair.quote_currency_id == bal.currency.id,
            ExPair.base_currency_id == bal.currency.id
        )).first()
        if not ex_pair:
            log.debug(f'{ex_pair} not active so {bal} is being set to 0...')
            bal.available = 0
            bal.last = 0
            bal.total = 0
            db_session.delete(bal)
    db_session.commit()


def get_currency(exchange, symbol):
    currency = Currency.query.filter_by(symbol=symbol).first()
    if currency:
        ex_pair = ExPair.query.filter_by(
            exchange_id=exchange.id,
            active=True
        ).filter(or_(
            ExPair.quote_currency_id == currency.id,
            ExPair.base_currency_id == currency.id
        )).first()
    else:
        ex_pair = ExPair.query.filter_by(
            exchange_id=exchange.id,
            active=True
        ).filter(or_(
            ExPair.quote_symbol == symbol,
            ExPair.base_symbol == symbol
        )).first()
    if not ex_pair:
        # ex_pair does not exist (currency not supported)
        raise ValueError('%s %s not supported' % (exchange, symbol))

    if ex_pair.quote_symbol == symbol:
        return ex_pair.quote_currency
    else:
        return ex_pair.base_currency


def add_virgin_bals(cube, ex):
    # Check for virgin currencies
    # Some exchanges omit never-traded/deposited currencies from balance query
    if ex.name not in ['Coinbase Pro', 'Poloniex', 'Bitstamp']:
        log.debug(f'{ex} {cube} Adding missing API balances')
        # GDAX only includes tradeable currencies
        # Poloniex, Bitstamp returns all virgin currencies
        # Bitfinex, Kraken, Bittrex only return deflowered currencies
        eps = ExPair.query.filter_by(
            exchange=ex,
            active=True
        ).all()
        all_curs = {}
        for ep in eps:
            all_curs[ep.quote_currency] = None
            all_curs[ep.base_currency] = None
        # Get currently accounted for currency ids
        cur_ids = cur_ids_from_balances(cube, ex)
        for cur in all_curs:
            if cur.id not in cur_ids:
                # Virgin currency
                # Don't set cube yet (so balance doesn't commit if tx creation fails)
                try:
                    log.debug(f'{ex} {cube} Adding missing balance for {cur}')
                    add_new_balance(cube, cur, ex, 0, 0, 0)
                    # Add new cur.id
                    cur_ids.append(cur.id)
                except Exception as e:
                    log.warning(f'{ex} {cube} Problem adding virgin balance {e}')


def update_filled(cube, ex, order_id, order):
    log.debug('%s Update filled for %s' % (cube, order))

    # New fill amount
    new_fill = dec(order['filled']) - cube.all_orders[order_id].filled

    if order['avg_price']:
        avg_price = order['avg_price']
    else:
        avg_price = 0

    # Update order
    cube.all_orders[order_id].unfilled -= new_fill
    cube.all_orders[order_id].filled += new_fill
    cube.all_orders[order_id].avg_price = avg_price
    db_session.add(cube)
    db_session.commit()


def reconcile_balances(cube, ex, bals):
    log.debug(f'{cube} reconciling balances for {ex}')
    #### Add virgin balances
    add_virgin_bals(cube, ex)

    #### Remove balances for de-listed assets ####
    remove_delisted(cube, ex)

    #### Check for new balances ####
    new_bals = {}
    for sym in bals:
        try:
            cur = get_currency(ex, sym)
        except ValueError as e:
            # Currency not supported
            continue

        if cur:
            current_bal = Balance.query.filter_by(
                                cube_id=cube.id,
                                exchange_id=ex.id,
                                currency_id=cur.id
                                ).first()
            # Balance exists
            if current_bal:
                continue
        else:
            continue

        # New balance
        log.debug(f'{ex} {cube} Adding missing balance for {sym}')
        bal = Balance(
            cube=cube,
            exchange_id=ex.id,
            currency_id=cur.id,
            available=bals[sym]['total'],
            total=bals[sym]['total'],
            last=bals[sym]['total'],
        )
        db_session.add(bal)
        db_session.commit()
        new_bals[cur.id] = bals[sym]
        db_session.refresh(cube)

    log.debug(f'{cube} new balances {new_bals}')

    #### Reconcile exchange balances with db balances ####
    for bal in cube.balances:

        if bal.exchange.name != ex.name:
            # Balance for different exchange
            continue

        if bal.currency.symbol not in bals:
            # Balance unavailable from exchange
            continue

        # Update available balance
        bal.available = trunc(bals[bal.currency.symbol]['total'])
        bal.total = trunc(bals[bal.currency.symbol]['total'])
        bal.last = trunc(bals[bal.currency.symbol]['total'])
        db_session.add(bal)
        db_session.commit()


def reconcile_order(cube, ex, order_id, order, bals):
    log.debug(f'{cube} Reconcile order: {order_id} {order}')
    if order_id in cube.all_orders:
        if not cube.all_orders[order_id].ex_pair.active:
            # Expair not active. Skip.
            log.warning(f'{ex} {cube} Deleting order {order_id} ex_pair inactive)')
            del cube.all_orders[order_id]
            db_session.add(cube)
            db_session.commit()
            return

        # Update filled
        update_filled(cube, ex, order_id, order)
