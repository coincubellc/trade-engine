from database import *
from decimal import Decimal as dec
from math import trunc as truncate

from tools import trunc, get_ex_pair
from .api import get_api_creds, api_request, record_api_key_error, get_price


MAX_VAL = 0.25  # BTC


def add_new_order(cube, ex_pair_id, order_id, side, price, amount):
    order = Order(
        cube_id=cube.id,
        ex_pair_id=ex_pair_id,
        order_id=order_id,
        side=side,
        price=trunc(price),
        amount=trunc(amount),
        filled=0,
        unfilled=trunc(amount),
        avg_price=0,
        pending=True,
    )
    log.info(f'{cube} New order {order}')
    db_session.add(order)
    db_session.commit()


def bals_from_order(cube, ex_id, order_id):
    bal_base = Balance.query.filter_by(
        cube_id=cube.id,
        currency_id=cube.all_orders[order_id].ex_pair.base_currency_id,
        exchange_id=ex_id).one()
    bal_quote = Balance.query.filter_by(
        cube_id=cube.id,
        currency_id=cube.all_orders[order_id].ex_pair.quote_currency_id,
        exchange_id=ex_id).one()
    return bal_base, bal_quote


def cancel_order(cube_id, exchange_id, order_id, base=None, quote=None):
    cube = Cube.query.get(cube_id)
    ex = Exchange.query.filter_by(id=exchange_id).first()
    log.info(f'{ex} {cube} Canceling order: {order_id}')
    try:
        # Get API credentials
        creds = get_api_creds(cube, ex)
        params = {
            # Auth credentials
            **creds,
            # Order details
            'base': base,
            'quote': quote
        }
        endpoint = f'/order/{order_id}'
        if api_request(cube, 'DELETE', ex.name, endpoint, params):
            delete_order(cube, order_id)
            return order_id
        else:
            delete_order(cube, order_id)
            log.debug(f'{cube} order {order_id} not found')
    except:
        raise


def delete_order(cube, order_id):
    try:
        # Delete from database
        del cube.all_orders[order_id]
        db_session.add(cube)
        db_session.commit()
    except KeyError:
        pass


def place_order(cube_id, ex_pair_id, side, amount, price):
    cube = Cube.query.get(cube_id)
    ex_pair = ExPair.query.filter_by(id=ex_pair_id).first()
    log.info(f'{ex_pair.exchange} {cube} Placing {side} order for {amount} \
             {ex_pair.base_currency.symbol} @ {price} {ex_pair.quote_currency.symbol}')
    # Get API credentials
    creds = get_api_creds(cube, ex_pair.exchange)

    # Params
    params = {
        # Auth credentials
        **creds,
        # Order details
        'side': side,
        'amount': amount,
        'price': price,
        'base': ex_pair.base_currency.symbol,
        'quote': ex_pair.quote_currency.symbol,
        'type': 'limit',
    }
    # Place order on exchange
    order_id = api_request(cube, 'POST', ex_pair.exchange.name, '/orders', params)
    log.debug(order_id)
    if order_id and order_id == 'InvalidOrder':
        log.debug(f'{cube} {ex_pair.exchange} {ex_pair.base_currency} reached target (below trade minimum)')
        b = Balance.query.filter_by(
            cube=cube,
            exchange=ex_pair.exchange,
            currency=ex_pair.base_currency
        ).first()
        b.target = None
        db_session.add(b)
        db_session.commit()
        return None
    if order_id and 'error' in order_id:
        record_api_key_error(cube, ex_pair.exchange.name, order_id['error'])
        return None
    if order_id:
        try:
            # Add to open_orders table
            add_new_order(cube, ex_pair.id, order_id, side, price, amount)
            return order_id
        except Exception as e:
            log.warn(f'{cube} {ex_pair} Unable to add order to database')
            log.warn(e)
            return None
    else:
        log.debug(f'{cube} {ex_pair} Unable to place order')


def failsafe(cube, i):
    # Valuation currency is balanced via other currencies
    if i.cur == cube.val_cur:
        log.debug(f'{cube} {i.ex} {i.cur} Valuation currency \
                    balanced via other currencies')
        b = Balance.query.filter_by(
            cube=cube,
            exchange=i.ex,
            currency=i.cur
        ).first()
        b.target = None
        db_session.add(b)
        return True
    # Target is removed when reached
    # Check for either None or nan...
    if (i.bal_tgt is None) or (i.bal_tgt != i.bal_tgt):
        log.debug(f'{cube} {i.ex} {i.cur} already at target')
        return True
    # Failsafe: if external or manual, clear balance target and continue (cannot trade)
    if i.ex.name in ['External', 'Manual']:
        b = Balance.query.filter_by(
            cube=cube,
            exchange=i.ex,
            currency=i.cur
        ).first()
        log.warning(f'{cube} Resetting balance target for {i.ex} {i.cur}')
        b.target = None
        db_session.add(b)
        return True
    return False


def get_order_details(cube, ex_pair, bal_diff, price, inverted):
    # Determine order side
    if bal_diff > 0:
        side = 'sell'
    else:
        side = 'buy'
    # Determine order size
    amount = abs(bal_diff)
    val = float(amount) * float(price)
    # Limit order size if necessary
    # Need to think of new solution for order size max calculation
    # Perhaps use a percentage of the last candle volume?
    # if val > MAX_VAL:
    #     log.debug(f'{cube} Limiting {ex_pair}')
    #     val = MAX_VAL
    #     amount = float(val) / float(price)
    # Flip order data if base/quote inverted
    if inverted:
        log.debug(f'Flipping order data for {ex_pair}')
        side = 'buy' if side == 'sell' else 'sell'
        amount, val = val, amount
        price = 1 / price
        amount = amount * 0.99
    log.debug(f'Amount={amount}, Price={price}, Value={val}')
    return side, amount, val, price


def below_trade_min(cube, ex_pair, bal_tgt, val_diff_pct, amount, val):
    # Get currency
    ex = ex_pair.exchange
    # Check if order meets trade minimum
    try:
        params = {
            'base': ex_pair.base_currency.symbol,
            'quote': ex_pair.quote_currency.symbol
        }
        d = api_request(
                cube, 
                'GET', 
                ex_pair.exchange.name, 
                '/details',
                params
                )
    except AttributeError:
        # Exchange is probably external... not performing minimum checks
        log.warning(f'{cube} No details for {ex_pair}')
        d = None
    except Exception as e:
        log.error(e)
        d = None
    else:
        if d and (amount < d['min_amt'] or val < d['min_val']):
            # Below trade minimum
            # Consider the target reached
            log.debug(f'{cube} {ex} {ex_pair.base_currency} reached target')
            b = Balance.query.filter_by(
                cube=cube,
                exchange=ex,
                currency=ex_pair.base_currency
            ).first()
            b.target = None
            db_session.add(b)
            db_session.commit()
            return True
    if bal_tgt != 0:
        if abs(dec(val_diff_pct)) < dec(cube.threshold / 100):
            # Below threshold, target reached
            log.info('%s %s %s below threshold' % (cube, ex, ex_pair.base_currency))
            b = Balance.query.filter_by(
                cube=cube,
                exchange=ex,
                currency=ex_pair.base_currency
            ).first()
            b.target = None
            db_session.add(b)
            db_session.commit()
            return True
    return False


def throttle_order(cube, ex_pair, indiv, ex_id, side, amount, val, price):
    # Check for available balance
    if side == 'sell':
        bal = indiv['bal'][ex_pair.base_currency_id, ex_id]
        if amount > bal:
            # Insufficient balance. Reducing order amount
            amount = bal
            val = amount * price
            log.debug(f'{cube} Reducing {ex_pair} order to {amount} \
                        {ex_pair.base_currency}')
    else:  # buy
        bal = indiv['bal'][ex_pair.quote_currency_id, ex_id]
        if val > bal:
            # Insufficient balance. Reducing order amount
            val = bal
            amount = val / price
            log.debug(f'{cube} Reducing {ex_pair} order to {amount} \
                       {ex_pair.base_currency}')

    log.debug(f'{cube} truncating precision')
    try:
        params = {
            'base': ex_pair.base_currency.symbol,
            'quote': ex_pair.quote_currency.symbol
        }
        d = api_request(
                cube, 
                'GET', 
                ex_pair.exchange.name, 
                '/details',
                params
                )
    except AttributeError:
        # Exchange is probably external... not performing minimum checks
        log.warning(f'{cube} No details for {ex_pair}')
        d = None
    else:
        if d:
            if '.' in str(d['min_amt']):
                precision = len(str(d['min_amt']).split('.')[1])
                amount = round(amount, precision)
                log.debug(f'Precision: {precision}')
            elif amount >= 1:
                amount = truncate(amount)
            
            log.debug(f'Amount: {amount}')

    return amount, val


def create_order(cube, ex_pair, i, indiv, price, orders, inverted=False):
    side, amount, val, price = get_order_details(cube, ex_pair, i.bal_diff, price, inverted)

    if below_trade_min(cube, ex_pair, i.bal_tgt, i.val_diff_pct, amount, val):
        return False

    amount, val = throttle_order(cube, ex_pair, indiv, ex_pair.exchange.id, side, amount, val, price)

    orders.append((amount, price, ex_pair.id, side))
    db_session.commit()
    return True


def secondary_pairs(cube, indiv, comb, orders):
    log.debug(f'{cube} running secondary pairs')
    for (cur_id, ex_id), i in indiv.iterrows():
        # Check to see if any of the other assets (except val_cur which is BTC) has a surplus or deficit which is opposed to this asset
        # Find all active ex_pairs on this exchange which are not val_cur

        if failsafe(cube, i):
            continue

        ex_pairs = ExPair.query.filter_by(
                                    exchange_id=ex_id,
                                    active=True
                                    ).filter(
                                    ExPair.base_currency == i.cur,
                                    ExPair.quote_currency != cube.val_cur,
                                    ).all()
        if ex_pairs:
            for ex_pair in ex_pairs:
                if cube.algorithm.name == 'Sphinx':
                    price = get_price(ex_pair.exchange.name, ex_pair.base_currency.symbol, ex_pair.quote_currency.symbol)
                    if create_order(cube, ex_pair, i, indiv, price, orders, inverted=False):
                        log.debug(f'{cube} order created for {ex_pair}')
                else:
                    # Check for corresponding positive or negative value for quote
                    quote_val_diff = indiv.loc[(ex_pair.quote_currency_id, ex_id), 'val_diff']
                    # Make sure quote val_diff >= base val_diff
                    # We don't want to trade more of the base value than the quote value
                    if abs(quote_val_diff) >= abs(i.val_diff):
                        # Get ex_pair price
                        price = get_price(ex_pair.exchange.name, ex_pair.base_currency.symbol, ex_pair.quote_currency.symbol)
                        if create_order(cube, ex_pair, i, indiv, price, orders, inverted=False):
                            log.debug(f'{cube} order created for {ex_pair}')
                            # Offest val_diff for base and quote so as not to attempt to place more trades than possible
                            indiv.loc[(ex_pair.quote_currency_id, ex_id), 'val_diff'] += i.val_diff
                            indiv.loc[(ex_pair.base_currency_id, ex_id), 'val_diff'] += quote_val_diff


def primary_pairs(cube, indiv, comb, orders):
    log.debug(f'{cube} running primary pairs')
    for (cur_id, ex_id), i in indiv.iterrows():

        if failsafe(cube, i):
            continue

        # Get expair and price
        ex_pair, inverted = get_ex_pair(i.ex, i.cur, cube.val_cur)
        price = comb['price'][cur_id]

        if create_order(cube, ex_pair, i, indiv, price, orders, inverted):
            log.debug(f'{cube} order created for {ex_pair}')


def target_orders(cube, indiv, comb, orders):

    # Determine balance difference between current and target
    indiv['bal_diff'] = indiv.bal - indiv.bal_tgt
    indiv['bal_diff_pct'] = indiv.bal_diff / indiv.bal
    # Determine value difference
    indiv['val_nnls'] = indiv.bal_tgt * indiv.price
    indiv['val_diff'] = indiv.val - indiv.val_nnls
    indiv['val_diff_pct'] = indiv.val_diff / indiv.val
    # Sort by value difference descending
    # this allows surpluses to be sold first
    # and the most assets to be properly allocated (large deficits are addressed last)
    indiv = indiv.sort_values('val_diff', ascending=False)
    log.debug('%s Individual balances\n%s' %
              (cube, indiv[['cur', 'ex', 'bal', 'bal_tgt', 'bal_diff', 'val_diff']]))

    try:
        # Place secondary pair trades
        secondary_pairs(cube, indiv, comb, orders)
    except Exception as e:
        log.debug(f'{cube} exeception in secondary pair: {e}')
        pass
    # Place primary pair trades
    primary_pairs(cube, indiv, comb, orders)

    # Check to see if Cube is balanced
    for b in cube.balances:
        if b.target != None:
            break
    else:
        now = datetime.utcnow()
        log.debug(f'{cube} balanced at {now}')
        cube.balanced_at = now
        db_session.add(cube)
        db_session.commit()

    return orders

