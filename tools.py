from decimal import Decimal as dec
import pandas as pd
from sqlalchemy.orm.exc import NoResultFound

from utils.api import get_price
from database import *

DUST_AMOUNT = 9e-8

log = logging.getLogger(__name__)


def trunc(d, places=8):
    # returns d, cast to a decimal truncated to 8 decimal places
    return dec(d).quantize(dec('1e-%d' % places), rounding='ROUND_DOWN')


def cur_ids_from_balances(cube, ex):
    bals = Balance.query.filter_by(cube_id=cube.id, exchange_id=ex.id).all()
    cur_ids = []
    if bals:
        cur_ids = [bal.currency_id for bal in bals]
    return cur_ids


def update_cube_cache(cube_id, processing):
    if processing == True:
        log.info(f'Cube: {cube_id} Add to Cache')
    else:
        log.info(f'Cube: {cube_id} Remove from Cache')
    cache = CubeCache.query.filter_by(cube_id=cube_id).first()
    if cache:
        cache.processing = processing
        db_session.add(cache)
        db_session.commit()
    else:
        cache = CubeCache(
            cube_id=cube_id,
            processing=processing
        )
        db_session.add(cache)
        db_session.commit()


def get_ex_pair(ex, base, quote):
    ex_pair = ExPair.query.filter_by(
        active=True,
        exchange_id=ex.id
    )
    try:
        ex_pair = ex_pair.filter_by(
            quote_currency_id=quote.id,
            base_currency_id=base.id,
        ).one()
        inverted = False
    except NoResultFound:
        try:
            ex_pair = ex_pair.filter_by(
                base_currency_id=quote.id,
                quote_currency_id=base.id,
            ).one()
            inverted = True
        except NoResultFound:
            raise ValueError(f'No ExPair involving {base} \
                             and {quote} on {ex}')
    log.debug(f'{ex_pair} inverted={inverted}')
    return ex_pair, inverted


def get_ex_pairs(exs, base, quote=None, inverts=True):
    query = ExPair.query.filter_by(active=True).filter(
                ExPair.exchange_id.in_([e.id for e in exs]))
    ex_pairs = []
    res = query.filter_by(base_currency_id=base.id)
    if quote:
        res = res.filter_by(quote_currency_id=quote.id)
    res = res.all()
    ex_pairs.extend([(r, False) for r in res])
    if inverts:
        res = query.filter_by(quote_currency_id=base.id)
        if quote:
            res = res.filter_by(base_currency_id=quote.id)
        res = res.all()
        ex_pairs.extend([(r, True) for r in res])
    return ex_pairs


def add_new_balance(cube, cur, ex, available, total, last):
    bal = Balance(
        cube=cube,
        exchange_id=ex.id,
        currency_id=cur.id,
        available=available,
        total=total,
        last=last
    )
    db_session.add(bal)
    db_session.commit()


def add_or_update_balance(cube, cur, ex, available, total, last):
    bal = Balance.query.filter_by(
                    cube_id=cube.id,
                    currency_id=cur.id,
                    exchange_id=ex.id
                    ).first()
    if bal:
        bal.available = available
        bal.total = total
        bal.last = last
        db_session.add(bal)
        db_session.commit()
    else:
        bal = Balance(
            cube=cube,
            exchange_id=ex.id,
            currency_id=cur.id,
            available=available,
            total=total,
            last=last
        )
        db_session.add(bal)
        db_session.commit()


def sanity_check(cube):
    # Assert allocation percent totals 100
    total = 0
    count = 0
    for a in cube.allocations.values():
        try:
            total += a.percent
        except TypeError:
            # percent is None??
            pass
        else:
            count += 1
    if total == 0:
        # No allocations set yet
        return False
    elif abs(total - 1) > count * 0.0001:
        # Pass this along to the user or trigger new allocations?
        log.warning(f'{cube} Allocations total {total}')
        log.warning(f'{cube} Attempting normalization of percentages')
        try:
            for a in cube.allocations.values():
                a.percent = dec(a.percent) / dec(total)
                db_session.add(a)
            db_session.commit()
            return True
        except Exception as e:
            log.debug(e)
            return False
    else:
        log.debug(f'{cube} Allocations total {total}')

    # Skip 0 balance cubes
    bal_tot = sum([bal.total for bal in cube.balances])
    if not bal_tot:
        log.warning(f'{cube} Zero balance')
        return False

    # Create 0% allocation for missing ones (balance available)
    curs = set([b.currency for b in cube.balances])
    for c in curs:
        if c.symbol not in cube.allocations:
            log.warning(f'{cube} Missing {c} allocation (setting to 0)')
            cube.allocations[c.symbol] = AssetAllocation(
                currency=c,
                percent=0
            )
    db_session.add(cube)
    db_session.commit()
    return True


def calc_indiv(cube):
    quotes = []
    ex_pairs = ExPair.query.filter_by(
        exchange_id=cube.exchange.id,
        active=True
        ).all()
    for ex_pair in ex_pairs:
        if ex_pair.quote_currency not in quotes:
            quotes.append(ex_pair.quote_currency)

    indiv = []
    for bal in cube.balances:
        i = {
            'cur_id': bal.currency.id,
            'cur': bal.currency,
            'ex': bal.exchange,
            'ex_id': bal.exchange.id,
            'bal': bal.total,
            'bal_tgt': bal.target
        }
        if bal.currency == cube.val_cur:
            i['price'] = 1
            i['val'] = i['bal']
        else:
            if ((i['bal'] <= DUST_AMOUNT) and
                    not cube.allocations[bal.currency.symbol].percent and
                    i['cur'].symbol != cube.val_cur.symbol):
                if bal.currency in quotes:
                    # Do not ignore quote currencies
                    log.debug('[Cube %d] Not ignoring unallocated zero balance %s (quote currency)' %
                              (cube.id, i['cur']))
                else:
                    # No balance, not allocated, and not a routed currency
                    continue
            if (i['bal'] <= DUST_AMOUNT) and (i['ex'].name in ['External', 'Manual']):
                log.debug('%s Ignoring External and Manual zero balance %s'
                          % (cube, i['cur']))
                continue
            try:
                ex_pair, inverted = get_ex_pair(bal.exchange, bal.currency, cube.val_cur)
            except ValueError as e:
                log.warning('[Cube %d] Ignoring %f balance (%s)' %
                         (cube.id, i['bal'], e.args[0]))
                continue
            try:
                quote_symbol = ex_pair.quote_currency.symbol
                base_symbol = ex_pair.base_currency.symbol
                i['price'] = get_price(ex_pair.exchange.name, base_symbol, quote_symbol)
            except Exception as e:
                log.warning(f'{cube} Price query failed for {ex_pair}')
                log.warning(e)
                i['price'] = ex_pair.get_close()
            if inverted:
                i['price'] = 1 / i['price']
            i['val'] = i['bal'] * i['price']
        indiv.append(i)
    indiv = pd.DataFrame(indiv)
    indiv['price'] = indiv['price'].astype(float)
    indiv['bal'] = indiv['bal'].astype(float)
    indiv['bal_tgt'] = indiv['bal_tgt'].astype(float)
    indiv['val'] = indiv['val'].astype(float)
    indiv = indiv.set_index(['cur_id', 'ex_id']).sort_index()
    return indiv


def calc_comb(cube, indiv):
    comb = indiv.groupby(level='cur_id').agg({
        'bal': 'sum',
        'val': 'sum',
        'cur': 'first',
        'price': 'mean'
    })
    comb.loc[comb['bal'] > 0, 'price'] = comb['val'] / comb['bal']
    comb['pct_tgt'] = comb['cur'].apply(
        lambda cur: cube.allocations[cur.symbol].percent) / 1
    comb['pct_tgt'] = comb['pct_tgt'].astype(float)

    # Ignore currencies with missing prices
    comb = comb[comb.price.notnull()].copy()
    comb.pct_tgt = comb.pct_tgt / comb.pct_tgt.sum()

    comb['val_tgt'] = comb['val'].sum() * comb['pct_tgt']
    return comb


