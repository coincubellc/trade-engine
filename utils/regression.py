from optimizer import solve_allocations, calculate_transfers
from database import *


def regression(cube: Cube, indiv, comb, **kwargs):
    # Perform non-negative linear regression to determine individual vals
    # Set individual balance targets in db

    indiv_sol, comb_sol = solve_allocations(indiv, comb, cube.val_cur.id, **kwargs)
    if indiv_sol is not None:
        log.info("Found L1 solution.")
        cube.requires_exchange_transfer = False
        indiv, comb = indiv_sol, comb_sol
    else:
        # Try to solve without exchange constraints
        cube.requires_exchange_transfer = True
        indiv, comb = solve_allocations(indiv, comb, cube.val_cur.id, L1=False, **kwargs)
        if indiv is not None:
            log.info("Found L2 solution.")
        else:
            log.info("No solution.")
            return None, None

    # Use indiv prices 
    indiv['bal_tgt'] = indiv.val_nnls / indiv.price

    indiv = indiv.reset_index().set_index(['cur_id', 'ex_id'])

    # Calculate combined nnls
    comb['val_nnls'] = indiv.groupby(level='cur_id').val_nnls.sum()
    comb['pct_nnls'] = comb.val_nnls / comb.val_nnls.sum()

    # Logging
    log.debug('%s Individual valuations\n%s' %
              (cube, indiv.reset_index().loc[:, ['ex_id', 'cur_id', 'val', 'val_nnls']]))
    log.debug('%s Combined valuations\n%s' %
              (cube, comb.loc[:, ['cur', 'val', 'val_tgt', 'val_nnls', 'pct_tgt', 'pct_nnls']]))
    log.debug('%s Total valuations\n%s' %
              (cube, comb.loc[:, ['val', 'val_tgt', 'val_nnls', 'pct_tgt', 'pct_nnls']].sum()))

    set_target_balances(cube, indiv)

    ## Legacy: transfer details not needed in current version of Coincube
    # if cube.requires_exchange_transfer:
    #     buy_amounts, transfers, sell_amounts = calculate_transfers(indiv)
    #     transfer_details = {
    #         'buy_amounts': buy_amounts,
    #         'transfers': transfers,
    #         'sell_amounts': sell_amounts,
    #     }
    # else:
    #     transfer_details = {}
    print(indiv)
    print(comb)
    print('end regression')
    return indiv, comb


def set_target_balances(cube, indiv):
    # set balance targets
    for b in cube.balances:
        try:
            b.target = float(indiv['bal_tgt'][b.currency_id, b.exchange_id])
            # convert nan to None
            if b.target != b.target:
                b.target = 0
            if b.target == float("inf"):
                b.target = 0
        except KeyError:
            b.target = None

        db_session.add(b)

    db_session.add(cube)
    db_session.commit()