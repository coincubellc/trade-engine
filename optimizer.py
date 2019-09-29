import logging
from collections import defaultdict
from enum import Enum, auto
from typing import Dict

import numpy as np
import pandas as pd
# noinspection PyUnresolvedReferences
import pyomo.environ
from pyomo.core import ConcreteModel, Var, NonNegativeReals, Constraint, Objective
from pyomo.opt import SolverFactory
from pyutilib.common import ApplicationError
from scipy.optimize import lsq_linear

SOLVER_PATH = '/opt/conda/bin/ipopt'
SOLVE_TIME_LIMIT = 90
SOLVE_MAX_ITERATIONS = 20000


log = logging.getLogger(__name__)


class UseRegression(Enum):
    force = auto()
    fallback = auto()
    forbid = auto()


def solve_allocations(indiv, comb, val_cur, L1=True, **kwargs):
    if L1:
        return solve_L1(indiv, comb, val_cur, **kwargs)

    return solve_L2(indiv, comb, val_cur, **kwargs)


def solve_L2(indiv, comb, val_cur, use_regression=UseRegression.fallback,
             solver_params=None, L1=False):
    """ Tries to find individual allocations according to the comb requirements.
    First, the function tries to find a solution using pyomo and Ipopt. If that fails,
    the regression function is tried. if that also fails, (None, None) is returned.
    """
    indiv, comb = indiv.copy(), comb.copy()
    indiv = indiv.reset_index()

    xc_funds = indiv.groupby('ex_id').sum().val.to_dict()
    target_pct = comb.val_tgt.to_dict()
    cur_bals = indiv.reset_index().val.to_dict()

    # find solution using pyomo
    sol_df, sol = find_solutions(indiv, xc_funds, target_pct,
                                 cur_bals, external_bals=None,
                                 solver_params=solver_params, L1=L1)

    if sol_df is not None and use_regression is not UseRegression.force:
        indiv_sol = indiv.set_index(['cur_id', 'ex_id'])
        indiv_sol['val_nnls'] = sol_df.val_nnls
        indiv_sol.reset_index(inplace=True)

        lp_success = verify_solution(indiv_sol, target_pct, xc_funds,
                                     exchange_constraints=L1)
        if lp_success:
            log.debug("Found a solution using pyomo/ipopt.")
            return indiv_sol, comb

    lp_success = False
    # try regression if no solution
    if ((sol_df is None or not lp_success) and
            use_regression is not UseRegression.forbid):
        indiv = perform_regr_L2(indiv, comb, val_cur)
        r_success = verify_solution(indiv, target_pct, xc_funds,
                                    exchange_constraints=L1)
        if r_success:
            log.debug("Found a solution using scipy.lsq_linear")
            return indiv, comb

    # no solution found
    return None, None


def solve_L1(indiv, comb, val_cur, use_regression=UseRegression.fallback,
             solver_params=None, L1=True, ):
    """ Tries to find individual allocations according to the comb requirements.
    First, the function tries to find a solution using pyomo and Ipopt. If that fails,
    the regression function is tried. if that also fails, (None, None) is returned.
    """
    indiv, comb = indiv.copy(), comb.copy()
    indiv = indiv.reset_index()

    xc_funds = indiv.groupby('ex_id').sum().val.to_dict()
    target_pct = comb.val_tgt.to_dict()
    cur_bals = indiv.reset_index().val.to_dict()

    # find solution using pyomo
    sol_df, sol = find_solutions(indiv, xc_funds, target_pct,
                                 cur_bals, solver_params=solver_params, L1=L1)

    lp_success = False
    if sol_df is not None and use_regression is not UseRegression.force:
        indiv_sol = indiv_sep.set_index(['cur_id', 'ex_id'])
        indiv_sol['val_nnls'] = sol_df.val_nnls

        indiv_sol.reset_index(inplace=True)
        del indiv_sol['index']
        lp_success = verify_solution(indiv_sol, target_pct, xc_funds,
                                     exchange_constraints=L1)
        if lp_success:
            log.debug("Found a solution using pyomo/ipopt.")
            return indiv_sol, comb

    # try regression if no solution
    if ((sol_df is None or not lp_success) and
            use_regression is not UseRegression.forbid):
        indiv = perform_regr(indiv, comb, val_cur)
        r_success = verify_solution(indiv, target_pct, xc_funds)
        if r_success:
            log.debug("Found a solution using scipy.lsq_linear")
            return indiv, comb

    # no solution found
    return None, None


def find_solutions(df, xc_funds, target_pct, cur_bals,
                   tee=False, start_vals=None,
                   solver_params=None, L1=True):
    """ Find solutions using pyomo / Ipopt.
    """

    m = build_model(df, xc_funds, target_pct, cur_bals, L1_constraints=L1)
    if start_vals is not None:
        assert len(start_vals) == len(m.bals)
        for idx, val in enumerate(start_vals):
            m.bals[idx] = val

    opt = SolverFactory("ipopt")
    opt.set_executable(name=SOLVER_PATH, validate=True)
    opt.options['max_cpu_time'] = SOLVE_TIME_LIMIT
    opt.options['max_iter'] = SOLVE_MAX_ITERATIONS
    if solver_params is not None:
        for k, v in solver_params.items():
            opt.options[k] = v
    try:
        results = opt.solve(m, tee=tee)
    except (ValueError, ApplicationError):
        try:
            opt = SolverFactory("ipopt")
            opt.set_executable(name=SOLVER_PATH, validate=True)
            opt.options['hessian_approximation'] = 'limited-memory'
            results = opt.solve(m, tee=tee)
        except (ValueError, ApplicationError):
            return None, None

    m.solutions.store_to(results)
    return parse_solution(df, results)


def parse_solution(df, results):
    """ Convert pyomo solution object to a dataframe.
    """
    sol = results['Solution']
    if len(sol) == 0:
        return None, None

    # assert len(sol) == 1
    v = sol[0]['Variable']

    rdf = pd.DataFrame([None] * len(df), index=df.set_index(['cur_id', 'ex_id']).index)
    for n in v.keys():
        id = n.split('[')[1].split(']')[0]
        rdf.iloc[int(id)] = v[n]['Value']

    rdf.rename(columns={0: 'val_nnls'}, inplace=True)
    return rdf, sol


def build_model(df, xc_funds, target_pct, cur_bals, L1_constraints=True):
    """ Construct pyomo model.
    """
    m = ConcreteModel()
    m.bals = Var(range(len(df)), domain=NonNegativeReals)
    df = df.reset_index()

    currencies = df.cur_id.unique()

    # create the model variables for currencies
    for cur in currencies:
        idx = np.where(df.cur_id == cur)[0]
        m.__setattr__(f'c_{str(cur).lower()}_idx', Var(idx, domain=NonNegativeReals))

    if L1_constraints:  # create the model variables for exchanges
        for x in df.ex_id.unique():
            idx = np.where(df.ex_id == x)[0]
            m.__setattr__(f'x_{str(x).lower()}_idx', Var(idx, domain=NonNegativeReals))

    m.indices = list(range(len(df)))

    if L1_constraints:
        # create the constraints that specify that excahnge sums shouldn't change
        for x in df.ex_id.unique():
            expr = sum(m.bals[i] for i in eval(f'm.x_{str(x).lower()}_idx')) == xc_funds[x]
            m.__setattr__(f'x_{str(x).lower()}_constraint', Constraint(expr=expr))
    else:  # for non L1 optimization, just the total funds sum
        expr = sum(m.bals[i] for i in m.indices) == sum(xc_funds.values())
        m.__setattr__(f'xc_sum_constraint', Constraint(expr=expr))

    for c in currencies:
        expr = (sum(m.bals[i] for i in eval(f'm.c_{str(c).lower()}_idx'))
                == target_pct[c])
        m.__setattr__(f'c_{str(c).lower()}_constraint', Constraint(expr=expr))

    m.obj = Objective(expr=sum(abs(m.bals[i] - cur_bals[i]) for i in m.indices), sense=1)

    return m


def verify_solution(sol_df: pd.DataFrame, cur_tgt: Dict,
                    xc_funds: Dict, exchange_constraints=True):
    """ Verify that the solution satisfies currency total and exchange total
    requirements.
    """
    if sol_df is None:
        return False

    c = 'val_nnls' if 'val_nnls' in sol_df.columns else 'bals'
    sol_df['val_nnls'] = sol_df.val_nnls.astype('float')
    bal_sums = sol_df.groupby('cur_id').sum()[c].to_dict()

    cur_success = all(np.isclose(bal, cur_tgt[cur], rtol=0.01, atol=0.01)
                      for cur, bal in bal_sums.items())
    if exchange_constraints:
        ex_success = True
        ex_bals = sol_df.groupby('ex_id').sum()[c]
        for x in xc_funds.keys():
            if abs(ex_bals.loc[x] - xc_funds[x]) > 0.01:
                ex_success = False
    else:
        ex_success = np.isclose(sol_df.val_nnls.sum(), sum(xc_funds.values()))

    return cur_success and ex_success


def perform_regr(indiv, comb, val_cur):
    """ Perform bounded linear regression using lsq_linear.
    """
    indiv, comb = indiv.copy(), comb.copy()
    # determine exchange val totals
    ex_tot = indiv.reset_index().groupby(
        ['ex_id'])[['val']].sum()
    a = []
    b = []
    indiv = indiv.reset_index()
    # create systems of equations
    # relating exchange currencies to exchange total
    for ex_id in ex_tot.index:
        # a is a list of ilocs for each exchange
        a.append((indiv.ex_id == ex_id).astype(int).tolist())
        # b is exchange total
        b.append(ex_tot.val.loc[ex_id])
    # relating individual currencies to their total target val
    for cur_id in comb.index:
        # skip bitcoin to prevent regression from overshooting targets
        # if allocation is unbalanceable given exchange distribution
        if cur_id == val_cur:
            continue
        # a is list of ilocs for each currency
        a.append((indiv.cur_id == cur_id).astype(int).tolist())
        # b is currency total target val
        b.append(comb.val_tgt.loc[cur_id])
    # regression
    a = np.array(a)
    b = np.array(b)
    s = lsq_linear(a, b, bounds=(0, np.inf))
    # pprint(s)
    s = [s['x']]
    # print(['%.8f' % v for v in s[0]])
    indiv['val_nnls'] = s[0]
    return indiv


def perform_regr_L2(indiv, comb, val_cur):
    """ Perform bounded linear regression using lsq_linear.
    """
    indiv, comb = indiv.copy(), comb.copy()
    indiv = indiv.reset_index()

    a = []
    b = []

    ### create systems of equations
    # condition that total funds stay the same
    a.append(list(np.ones(len(indiv))))
    b.append(indiv.val.sum())

    # relating individual currencies to their total target val
    for cur_id in comb.index:
        # skip bitcoin to prevent regression from overshooting targets
        # if allocation is unbalanceable given exchange distribution
        if cur_id == val_cur:
            continue
        # a is list of ilocs for each currency
        a.append((indiv.cur_id == cur_id).astype(int).tolist())
        # b is currency total target val
        b.append(comb.val_tgt.loc[cur_id])

    # regression
    a = np.array(a)
    b = np.array(b)
    s = lsq_linear(a, b, bounds=(0, np.inf))

    s = [s['x']]

    indiv['val_nnls'] = s[0]
    return indiv

def calculate_transfers(df: pd.DataFrame):
    # calculate surplus / deficit for each exchange
    df = df.copy()
    df['diffs'] = df.val - df.val_nnls
    exd = df.groupby('ex_id').diffs.sum()

    # sort source/destination exchanges according to
    # deficit/surplus size, in order to minimize the
    # number of transfers
    srcs = exd[exd > 0].sort_values(ascending=False)
    dests = (exd[exd < 0] * -1).sort_values(ascending=False)
    assert np.isclose(srcs.sum(), dests.sum())

    transfers = defaultdict(list)

    # calculate transfers
    dest_exc = None
    dest_funds = None

    while len(srcs):
        src_exc = srcs.index[0]
        src_funds = srcs.pop(src_exc)

        while True:
            if dest_funds is None:
                dest_exc = dests.index[0]
                dest_funds = dests.pop(dest_exc)

            if dest_funds > src_funds:
                transfers['source_id'] += [src_exc]
                transfers['dest_id'] += [dest_exc]
                transfers['amount'] += [src_funds]
                dest_funds -= src_funds
                break

            transfers['source_id'] += [src_exc]
            transfers['dest_id'] += [dest_exc]
            transfers['amount'] += [dest_funds]
            src_funds -= dest_funds

            if len(dests):
                dest_exc = dests.index[0]
                dest_funds = dests.pop(dest_exc)

    transfers = pd.DataFrame(transfers,
                columns=['source_id', 'dest_id', 'amount'])
    assert np.isclose(transfers.amount.sum(), exd[exd > 0].sum())

    df = df.reset_index().set_index(['cur_id', 'ex_id'])
    df['bal_diff'] = df['bal'] - df['bal_tgt']
    sell_amounts = df[df['diffs'] < 0][['diffs', 'bal_diff']]
    sell_amounts = np.absolute(sell_amounts)
    buy_amounts = df[df['diffs'] > 0][['diffs', 'bal_diff']]
    col_names = dict(diffs='val', bal_diff='bal')
    buy_amounts.rename(columns=col_names, inplace=True)
    sell_amounts.rename(columns=col_names, inplace=True)

    return sell_amounts, transfers, buy_amounts
