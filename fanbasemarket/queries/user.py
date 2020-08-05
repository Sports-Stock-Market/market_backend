from datetime import datetime, timedelta
from json import dumps
from functools import reduce
from sqlalchemy import and_, not_
from pytz import timezone
from flask_socketio import emit

from fanbasemarket.queries.utils import get_graph_x_values
from fanbasemarket.queries.team import update_teamPrice
from fanbasemarket.models import Purchase, User, Team, Sale, PurchaseTransaction, Teamprice

EST = timezone('US/Eastern')

def get_active_holdings(uid, db, date=None):
    if not date:
        date = str(datetime.now(EST))
    results = db.session.query(Purchase).\
        filter(Purchase.user_id == uid).\
        filter(Purchase.exists == True).\
        all()
    holdings = {}
    for result in results:
        tm = db.session.query(Team).filter(Team.id == result.team_id).first()
        bt_at = str(result.purchased_at)
        bt_f = result.purchased_for
        amt_shares = result.amt_shares
        res = {'bought_at': bt_at, 'bought_for': bt_f, 'num_shares': amt_shares}
        if tm.abr not in holdings:
            holdings[tm.abr] = [res]
        else:
            holdings[tm.abr].append(res)
    return holdings

from fanbasemarket.queries.team import get_price

def prev_prchs(uid, end, prev_ps, prev_ss, start=None):
    if start is None:
        new_p = [p for p in prev_ps if EST.localize(p.date) <= end]
        new_s = [s for s in prev_ss if EST.localize(s.date) <= end]
    else:
        new_p = [p for p in prev_ps if EST.localize(p.date) <= end and EST.localize(p.date) > start]
        new_s = [s for s in prev_ss if EST.localize(s.date) <= end and EST.localize(s.date) > start]
    return new_p, new_s

def get_assets_in_date_range(uid, previous_balance, end, prev_ps, prev_ss, db, start=None, prev={}):
    previous_purchases, previous_sales = prev_prchs(uid, end, prev_ps, prev_ss, start=start)
    last_date = end
    net_spend = 0
    for purchase in previous_purchases:
        abr = db.session.query(Team).filter(Team.id == purchase.team_id).first().abr
        if abr not in prev:
            prev[abr] = 0
        prev[abr] += purchase.amt_purchased
        net_spend += (purchase.purchased_for * purchase.amt_purchased)
        if EST.localize(purchase.date) >= last_date:
            last_date = EST.localize(purchase.date)
    for sale in previous_sales:
        abr = db.session.query(Team).filter(Team.id == sale.team_id).first().abr
        if abr not in prev:
            prev[abr] = 0
        prev[abr] -= sale.amt_sold
        net_spend -= (sale.sold_for * sale.amt_sold)
        if EST.localize(sale.date) >= last_date:
            last_date = EST.localize(sale.date)
    funds = previous_balance - net_spend

    assets = 0
    for abr, amt in prev.items():
        tm = db.session.query(Team).filter(Team.abr == abr).first()
        price = get_price(tm.id, db, last_date)
        assets += price * amt

    return last_date, funds + assets, funds

def get_current_usr_value(uid, db):
    now = datetime.now(EST)
    _, t, _ = get_assets_in_date_range(uid, 50000, now, db)
    return t

def get_leaderboard(db):
    usrs_all = db.session.query(User).all()
    res = []
    for usr in usrs_all:
        holdings = get_active_holdings(usr.id, db)
        total = usr.available_funds
        for abr, item in holdings.items():
            for p in item:
                t = db.session.query(Team).filter(Team.abr == abr).first()
                total += p['num_shares'] * t.price
        res.append({'username': usr.username, 'value': total})
    return res

def get_user_graph_points(uid, db):
    x_values_dict = get_graph_x_values()
    prev_ps = db.session.query(PurchaseTransaction).\
        filter(PurchaseTransaction.user_id == uid).all()
    prev_ss = db.session.query(Sale).\
        filter(Sale.user_id == uid).all()
    data_points = []
    data_points = {}
    for k, x_values in x_values_dict.items():
        data_points[k] = []
        prev = {}
        initial_date, val, funds = get_assets_in_date_range(uid, 50000, x_values[0], prev_ps, prev_ss, db, prev=prev)
        data_points[k].append({'date': str(initial_date), 'price': val})
        for i, x_val in enumerate(x_values[:-1]):
            date, val, funds = get_assets_in_date_range(uid, funds, x_values[i + 1], prev_ps, prev_ss, db, start=x_val, prev=prev)
            date_s = str(date)
            data_points[k].append({'date': date_s, 'price': val})
    return data_points

def generate_user_graph(uid, db):
    now = datetime.now(EST)
    milestones = []
    sales = db.session.query(Sale).\
        filter(Sale.user_id == uid)
    milestones += [{'type': 'SALE', 'tid': s.team_id, 'date': EST.localize(s.date), 'amt': s.amt_sold, 'for': s.sold_for} for s in sales]
    ps = db.session.query(PurchaseTransaction).\
        filter(Sale.user_id == uid)
    milestones += [{'type': 'PURCHASE', 'tid': p.team_id, 'date': EST.localize(p.date), 'amt': p.amt_purchased, 'for': p.purchased_for} for p in ps]
    for team in db.session.query(Team).all():
        prices = db.session.query(Teamprice).filter(Teamprice.team_id == team.id).all()
        milestones += [{'type': 'PRICE', 'tid': team.id, 'date': EST.localize(p.date), 'price': p.elo} for p in prices]
    milestones = sorted(milestones, key=lambda x: x['date'])
    points = []
    holdings = {}
    funds = 50000.0
    for milestone in milestones:
        tid = milestone['tid']
        if milestone['type'] == 'PURCHASE':
            pfor = milestone['for']
            if tid not in holdings:
                holdings[tid] = [0, pfor]
            holdings[tid][0] += milestone['amt']
        elif milestone['type'] == 'SALE':
            sfor = milestone['for']
            holdings[tid][0] -= milestone['amt']
            holdings[tid][1] = sfor
        else:
            if tid in holdings:
                amt = holdings[tid][0]
                prev = holdings[tid][1]
                funds += amt * (milestone['price'] - prev)
                holdings[tid][1] = milestone['price']
                points.append((milestone['date'], funds))
    graph = {}
    graph['1D'] = [{'date': str(point[0]), 'price': point[1]} for point in points if \
                   point['date'] + timedelta(hours=24) >= now]
    graph['1W'] = [{'date': str(point[0]), 'price': point[1]} for point in points if \
                   point['date'] + timedelta(days=7) >= now]
    graph['1M'] = [{'date': str(point[0]), 'price': point[1]} for point in points if \
                   point['date'] + timedelta(weeks=4) >= now]
    graph['SZN'] = [{'date': str(point[0]), 'price': point[1]} for point in points]
    return graph

def buy_shares(usr, abr, num_shares, db):
    team = db.session.query(Team).filter(Team.abr == abr).first()
    price = num_shares * team.price * 1.005
    if usr.available_funds < price:
        raise ValueError('not enough funds')
    now = datetime.now(EST)
    purchase = Purchase(team_id=team.id, user_id=usr.id, purchased_at=now,
                        purchased_for=team.price * 1.005, amt_shares=num_shares)
    db.session.add(purchase)
    db.session.commit()
    ptransac = PurchaseTransaction(team_id=team.id, user_id=usr.id, date=now,
                                  purchased_for=team.price * 1.005, amt_purchased=num_shares)
    db.session.add(ptransac)
    db.session.commit()
    res = [{team.abr: {'date': str(now), 'price': team.price * 1.005}}]
    emit('prices', res, broadcast=True, namespace='/')
    update_teamPrice(team, (team.price * .005), now , db)
    usr.available_funds -= price
    loc = db.session.merge(usr)
    db.session.add(loc)
    db.session.commit()
    return res

def sell_shares(usr, abr, num_shares, db):
    team = db.session.query(Team).filter(Team.abr == abr).first()
    price = num_shares * team.price * 0.995
    all_holdings = db.session.query(Purchase).filter(Purchase.user_id == usr.id).filter(Purchase.team_id == team.id).filter(Purchase.exists == True).all()
    total_shares = reduce(lambda x, p: x + p.amt_shares, all_holdings, 0)
    if num_shares > total_shares:
        raise ValueError('not enough shares owned')
    now = datetime.now(EST)
    all_holdings.sort(key=lambda p: p.amt_shares, reverse=True)
    left_to_delete = num_shares
    ix = 0
    while left_to_delete > 0:
        p = all_holdings[ix]
        to_del = min(p.amt_shares, left_to_delete)
        if to_del == p.amt_shares:
            p.exists = False
            p.sold_at = now
            p.sold_for = team.price * .995
        else:
            p.amt_shares -= to_del
        loc = db.session.merge(p)
        db.session.add(loc)
        db.session.commit()
        left_to_delete -= to_del
        ix += 1
    usr.available_funds += price
    loc_u = db.session.merge(usr)
    db.session.add(loc_u)
    db.session.commit()
    new_sale = Sale(team_id=team.id, date=now, amt_sold=num_shares, user_id=usr.id,
                    sold_for=team.price * .995)
    db.session.add(new_sale)
    db.session.commit()
    res = [{team.abr: {'date': str(now), 'price': team.price * .995}}]
    emit('prices', res, broadcast=True, namespace='/')
    update_teamPrice(team, -(team.price * .005), now, db)
    return res
