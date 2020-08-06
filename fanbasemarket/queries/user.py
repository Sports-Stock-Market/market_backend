from datetime import datetime, timedelta
from json import dumps
from functools import reduce
from sqlalchemy import and_, not_
from pytz import timezone
from flask_socketio import emit

from fanbasemarket.queries.team import update_teamPrice
from fanbasemarket.models import Purchase, User, Team, Sale, PurchaseTransaction, Teamprice, \
                                 Short, ShortTransaction, Unshort

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

def prev_prchs(uid, end, prev_ps, prev_ss, start=None):
    if start is None:
        new_p = [p for p in prev_ps if EST.localize(p.date) <= end]
        new_s = [s for s in prev_ss if EST.localize(s.date) <= end]
    else:
        new_p = [p for p in prev_ps if EST.localize(p.date) <= end and EST.localize(p.date) > start]
        new_s = [s for s in prev_ss if EST.localize(s.date) <= end and EST.localize(s.date) > start]
    return new_p, new_s

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

def generate_user_graph(uid, db):
    now = datetime.now(EST)
    milestones = []
    sales = db.session.query(Sale).\
        filter(Sale.user_id == uid).all()
    milestones += [{'type': 'SALE', 'tid': s.team_id, 'date': EST.localize(s.date), 'amt': s.amt_sold, 'for': s.sold_for} for s in sales]
    ps = db.session.query(PurchaseTransaction).\
        filter(PurchaseTransaction.user_id == uid).all()
    milestones += [{'type': 'PURCHASE', 'tid': p.team_id, 'date': EST.localize(p.date), 'amt': p.amt_purchased, 'for': p.purchased_for} for p in ps]
    shorts = db.session.query(ShortTransaction).\
        filter(ShortTransaction.user_id == uid).all()
    milestones += [{'type': 'SHORT', 'tid': p.team_id, 'date': EST.localize(p.shorted_at), 'amt': p.amt_shorted, 'for': p.shorted_for} for p in shorts]
    unshorts = db.session.query(Unshort).\
        filter(Unshort.user_id == uid).all()
    milestones += [{'type': 'UNSHORT', 'tid': p.team_id, 'date': EST.localize(p.unshorted_at), 'amt': p.amt_unshorted, 'for': p.unshorted_for} for p in unshorts]
    for team in db.session.query(Team).all():
        prices = db.session.query(Teamprice).filter(Teamprice.team_id == team.id).all()
        milestones += [{'type': 'PRICE', 'tid': team.id, 'date': EST.localize(p.date), 'price': p.elo} for p in prices]
    milestones = sorted(milestones, key=lambda x: x['date'])
    points = []
    holdings = {}
    funds = 50000.0
    for milestone in milestones:
        tid = milestone['tid']
        if (milestone['type'] == 'PURCHASE' or milestone['type'] == 'UNSHORT'):
            pfor = milestone['for']
            if tid not in holdings:
                holdings[tid] = [0, pfor]
            funds -= pfor * milestone['amt']
            holdings[tid][0] += milestone['amt']
            holdings[tid][1] = pfor
        elif (milestone['type'] == 'SALE' or milestone['type'] == 'SHORT'):
            sfor = milestone['for']
            funds += sfor * milestone['amt']
            holdings[tid][0] -= milestone['amt']
            holdings[tid][1] = sfor
        else:
            if tid in holdings:
                holdings[tid][1] = milestone['price']
                assets = funds
                for _, val in holdings.items():
                    assets += val[0] * val[1]
                points.append((milestone['date'], assets))
    graph = {}
    graph['1D'] = [{'date': str(point[0]), 'price': point[1]} for point in points if \
                   point[0] + timedelta(hours=24) >= now]
    graph['1W'] = [{'date': str(point[0]), 'price': point[1]} for point in points if \
                   point[0] + timedelta(days=7) >= now]
    graph['1M'] = [{'date': str(point[0]), 'price': point[1]} for point in points if \
                   point[0] + timedelta(weeks=4) >= now]
    graph['SZN'] = [{'date': str(point[0]), 'price': point[1]} for point in points]
    return graph

def buy_shares(usr, abr, num_shares, db):
    team = db.session.query(Team).filter(Team.abr == abr).first()
    price = num_shares * team.price * 1.0025
    if usr.available_funds < price:
        raise ValueError('not enough funds')
    now = datetime.now(EST)
    purchase = Purchase(team_id=team.id, user_id=usr.id, purchased_at=now,
                        purchased_for=team.price * 1.0025, amt_shares=num_shares)
    db.session.add(purchase)
    db.session.commit()
    ptransac = PurchaseTransaction(team_id=team.id, user_id=usr.id, date=now,
                                  purchased_for=team.price * 1.0025, amt_purchased=num_shares)
    db.session.add(ptransac)
    db.session.commit()
    res = [{team.abr: {'date': str(now), 'price': team.price * 1.0025}}]
    emit('prices', res, broadcast=True, namespace='/')
    update_teamPrice(team, (team.price * .0025), now , db)
    usr.available_funds -= price
    loc = db.session.merge(usr)
    db.session.add(loc)
    db.session.commit()
    return res

def sell_shares(usr, abr, num_shares, db):
    team = db.session.query(Team).filter(Team.abr == abr).first()
    price = num_shares * team.price * .9975
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
            p.sold_for = team.price * .9975
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
                    sold_for=team.price * .9975)
    db.session.add(new_sale)
    db.session.commit()
    res = [{team.abr: {'date': str(now), 'price': team.price * .9975}}]
    emit('prices', res, broadcast=True, namespace='/')
    update_teamPrice(team, -(team.price * .0025), now, db)
    return res

def short_team(usr, abr, num_shares, db):
    now = datetime.now(EST)
    tm = db.session.query(Team).\
        filter(Team.abr == abr).first()
    price = num_shares * tm.price * .9975
    usr.available_funds += price
    short = Short(team_id=tm.id, user_id=usr.id, shorted_for=tm.price * .9975,
                  shorted_at=now, amt_shorted=num_shares)
    db.session.add(short)
    db.session.commit()
    transac = ShortTransaction(team_id=tm.id, user_id=usr.id,
                               shorted_for=tm.price * .9975,
                               shorted_at=now, amt_shorted=num_shares)
    db.session.add(transac)
    db.session.commit()
    loc_usr = db.session.merge(usr)
    db.session.add(loc_usr)
    db.session.commit()
    res = [{tm.abr: {'date': str(now), 'price': tm.price * .9975}}]
    emit('prices', res, broadcast=True, namespace='/')
    update_teamPrice(tm, -(tm.price * .0025), now , db)
    return res

def unshort_team(usr, abr, num_shares, db):
    team = db.session.query(Team).\
        filter(Team.abr == abr).\
        first()
    price = num_shares * team.price * 1.0025
    if price > usr.available_funds:
        raise ValueError('insufficient funds')
    all_shorts = db.session.query(Short).\
        filter(Short.user_id == usr.id).\
        filter(Short.team_id == team.id).\
        filter(Short.exists == True).\
        all()
    total_shares = reduce(lambda x, p: x + p.amt_shorted, all_shorts, 0)
    if num_shares > total_shares:
        raise ValueError('not enough shares owned')
    now = datetime.now(EST)
    all_holdings.sort(key=lambda p: p.amt_shares, reverse=True)
    left_to_delete = num_shares
    ix = 0
    while left_to_delete > 0:
        p = all_shorts[ix]
        to_del = min(p.amt_shorted, left_to_delete)
        if to_del == p.amt_shorted:
            p.exists = False
            p.sold_at = now
            p.sold_for = team.price * 1.0025
        else:
            p.amt_shorted -= to_del
        loc = db.session.merge(p)
        db.session.add(loc)
        db.session.commit()
        left_to_delete -= to_del
        ix += 1
    usr.available_funds += price
    loc_u = db.session.merge(usr)
    db.session.add(loc_u)
    db.session.commit()
    new_unshort = Unshort(team_id=team.id, unshorted_at=now, amt_unshorted=num_shares,
                          user_id=usr.id, unshorted_for=team.price * 1.0025)
    db.session.add(new_unshort)
    db.session.commit()
    res = [{team.abr: {'date': str(now), 'price': team.price * 1.0025}}]
    emit('prices', res, broadcast=True, namespace='/')
    update_teamPrice(team, team.price * .0025, now, db)
    return res