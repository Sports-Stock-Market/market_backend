from datetime import datetime, timedelta
from json import dumps
from functools import reduce
from sqlalchemy import and_, not_
from pytz import timezone
from flask_socketio import emit

from fanbasemarket.queries.utils import get_graph_x_values
from fanbasemarket.queries.team import update_teamPrice
from fanbasemarket.models import Purchase, User, Team, Sale, PurchaseTransaction

EST = timezone('US/Eastern')

def get_active_holdings(uid, db, date=None):
    if not date:
        date = str(datetime.now(EST))
    results = Purchase.query.\
        filter(Purchase.user_id == uid).\
        filter(Purchase.exists == True).\
        all()
    holdings = {}
    for result in results:
        tm = Team.query.filter(Team.id == result.team_id).first()
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

def get_assets_in_date_range(uid, previous_balance, end, db, start=None, prev={}):
    if start is None:
        previous_purchases = PurchaseTransaction.query.\
            filter(PurchaseTransaction.user_id == uid).\
            filter(PurchaseTransaction.date <= end).all()
        previous_sales = Sale.query.\
            filter(Sale.user_id == uid).\
            filter(Sale.date <= end).all()
    else:
        previous_purchases = PurchaseTransaction.query.\
            filter(PurchaseTransaction.user_id == uid).\
            filter(PurchaseTransaction.date <= end).\
            filter(PurchaseTransaction.date > start).all()
        previous_sales = Sale.query.\
            filter(Sale.user_id == uid).\
            filter(Sale.date <= end).\
            filter(Sale.date > start).all()
    total = previous_balance
    last_date = end
    if len(previous_purchases) != 0:
        for purchase in previous_purchases:
            abr = Team.query.filter(Team.id == purchase.team_id).first().abr
            if abr not in prev:
                prev[abr] = 0
            prev[abr] += purchase.amt_purchased
            total -= (purchase.purchased_for * purchase.amt_purchased)
            if purchase.date >= last_date:
                last_date = purchase.date
    if len(previous_sales) != 0:
        for sale in previous_sales:
            abr = Team.query.filter(Team.id == purchase.team_id).first().abr
            prev[abr] -= sale.amt_sold
            if sale.date >= last_date:
                last_date = sale.date
            total += (sale.sold_for * sale.amt_sold)
        funds = total
    for abr, amt in prev:
        tm = Team.query.filter(Team.abr == abr).first()
        prev_prices = Teamprice.query.filter(Teamprice.team_id == tm.id).all()
        total += get_price(prev_prices, last_date)
    return last_date, total, funds

def get_current_usr_value(uid, db):
    now = str(datetime.now(EST))
    _, t = get_assets_in_date_range(uid, 1500, now, db)
    return t

def get_leaderboard(db):
    usrs_all = User.query.all()
    vals = [(u.username, get_current_usr_value(u.id, db)) for u in usrs_all]
    return [{'username': x, 'value': y} for x, y in vals]   

def get_user_graph_points(uid, db):
    x_values_dict = get_graph_x_values()
    data_points = []
    data_points = {}
    for k, x_values in x_values_dict.items():
        data_points[k] = []
        prev = {}
        initial_date, val, funds = get_assets_in_date_range(uid, 15000, x_values[0], db, prev=prev)
        data_points[k].append({'date': str(initial_date), 'price': val})
        for i, x_val in enumerate(x_values[:-1]):
            date, val, funds = get_assets_in_date_range(uid, funds, x_values[i + 1], db, start=x_val, prev=prev)
            date_s = str(date)
            data_points[k].append({'date': date_s, 'price': val})
    return data_points

def buy_shares(usr, abr, num_shares, db):
    team = Team.query.filter(Team.abr == abr).first()
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
    team = Team.query.filter(Team.abr == abr).first()
    price = num_shares * team.price * 0.995
    all_holdings = Purchase.query.filter(Purchase.user_id == usr.id).filter(Purchase.team_id == team.id).filter(Purchase.exists == True).all()
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
