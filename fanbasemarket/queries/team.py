from sqlalchemy import desc

from sqlalchemy.ext.declarative import declarative_base
from fanbasemarket.models import Teamprice, Player, Purchase, Team, \
                                 Short

from datetime import datetime, timedelta
from pytz import timezone

EST = timezone('US/Eastern')

def get_all_team_data(db):
    payload = {}
    all_teams = db.session.query(Team).all()
    now = datetime.now(EST)
    for team in all_teams:
        d = {}
        d['name'] = team.name
        d['price'] = {'date': now, 'price': team.price}
        prev_prices = db.session.query(Teamprice).\
            filter(Teamprice.team_id == team.id).all()
        d['graph'] = {}
        d['graph']['SZN'] = [{'date': str(price.date), 'price': price.elo} \
                             for price in prev_prices]
        d['graph']['1M'] = [{'date': str(price.date), 'price': price.elo} \
                            for price in prev_prices if \
                            EST.localize(price.date) + timedelta(weeks=4) >= now]
        d['graph']['1W'] = [{'date': str(price.date), 'price': price.elo} \
                            for price in prev_prices if \
                            EST.localize(price.date) + timedelta(weeks=1) >= now]
        d['graph']['1D'] = [{'date': str(price.date), 'price': price.elo} \
                            for price in prev_prices if \
                            EST.localize(price.date) + timedelta(hours=24) >= now]
        d['graph']['1D'].append(d['price'])
        if len(d['graph']['1D']) == 1:
            dt = str(now - timedelta(hours=24))
            p = d['price']['price']
            d['graph']['1D'].append({'date': dt, 'price': p})
        payload[team.abr] = d
    return payload

def update_teamPrice(team, delta, dt, db):
    newprice = team.price + delta
    team.prev_price = team.price
    team.price = newprice
    team.delta = delta
    loc = db.session.merge(team)
    db.session.add(loc)
    db.session.commit()
    price_obj = Teamprice(date=dt, team_id=team.id, elo=newprice)
    db.session.add(price_obj)
    db.session.commit()

def set_teamPrice(team, p, dt, db):
    team.prev_price = team.price
    team.price = p
    team.delta = p - team.prev_price
    loc = db.session.merge(team)
    db.session.add(loc)
    db.session.commit()
    price_obj = Teamprice(date=dt, team_id=team.id, elo=p)
    db.session.add(price_obj)
    db.session.commit()

def set_player_rating(team, db):
    players = db.session.query(Player).filter(Player.team_id==team.id).all()
    return sum([player.rating * player.mpg for player in players])

def active_player_rating(team, db):
    active_ps = db.session.query(Player).filter(Player.team_id == team.id).\
        filter(Player.is_injured == False).\
        all()
    return sum([player.rating * player.mpg for player in active_ps])

from fanbasemarket.queries.user import get_active_holdings

def get_user_position(team, user, db):
    holdings = db.session.query(Purchase).\
        filter(Purchase.user_id == user.id).\
        filter(Purchase.team_id == team.id).\
        filter(Purchase.exists == True).\
        all()
    shorts = db.session.query(Short).\
        filter(Short.user_id == user.id).\
        filter(Short.team_id == team.id).\
        filter(Short.exists == True).\
        all()
    values = [h.purchased_for for h in holdings]
    if len(values) > 0:
        bought_at = sum(values) / len(values)
    else: 
        bought_at = 0
    num_shares = sum([h.amt_shares for h in holdings])
    date = str(datetime.now(EST))
    all_holdings = get_active_holdings(user.id, db, date=date)
    total_val = 0
    for abr, purchases in all_holdings.items():
        tm = db.session.query(Team).\
            filter(Team.abr == abr).first()
        for purchase in purchases:
            total_val += tm.price * purchase['num_shares']
    if total_val != 0:
        weight = team.price * num_shares / total_val
    else:
        weight = 0
    d = {}
    d['bought_at'] = bought_at
    d['num_shares'] = num_shares
    d['weight'] = weight
    d['short'] = {}
    shorted_for = 0
    values = [s.shorted_for for s in shorts]
    if len(values) > 0:    
        shorted_for = sum(values) / len(values)
    else:
        shorted_for = 0
    num_shorted = sum([s.amt_shorted for s in shorts])
    if len(values) != 0:
        d['short']['num_shorted'] = num_shorted
        d['short']['shorted_for'] = shorted_for
    return d
