from requests import get
from nba_api.stats.static import teams
from fanbasemarket.pricing.nba_data import liveGame, mov_multiplier
from fanbasemarket.queries.team import update_teamPrice, set_teamPrice
from fanbasemarket.models import Team, Teamprice, Purchase, Sale, ShortTransaction, Unshort, Game
from datetime import datetime
from dateutil import parser
from pytz import timezone

EST = timezone('US/Eastern')

generate_url = lambda s: f'http://data.nba.net/10s/prod/v1/{s}/scoreboard.json'

def bigboy_pulls_only(db):
    k = 75
    h = 10
    today = datetime.now(EST)
    url = generate_url(today.strftime('%Y%m%d'))
    games = get(url).json()['games']
    nba_teams = teams.get_teams()
    prices = {}
    ret = []
    for game in games:
        l = {}
        l['id'] = game['gameId']
        l['start'] = str(parser.parse(game['startTimeUTC']).astimezone(EST))
        l['is_on'] = game['isGameActivated']
        l['arena'] = game['arena']['name']
        ht, vt = game['hTeam'], game['vTeam']
        l['home_team'] = ht['triCode']
        l['away_team'] = vt['triCode']
        l['home_score'], l['away_score'] = ht['linescore'], \
                                           vt['linescore']
        home_score = 0.0 if ht['score'] == '' else float(ht['score'])
        away_score = 0.0 if vt['score'] == '' else float(vt['score'])
        l['score_margin'] = home_score - away_score
        l['period'] = game['period']['current']
        l['clock'] = game['clock']
        ret.append(l)
    results = []
    for i in ret:
        home_abv = i['home_team']
        away_abv = i['away_team']
        home_tObj = Team.query.filter(Team.abr == home_abv).first()
        away_tObj = Team.query.filter(Team.abr == away_abv).first()
        if not i['is_on']:
            if i['home_score'] != 0.0 or i['away_score'] != 0.0:
                match = db.session.query(Game).\
                    filter(Game.gameid == i['id']).\
                    first()
                if match is not None:
                    continue
                newgame = Game(gameid=i['id'], home=home_tObj.id, away=away_tObj.id, home_score=i['home_score'], away_score=i['away_score'], start=i['start'])
                db.session.add(newgame)
                db.session.commit()
                if i['home_score'] > i['away_score']:
                    home_tObj.playoff_wins += 1
                    db.session.commit()
                    winner = home_tObj
                else:
                    away_tObj.playoff_wins += 1
                    db.session.commit()
                    winner = away_tObj
                # figure out which series we're in
                starts = [
                    datetime(year=2020, month=8, day=17, tzinfo=EST),
                    datetime(year=2020, month=8, day=31, tzinfo=EST),
                    datetime(year=2020, month=9, day=15, tzinfo=EST),
                    datetime(year=2020, month=9, day=30, tzinfo=EST)
                ]
                ix = 0
                while True:
                    if starts[ix] > today:
                        break
                    ix += 1
                startOfSeries = starts[ix]
                purchases = db.session.query(Purchase).\
                    filter(Purchase.purchased_at <= startOfSeries).\
                    filter(Purchase.exists == True).\
                    filter(Purchase.team_id == winner.id).\
                    all()
                price = winner.price
                mult = 0
                dividend = 100
                if price >= 1800:
                    dividend = 5
                elif price >= 1700:
                    dividend = 10
                elif price >= 1650:
                    dividend = 15
                elif price >= 1600:
                    dividend = 17.5
                elif price >= 1550:
                    dividend = 20
                elif price >= 1500:
                    dividend = 37.5
                elif price >= 1450:
                    dividend = 45
                elif price >= 1400:
                    dividend = 55
                elif price >= 1350:
                    dividend = 65
                elif price >= 1300:
                    dividend = 75
                elif price >= 1250:
                    dividend = 85
                elif price >= 1200:
                    dividend = 95
                elif price >= 1150:
                    dividend = 110
                elif price >= 1100:
                    dividend = 125
                elif price >= 1050:
                    dividend = 145
                elif price >= 1000:
                    dividend = 175
                if (winner.playoff_wins % 4 == 0) and (winner.playoff_wins > 0):
                    mult = (1 + (team.playoff_wins / 4))
                dividend *= mult
                if dividend > 0:
                    for purchase in purchases:
                        payoff = purchase.amt_shares * dividend
                        usr = db.Session.query(User).\
                            filter(User.id == purchase.user_id).\
                            first()
                        usr.available_funds += dividend
                        db.session.commit()
        if i['home_score'] == 0 and i['away_score'] == 0:
            continue
        score_margin = i['score_margin']
        period = i['period']
        clock = i['clock']
        for team in nba_teams:
            if str(team['abbreviation']) == home_abv:
                home_team = str([team["nickname"]])
                prices[home_team] = 0
            elif str(team['abbreviation']) == away_abv:
                away_team = str([team["nickname"]])
                prices[away_team] = 0
        home_elo = Teamprice.query.\
            filter(Teamprice.team_id == home_tObj.id).\
            filter(Teamprice.date <= i['start']).\
            all()[-1].elo
        away_elo = Teamprice.query.\
            filter(Teamprice.team_id == away_tObj.id).\
            filter(Teamprice.date <= i['start']).\
            all()[-1].elo
        i_home_win_prob = 1/(1+10**((away_elo - home_elo - h)/400))
        i_away_win_prob = 1 - i_home_win_prob
        if ':' in clock:
            minutes = float(clock[:clock.index(":")])
            seconds = float(clock[clock.index(":")+1:])
        else:
            minutes = 0.0
            if clock == '':
                seconds = 0.0
            else:
                seconds = float(clock)
        since_period = round(minutes + (seconds /60.0), 2)
        time_left_in_q = 12 - since_period
        if period <= 4: 
            time_elapsed = ((period)-1) * 12 + (time_left_in_q)
        else:
            time_elapsed = 48 + ((period - 4) * 5 - since_period)
        
        live_prob = liveGame(time_elapsed, score_margin, i_home_win_prob * 100, period)
        if score_margin > 0:
            proj_mov = score_margin * live_prob/100
        else:
            proj_mov = -score_margin * (1-live_prob/100)
        marginofv = mov_multiplier(home_elo, away_elo, proj_mov)
        elo_change = k * marginofv * ((live_prob - (i_home_win_prob*100))/100)
        
        new_homeElo = home_elo + elo_change
        new_awayElo = away_elo - elo_change
        ps_during_game = Purchase.query.\
            filter(Purchase.purchased_at <= today).\
            filter(Purchase.purchased_at >= i['start'])
        home_ps = len(ps_during_game.filter(Purchase.team_id == home_tObj.id).all())
        away_ps = len(ps_during_game.filter(Purchase.team_id == away_tObj.id).all())
        ss_during_game = Sale.query.\
            filter(Sale.date <= today).\
            filter(Sale.date >= i['start'])
        home_ss = len(ss_during_game.filter(Sale.team_id == home_tObj.id).all())
        away_ss = len(ss_during_game.filter(Sale.team_id == away_tObj.id).all())
        new_homeElo *= (1.0025 ** (home_ps - home_ss))
        new_awayElo *= (1.0025 ** (away_ps - away_ss))
        shorts_during_game = ShortTransaction.query.\
            filter(ShortTransaction.shorted_at <= today).\
            filter(ShortTransaction.shorted_at >= i['start'])
        unshorts_during_game = Unshort.query.\
            filter(Unshort.unshorted_at <= today).\
            filter(Unshort.unshorted_at >= i['start'])
        home_shorts = len(shorts_during_game.filter(ShortTransaction.team_id == home_tObj.id).all())
        away_shorts = len(shorts_during_game.filter(ShortTransaction.team_id == away_tObj.id).all())
        home_unshorts = len(unshorts_during_game.filter(Unshort.team_id == home_tObj.id).all())
        away_unshorts = len(unshorts_during_game.filter(Unshort.team_id == away_tObj.id).all())
        new_homeElo *= (1.0025 ** (home_unshorts - home_shorts))
        new_awayElo *= (1.0025 ** (away_unshorts - away_shorts))
        set_teamPrice(home_tObj, new_homeElo, today, db)
        results.append({home_abv: {'date': str(today), 'price': new_homeElo}})
        set_teamPrice(away_tObj, new_awayElo, today, db)
        results.append({away_abv: {'date': str(today), 'price': new_awayElo}})
    return results
