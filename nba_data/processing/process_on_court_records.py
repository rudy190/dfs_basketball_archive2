import pandas as pd
from sqlalchemy import (and_, case, func, literal)
from nba_data.processing.db_config import Session
from nba_data.processing.nba_game_seq import GameSequence
from nba_data.processing.nba_player_boxes import PlayerBoxScore

def process_on_court_records(game_ids, sport, season):
    session=Session()
    on_court_events = get_on_court_events(session, game_ids, sport, season)
    on_court_events = add_on_court_ind(on_court_events)
    on_court_events = remove_off_court_records(on_court_events)
    insert_on_court_records(session, on_court_events)
    session.close()
    return True

def add_on_court_ind(on_court_events):
    group_cols=['game_id','period','person_id']
    on_court_events['on_court_ind']=on_court_events.groupby(group_cols)['sub_count'].cumsum()
    criteria = on_court_events['model_event_num']==1
    on_court_events.loc[criteria,'on_court_ind'] = on_court_events.loc[criteria,'sub_count']
    on_court_events['on_court_ind'] = on_court_events['on_court_ind'].fillna(0).astype('int')
    return on_court_events

def remove_off_court_records(on_court_events):
    criteria = on_court_events['on_court_ind']!=0
    on_court_events = on_court_events.loc[criteria,:]
    return on_court_events

def insert_on_court_records(session, on_court_events):
    columns=['sport','season','game_id','period','event_num','model_event_num',
             'team_id','person_id','on_court_ind']
    on_court_events[columns].to_sql('game_on_court',
                                    session.bind,
                                    if_exists='append',
                                    index=False,
                                    chunksize=500000)
    return True

def get_on_court_events(session, game_ids, sport, season):
    on_court_events_query=set_on_court_events_query(session, game_ids, sport, season)
    on_court_events=pd.read_sql_query(on_court_events_query.statement, session.bind)
    return on_court_events

def set_on_court_events_query(session, game_ids, sport, season):
    sub_events=sub_events_query(session, game_ids, sport, season).subquery()
    model_events=model_events_query(session, game_ids, sport, season).subquery()
    on_court_events=join_sub_events_query(session, sub_events, model_events)
    return on_court_events

def sub_events_query(session, game_ids, sport, season):
    sub_events = in_game_sub_events_query(session, game_ids, sport, season)
    return sub_events

def in_game_sub_events_query(session, game_ids, sport, season):
    in_game_sub_events=(session.query(GameSequence.id.label('id'),
                                      GameSequence.game_id.label('game_id'),
                                      GameSequence.period.label('period'),
                                      GameSequence.model_event_num.label('model_event_num'),
                                      GameSequence.team_id.label('team_id'),
                                      GameSequence.person_id.label('person_id'),
                                      GameSequence.action_sub_category.label('action_sub_category'),
                                      case([(GameSequence.action_sub_category=='sub_in', 1),
                                            (GameSequence.action_sub_category=='sub_out', -1)]).label('sub_count'))
                               .filter(GameSequence.game_id.in_(game_ids))
                               .filter(GameSequence.sport==sport)
                               .filter(GameSequence.season==season)
                               .filter(GameSequence.action_sub_category.in_(['sub_in','sub_out'])))
    return in_game_sub_events

def model_events_query(session, game_ids, sport, season):
    model_events=(session.query(GameSequence.sport,
                                GameSequence.season,
                                GameSequence.game_id,
                                GameSequence.period,
                                GameSequence.model_event_num,
                                GameSequence.event_num,
                                PlayerBoxScore.team_id,
                                PlayerBoxScore.player_id.label('person_id'))
                         .filter(GameSequence.game_id.in_(game_ids))
                         .filter(GameSequence.sport==sport)
                         .filter(GameSequence.season==season)
                         .filter(GameSequence.game_id==PlayerBoxScore.game_id)
                         .group_by(GameSequence.sport,
                                   GameSequence.season,
                                   GameSequence.game_id,
                                   GameSequence.period,
                                   GameSequence.model_event_num,
                                   GameSequence.event_num,
                                   PlayerBoxScore.team_id,
                                   PlayerBoxScore.player_id))
    return model_events

def join_sub_events_query(session, sub_events, model_events):
    on_court_events=(session.query(model_events.c.sport,
                                   model_events.c.season,
                                   model_events.c.game_id,
                                   model_events.c.period,
                                   model_events.c.event_num,
                                   model_events.c.model_event_num,
                                   model_events.c.team_id,
                                   model_events.c.person_id,
                                   case([(sub_events.c.model_event_num!=None,
                                          sub_events.c.action_sub_category)],
                                        else_=None).label('action_sub_category'),
                                   case([(sub_events.c.model_event_num!=None,
                                          sub_events.c.sub_count)],
                                        else_=0).label('sub_count'))
                        .select_from(model_events)
                        .outerjoin(sub_events,
                                   and_(model_events.c.game_id==sub_events.c.game_id,
                                        model_events.c.model_event_num==sub_events.c.model_event_num,
                                        model_events.c.team_id==sub_events.c.team_id,
                                        model_events.c.person_id==sub_events.c.person_id))
                        .filter(model_events.c.model_event_num!=None)
                        .order_by(model_events.c.game_id,
                                  model_events.c.team_id,
                                  model_events.c.person_id,
                                  model_events.c.model_event_num))
    return on_court_events
