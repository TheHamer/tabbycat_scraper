import psycopg2
from sqlalchemy import create_engine
import pandas as pd

class TabDatabase:
    
    def __init__(self, hostname, database, username, pwd, port_id):
        
        self.hostname = hostname
        self.database = database
        self.username = username
        self.pwd = pwd
        self.port_id = port_id
        
        self.db = None
        self.cur = None
        self.engine = None

    def __enter__(self):
        
        self.db = psycopg2.connect(host = self.hostname, dbname = self.database, user = self.username, password = self.pwd, port = self.port_id)
        self.cur = self.db.cursor()
        self.engine = create_engine(f'postgresql://{self.database}:{self.pwd}@{self.hostname}:{self.port_id}/{self.username}')
        
        return self
       
    def __exit__(self, exc_type, exc_value, traceback):
        
        if traceback is None:
            self.db.commit()
        else:
            self.db.rollback()
        
        self.cur.close()
        self.db.close()
            
    def archive_tab(self, tab, name = None, date = None, comp_type = None, region = None):

        self.cur.execute(f'CREATE SCHEMA "{name}"')
        self.db.commit()
        
        df_info = pd.DataFrame({"name": name, "date": date, "comp_type": comp_type, "region": region}, index = [0])
        df_info.to_sql("info", self.engine, schema = f"{name}")
        
        for key, value in tab.items():
            
            if type(value) is list:
                for i in value:
                    for inner_key, inner_value in i.items():
                        inner_value.to_sql(key + " " + inner_key, self.engine, schema = f"{name}")
            else:
                value.to_sql(key, self.engine, schema = f"{name}")

    def archive_motions(self, tab, name = None, date = None, comp_type = None, region = None):
        # need to add date, region, comp type, possibly CAs, in-round or out-round, maybe type (e.g. THW), maybe catogory (e.g. econ), room number, exact balance, maybe average speaks

        if "Motions Tab" in tab:
            motions_tab = tab["Motions Tab"]
            
        elif "Motions" in tab:
            motions_tab = tab["Motions"]
            motions_tab = motions_tab.reindex(columns= motions_tab.columns.tolist() + ['info_slide', 'motion_type',
                   'og_balance', 'oo_balance', 'cg_balance', 'co_balance'])
        
        else:
            return
            
        # new motion DataFrame
        motions_df = motions_tab
        motions_df = pd.concat([pd.DataFrame([[name, date, comp_type, region]], index = motions_df.index, columns = ["comp_name", "date", "comp_type", "region"]), motions_df], axis = 1)
        
        # motion types
        motion_types = []
        
        types = {"THBT": ("thbt", "th believes", "this house believes", "th, believes", "this house, believes"),
                 "THS": ("ths", "th supports", "this house supports", "th, supports", "this house, supports"),
                 "THO": ("tho", "th opposes", "this house opposes", "th, opposes", "this house, opposes"),
                 "THW": ("thw", "th would", "this house would", "th, would", "this house, would"),
                 "THR": ("thr", "th regrets", "this house regrets", "th, regrets", "this house, regrets"),
                 "THP": ("thp", "th prefers", "this house prefers", "th, prefers", "this house, prefers"),
                 "Actor": ("th, as", "th as", "this house, as", "this house as")
                 }
        
        for motion in motions_tab["motion"]:
            type_ = next((key for key, value in types.items() if motion.lower().startswith(value)), "Unknown")
        
        for motion in motions_tab["motion"]:
            type_ = next((key for key, value in types.items() if motion.lower().startswith(value)), "Unknown")

        
            motion_types.append(type_)
            
        motions_df["motion_category"] = motion_types
        
        # calculate room number
        if "Results" in tab and "Round 1" in tab["Results"][0].keys():
            round_1 = len(tab["Results"][0]["Round 1"])
            room_number = -1*(-round_1//4)
            motions_df["room_number"] = room_number
            
        else:
            motions_df["room_number"] = None
        
        # calculate exact balance
        inround_motions = motions_tab.loc[motions_tab["round_type"] == "in-round"]
        outround_motions = motions_tab.loc[motions_tab["round_type"] == "out-round"]

        motion_balance = []  # list of lists containing og, oo, cg, co balance per motiom
        rank_1_4 = [] # list of lists containing number of 1-4 for og, oo, cg, co per motiom
        
        for row in inround_motions.itertuples():
            position_balance = []
            list_of_ranks = []

            round_results = next(
                (item[row.round] for item in tab["Results"] if row.round in item), None)
            if round_results is None:
                position_balance.append([None]*4)

            else:
                positions = ["Opening Government", "Opening Opposition",
                             "Closing Government", "Closing Opposition"]
                for i in positions:
                    position_df = round_results.loc[round_results["Side"] == i]
                    balance = position_df["Result"].mean() - 1
                    position_balance.append(balance)

                    posible_rank = [1, 2, 3, 4]
                    for rank in posible_rank:
                        number_of_rank = (position_df["Result"] == rank).sum()
                        list_of_ranks.append(number_of_rank)

            motion_balance.append(position_balance)
            rank_1_4.append(list_of_ranks)
        
        for row in outround_motions.itertuples():
            motion_balance.append([None]*4)
            rank_1_4.append([None]*16)
        
        # add exact motion balance and rankings to motions_df
        balance_columns = ["precise_og_balance",
                           "precise_oo_balance",
                           "precise_cg_balance",
                           "precise_co_balance"]
        
        rankings_columns = ["og_number_1st",
                            "og_number_2nd",
                            "og_number_3rd",
                            "og_number_4th",
                            "oo_number_1st",
                            "oo_number_2nd",
                            "oo_number_3rd",
                            "oo_number_4th",
                            "cg_number_1st",
                            "cg_number_2nd",
                            "cg_number_3rd",
                            "cg_number_4th",
                            "co_number_1st",
                            "co_number_2nd",
                            "co_number_3rd",
                            "co_number_4th"]
        
        motions_df = motions_df.join(pd.DataFrame(motion_balance, columns=balance_columns))
        motions_df = motions_df.join(pd.DataFrame(rank_1_4, columns=rankings_columns))    
        
        # avg and std speaks
        avg_speaks_list = []
        std_speaks_list = []

        for row in inround_motions.itertuples():
            round_ = "R" + row.round[-1]

            if "Speaker Tab" in tab and round_ in tab["Speaker Tab"].columns:
                speaker_tab = tab["Speaker Tab"]
                avg_speaks = speaker_tab[round_].mean()
                avg_speaks_list.append(avg_speaks)
                std_speaks = speaker_tab[round_].std()
                std_speaks_list.append(std_speaks)

            else:
                avg_speaks_list.append(None)
                std_speaks_list.append(None)
        
        for row in outround_motions.itertuples():
            avg_speaks_list.append(None)
            std_speaks_list.append(None)
                
        motions_df["avg_speaks"] = avg_speaks_list
        motions_df["std_speaks"] = std_speaks_list
        motions_df.to_sql("motions", self.engine, schema = "general", if_exists = "append")

        return motions_df
        