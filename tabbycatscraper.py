from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
import requests
import itertools
import re


class TabbycatScraper:
    
    def __init__(self, path):
        
        self.path = path
        self.structure = self.get_head()
    
    def get_tab(self, get_ballots = True, get_results_debate_view = True, get_staff = True):
        
        pages_to_scrap = {"Team Tab": self.get_team_tab,
                          "Speaker Tab": self.get_speaker_tab,
                          "Motions Tab": self.get_motions_tab,
                          "Motions": self.get_motions,
                          "ESL Speakers": self.get_speaker_tab,
                          "EFL Speakers": self.get_speaker_tab,
                          "English as a Second Language Speakers": self.get_speaker_tab,
                          "English as a Foreign Language Speakers": self.get_speaker_tab,
                          "Novice Speakers": self.get_speaker_tab,
                          "ESL Teams": self.get_team_tab,
                          "EFL Teams": self.get_team_tab,
                          "Pro Am Teams": self.get_team_tab,
                          "Novice Teams": self.get_team_tab}
        
        dataframe_dict = {}
        
        for key, value in pages_to_scrap.items():
            
            if key in self.structure:
                link = self.structure[key]
                link_no_comp = link.split("/", 2)[2]
                url = self.path + link_no_comp
                df = value(url)
                dataframe_dict.update({key: df})
                
        if "Participants" in self.structure:
            
            link = self.structure["Participants"]
            link_no_comp = link.split("/", 2)[2]
            url = self.path + link_no_comp
            df_speakers = self.get_speakers(url)
            df_judges = self.get_judges(url)
            dataframe_dict.update({"Speakers": df_speakers, "Judges": df_judges})
            
        if "Results" in self.structure:
            
            round_results = []
            
            for debate_round in self.structure["Results"]:
                
                link = list(debate_round.values())[0]
                link_no_comp = link.split("/", 2)[2]
                url = self.path + link_no_comp
                df = self.get_round_results(url)
                
                results_dict = {list(debate_round.keys())[0]: df}
                
                if get_results_debate_view:                
                    debate_view_url = self.path + link_no_comp + "?view=debate"
                    df_debate_veiw = self.get_round_results_team(debate_view_url)
                    results_dict.update({list(debate_round.keys())[0] + "_debate_view": df_debate_veiw})
                    
                round_results.append(results_dict)
            
            dataframe_dict.update({"Results": round_results})
            
        if "Break" in self.structure:
            
            break_results = []
            
            for break_ in self.structure["Break"]:
                link = list(break_.values())[0]
                link_no_comp = link.split("/", 2)[2]
                url = self.path + link_no_comp

                if "Adjudicators" in break_.keys():
                    df = self.get_judge_break(url)
                else:
                    df = self.get_speaker_break(url)
                    
                break_results.append({list(break_.keys())[0]: df})
            
            dataframe_dict.update({"Break": break_results})
        
        if get_ballots:
            df_ballots = pd.DataFrame()
            
            try:
                for i in itertools.count(start=1):
                    ballot_path = self.path + f"results/debate/{i}/scoresheets/"
                    ballot = self.get_ballot(ballot_path)
                    df_ballots = pd.concat([df_ballots, ballot], ignore_index=True)
    
            except Exception:
                dataframe_dict.update({"Ballots": df_ballots})
                pass
        
        if get_staff:
            staff = self.get_tournament_staff(self.path)
            if staff is not None:
                dataframe_dict.update({"Tournament_Staff": staff})
        
        return dataframe_dict
            
    def get_soup(self, page_path):
        if page_path[0:4] == "http":
            html_content = requests.get(page_path).text
        else:
            with open(page_path, "r",  encoding="utf8") as html_file:
                html_content = html_file.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        return soup
    
    def get_head(self):
        
        # get names and links form header.
        # returns dict of items in header.
        # each item is a dictonary with the name as the key and link as the value.
        # if the item is a drop down the value is a list of dictionaries
        
        soup = self.get_soup(self.path)
        tags = soup.nav.find_all("li")
        a_tags = [i.find_all("a") for i in tags]
        headings = [[i.string.strip() for i in k] for k in a_tags]
        links = [[i["href"] for i in k] for k in a_tags]
        
        def dictionary_structure(headings, links):
            dictionary = {}
            for idx, i in enumerate(headings): 
                if len(i) == 1:
                    dictionary.update({i[0]: links[idx][0]})
                else:
                    list_of_dictionaries = []
                    for k in range(len(i)-1):
                        list_of_dictionaries.append({i[k+1]: links[idx][k+1]})
                    dictionary.update({i[0]: list_of_dictionaries})
            return dictionary
        
        head_structure = dictionary_structure(headings, links)
    
        return head_structure 

    def get_tournament_staff(self, path):
        
        # get json string 
        soup = self.get_soup(path)
        staff = soup.find('div', {'class': 'card mt-3'})
        if staff is not None:
            staff_body = staff.find('div', {'class': 'card-body'}).text
            df = pd.DataFrame()
            df["Tournament_Staff"] = staff_body

        else:
            return
        
        return df

    
    def get_judges(self, path):
        
        # get json string and split for judges and speakers
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        cut_tag_0_split = cut_tag_0.split(', {"head":', 1)
        raw_data_judges = json.loads(cut_tag_0_split[0])
        
        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data_judges["head"]]
        table_data = [[k["text"] if "text" in k else k["icon"] for k in i] for i in raw_data_judges["data"]]
        df = pd.DataFrame(table_data, columns=column_headings)
        
        # replace check in columns Member of the Adjudication Core and Independent Adjudicator                                         
        df["Member of the Adjudication Core"] = df["Member of the Adjudication Core"].replace({"check": True, "": False})
        df["Independent Adjudicator"] = df["Independent Adjudicator"].replace({"check": True, "": False})

        return df

    def get_speakers(self, path):
        
        # get json string and split for judges and speakers
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        cut_tag_0_split = cut_tag_0.split(', {"head":', 1)
        raw_data_speaker = json.loads('{"head":' + cut_tag_0_split[1])
        
        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data_speaker["head"]]
        table_data = [[k["text"] for k in i] for i in raw_data_speaker["data"]]
        df = pd.DataFrame(table_data, columns=column_headings)
        
        # add full team name, emoji and team members form popover for team name
        team_column = column_headings.index("Team")
        
        full_team_name = [j[team_column]["popover"]["title"] if "popover" in j[team_column] and "title" in j[team_column]["popover"] else "" for j in raw_data_speaker["data"]]
        df.insert(df.columns.get_loc("Team") + 1, "Team_Long", full_team_name)
        
        emoji = [j[team_column]["emoji"] if "emoji" in j[team_column] else "" for j in raw_data_speaker["data"]]
        df.insert(df.columns.get_loc("Team") + 2, "Emoji", emoji)
        
        team_members = [j[team_column]["popover"]["content"][0]["text"] if "popover" in j[team_column] and "content" in j[team_column]["popover"] else "" for j in raw_data_speaker["data"]]
        team_member_1 = [i.split(", ", 1)[0] if ", " in i else i for i in team_members]
        team_member_2 = [i.split(", ", 1)[1] if ", " in i else i for i in team_members]
        df.insert(df.columns.get_loc("Team") + 3, "Team_Member_1", team_member_1)
        df.insert(df.columns.get_loc("Team") + 4, "Team_Member_2", team_member_2)
        
        return df
    
    def get_speaker_tab(self, path):

        # get json string
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        raw_data = json.loads(cut_tag_0)

        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data["head"]]
        table_data = [[k["text"] for k in i] for i in raw_data["data"]]
        df = pd.DataFrame(table_data, columns=column_headings)
        
        # add full team name, emoji and team members form popover
        team_column = column_headings.index("Team")
        full_team_name = [j[team_column]["popover"]["title"] if "popover" in j[team_column] and "title" in j[team_column]["popover"] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 1, "Team_Long", full_team_name)
        emoji = [j[team_column]["emoji"] if "emoji" in j[team_column] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 2, "Emoji", emoji)

        team_members = [j[team_column]["popover"]["content"][0]["text"] if "popover" in j[team_column] and "content" in j[team_column]["popover"] else "" for j in raw_data["data"]]
        team_member_1 = [i.split(", ", 1)[0] if ", " in i else i for i in team_members]
        team_member_2 = [i.split(", ", 1)[1] if ", " in i else i for i in team_members]
        df.insert(df.columns.get_loc("Team") + 3, "Team_Member_1", team_member_1)
        df.insert(df.columns.get_loc("Team") + 4, "Team_Member_2", team_member_2)
        
        # make Rank type float
        df["Rank"] = df["Rank"].apply(lambda x: x[:-1] if x[-1] == "=" else x)
        df["Rank"] = pd.to_numeric(df["Rank"], errors='coerce')
        
        # make rounds type float
        for i in itertools.count(start=1):
            if f"R{i}" in df.columns:
                df[f"R{i}"] = df[f"R{i}"].replace({"—": np.nan})
                df[f"R{i}"] = pd.to_numeric(df[f"R{i}"], errors='coerce')

            else:
                break

        # make Total, Avg, Stdev and Trim type float
        if "Total" in df.columns:
            df["Total"] = pd.to_numeric(df["Total"], errors='coerce')

        if "Avg" in df.columns:
            df["Avg"] = df["Avg"].apply(lambda x: re.sub("[^0-9,.,—]", "", x))
            df["Avg"] = pd.to_numeric(df["Avg"], errors='coerce')

        if "Stdev" in df.columns:
            df["Stdev"] = df["Stdev"].apply(lambda x: re.sub("[^0-9,.,—]", "", x))
            df["Stdev"] = pd.to_numeric(df["Stdev"], errors='coerce')
            
        if "Trim" in df.columns:
            df["Trim"] = df["Trim"].apply(lambda x: re.sub("[^0-9,.,—]", "", x))
            df["Trim"] = pd.to_numeric(df["Trim"], errors='coerce')

        return df
    
    def get_team_tab(self, path):
        
        # get json string
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        raw_data = json.loads(cut_tag_0)
        
        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data["head"]]
        table_data = [[k["text"] for k in i] for i in raw_data["data"]]
        df = pd.DataFrame(table_data, columns=column_headings)
        
        # make Rank type float
        df["Rank"] = df["Rank"].apply(lambda x: x[:-1] if x[-1] == "=" else x)
        df["Rank"] = pd.to_numeric(df["Rank"], errors='coerce')
        
        # make rounds type float 
        for i in itertools.count(start=1):
            if f"R{i}" in df.columns:
                df[f"R{i}"] = df[f"R{i}"].str[0]
                df[f"R{i}"] = pd.to_numeric(df[f"R{i}"], errors='coerce')

            else:
                break
        
        # add full team name, emoji and team members form popover for team name
        team_column = column_headings.index("Team")
        
        full_team_name = [j[team_column]["popover"]["title"] if "popover" in j[team_column] and "title" in j[team_column]["popover"] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 1, "Team_Long", full_team_name)
        
        emoji = [j[team_column]["emoji"] if "emoji" in j[team_column] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 2, "Emoji", emoji)
        
        team_members = [j[team_column]["popover"]["content"][0]["text"] if "popover" in j[team_column] and "content" in j[team_column]["popover"] else "" for j in raw_data["data"]]
        team_member_1 = [i.split(", ", 1)[0] if ", " in i else i for i in team_members]
        team_member_2 = [i.split(", ", 1)[1] if ", " in i else i for i in team_members]
        df.insert(df.columns.get_loc("Team") + 3, "Team_Member_1", team_member_1)
        df.insert(df.columns.get_loc("Team") + 4, "Team_Member_2", team_member_2)
        
        # add speaks and other teams in debate form popovers for each round
        for i in itertools.count(start=1):
            if f"R{i}" in column_headings:
                round_name = f"R{i}"
                round_index = column_headings.index(f"R{i}")
                
                speaks = [float(j[round_index]["subtext"].split("<", 1)[0]) if "subtext" in j[round_index] else np.nan if "text" in j[round_index] and j[round_index]["text"] == "\u2014" else np.nan for j in raw_data["data"]]
                df.insert(df.columns.get_loc(round_name) + 1, f"{i}_speaks", speaks)
                
                other_teams = [j[round_index]["popover"]["content"][0]["text"] if "popover" in j[round_index] else "\u2014" if "text" in j[round_index] and j[round_index]["text"] == "\u2014" else None for j in raw_data["data"]]
                df.insert(df.columns.get_loc(round_name) + 2, f"{i}_teams", other_teams)
                df[f"{i}_teams"] = df[f"{i}_teams"].str.replace("Teams in debate:|<br />|<strong>|</strong>", "", regex=True)
                
            else:
                break
        
        # make Pts, Spk, 1sts and 2nds type float
        if "Pts" in df.columns:
            df["Pts"] = pd.to_numeric(df["Pts"], errors='coerce')
            
        if "Spk" in df.columns:
            df["Spk"] = pd.to_numeric(df["Spk"], errors='coerce')

        if "1sts" in df.columns:
            df["1sts"] = pd.to_numeric(df["1sts"], errors='coerce')
                
        if "2nds" in df.columns:
            df["2nds"] = pd.to_numeric(df["2nds"], errors='coerce')

        return df
    
    def get_round_results(self, path):
        
        # get json string
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        raw_data = json.loads(cut_tag_0)
        
        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data["head"]]
        table_data = [[k["text"] for k in i] for i in raw_data["data"]]
        df = pd.DataFrame(table_data, columns=column_headings)
        if "The ballot you submitted" in df.columns:
            df = df.drop(columns = "The ballot you submitted")
        
        # make Results type float
        if not df["Result"][0] == "advancing" and not df["Result"][0] == "eliminated":
            df["Result"] = df["Result"].str[0]
            df["Result"] = pd.to_numeric(df["Result"], errors='coerce')
        
        # remove tags from Adjudicators and split into Chair and Wings
        df["Adjudicators"] = df["Adjudicators"].str.replace(
            """<span class="d-inline">|<i class='adj-symbol'>|</i>|<div class='clearfix pt-1 pb-1 d-block d-md-none'>|</div>|<span class='d-none d-md-inline'>, </span>""", "", regex=True)
        df["Adjudicators"] = df["Adjudicators"].str.replace("</span>", ",", regex=True)
        df[["Adjudicators", "Wings"]] = df["Adjudicators"].str.split("Ⓒ,", 1, expand=True)
        df.rename(columns={"Adjudicators": "Chair"}, inplace=True)
        
        # add full team name, emoji and team members form popover for team name
        team_column = column_headings.index("Team")
        
        full_team_name = [j[team_column]["popover"]["title"] if "popover" in j[team_column] and "title" in j[team_column]["popover"] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 1, "Team_Long", full_team_name)
        
        emoji = [j[team_column]["emoji"] if "emoji" in j[team_column] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 2, "Emoji", emoji)

        team_members = [j[team_column]["popover"]["content"][0]["text"] if "popover" in j[team_column] and "content" in j[team_column]["popover"] else "" for j in raw_data["data"]]
        team_member_1 = [i.split(", ", 1)[0] if ", " in i else i for i in team_members]
        team_member_2 = [i.split(", ", 1)[1] if ", " in i else i for i in team_members]
        df.insert(df.columns.get_loc("Team") + 3, "Team_Member_1", team_member_1)
        df.insert(df.columns.get_loc("Team") + 4, "Team_Member_2", team_member_2)
        
        # add teams in debate
        results_column = column_headings.index("Result")
        teams_in_debate = [j[results_column]["popover"]["content"][0]["text"] if "popover" in j[results_column] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Result") + 1, "Debate_Teams", teams_in_debate)
        df["Debate_Teams"] = df["Debate_Teams"].str.replace("Teams in debate:|<br />|<strong>|</strong>", "", regex=True)
        
        return df
    
    def get_round_results_team(self, path):

        # get json string
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        raw_data = json.loads(cut_tag_0)
        
        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data["head"]]
        table_data = [[k["text"] for k in i] for i in raw_data["data"]]
        df = pd.DataFrame(table_data, columns=column_headings)
        if "The ballot you submitted" in df.columns:
            df = df.drop(columns = "The ballot you submitted")
        
        # remove tags from Adjudicators and split into Chair and Wings
        df["Adjudicators"] = df["Adjudicators"].str.replace(
            """<span class="d-inline">|<i class='adj-symbol'>|</i>|<div class='clearfix pt-1 pb-1 d-block d-md-none'>|</div>|<span class='d-none d-md-inline'>, </span>""", "", regex=True)
        df["Adjudicators"] = df["Adjudicators"].str.replace("</span>", ",", regex=True)
        df[["Adjudicators", "Wings"]] = df["Adjudicators"].str.split("Ⓒ,", 1, expand=True)
        df.rename(columns={"Adjudicators": "Chair"}, inplace=True)
        
        # add rank, full_team_name and team_members for each team
        positions = ["Og", "Oo", "Cg", "Co"]
        
        for position in positions:
            position_index = column_headings.index(f"{position}")
            
            full_team_name = [j[position_index]["popover"]["title"] if "popover" in j[position_index] and "title" in j[position_index]["popover"] else "" for j in raw_data["data"]]
            full_team_name_no_position = [i.split(" placed", 1)[0] for i in full_team_name]
            df.insert(df.columns.get_loc(position) + 1, f"Team_Long_{position}", full_team_name_no_position)
            
            rank = [i[position_index]["icon"] if "icon" in i[position_index] else "" for i in raw_data["data"]]
            rank_dict = {"chevrons-up": 1, "chevron-up": 2, "chevron-down": 3, "chevrons-down": 4, "": None}
            rank_numeric = [rank_dict[i] for i in rank]
            df.insert(df.columns.get_loc(position) + 2, f"Rank_{position}", rank_numeric)
            
            team_members = [j[position_index]["popover"]["content"][0]["text"] if "popover" in j[position_index] and "content" in j[position_index]["popover"] else "" for j in raw_data["data"]]
            team_member_1 = [i.split(", ", 1)[0] if ", " in i else i for i in team_members]
            team_member_2 = [i.split(", ", 1)[1] if ", " in i else i for i in team_members]
            df.insert(df.columns.get_loc(position) + 3, f"Team_Member_1_{position}", team_member_1)
            df.insert(df.columns.get_loc(position) + 4, f"Team_Member_2_{position}", team_member_2)
        
        return df
    
    def get_motions_tab(self, path):
        
        soup = self.get_soup(path)
        
        # find rounds in soup
        rounds = soup.find_all('div', {'class': 'list-group mt-3'})
        
        # find the name of each round and find if they are an in or an out round
        round_name_text = [i.find('span', {'class': "badge badge-secondary"}).text for i in rounds]
        round_types = ["in-round" if i.startswith("Round") else "out-round" for i in round_name_text]
        
        # find motion for each round
        motions_text = [i.find('h4').contents[0].text.strip() for i in rounds]
        motions_no_n = [i.replace("\n", "") for i in motions_text]
        
        # find motion types for each round
        motion_types = [i.find('h4').contents[1].text.strip() if len(i.find('h4').contents) > 1 else None for i in rounds]
        
        # find info slide for each round
        info_slides_text = [i.find('div', {'class': 'modal-body lead'}).text.strip() if i.find('div', {'class': 'modal-body lead'}) != None else i.find('div', {'class': 'modal-body lead'}) for i in rounds]
        info_slide_no_n = [i.replace("\n", "") if i !=None else i for i in info_slides_text]
        
        # get in-round balance
        position_names = ["og", "oo", "cg", "co"]
        in_round_balance = [[float(i.find('div', {'class': 'progress-bar progress-bar-{}'.format(t)}).text.strip()[-4:]) if k == "in-round" else None for k, i in zip(round_types, rounds)] for t in position_names]
        
        # get out-round results
        out_rounds = [i.find_all('div', {'class': "col-md-3 mb-3"}) for i in rounds]
        out_round_balance_raw = [[float(i.find('div', {'class': "progress-bar"})['style'].split("%", 1)[0][6:])/100 for i in k] for k in out_rounds]
        out_round_balance = [[i[k] if len(i) != 0 else None for i in out_round_balance_raw] for k in range(4)]
        all_round_balance = [[i if i != None else k for i, k in zip(t, q)] for t, q in zip(in_round_balance, out_round_balance)]
        
        # make DataFrame
        df = pd.DataFrame({"round": round_name_text, "round_type": round_types, "motion": motions_no_n, "info_slide": info_slide_no_n, "motion_type": motion_types,"og_balance": all_round_balance[0], "oo_balance": all_round_balance[1], "cg_balance": all_round_balance[2], "co_balance": all_round_balance[3]})
        
        return df
    
    def get_ballot(self, path):
    
        # get json string
        soup = self.get_soup(path)

        # get round_ and room
        head = soup.find('div', {'class': 'container-fluid'})
        round_and_room = head.find("small", {"class": "text-muted d-md-inline d-block"}).text
        round_and_room_split = round_and_room.split("@", 1)
        round_ = round_and_room_split[0].strip()
        room = round_and_room_split[1].strip()
        
        # get motion
        motion_branch = head.find("div", {"class": "card-body"})
        extract_tag = motion_branch.find("h4", {"class": "card-title"})
        extract_tag.extract()
        motion = motion_branch.text.strip()
        
        # find speaker name, speaker speaks, team name and team speaks
        data_branch = head.find("div", {"class": "card mt-3"}).find("div", {"class": "row pl-3 pt-3 p-0"})
        team_data_branchs = data_branch.find_all("div", {"class": "col-6 list-group mb-3"})

        team_data = []

        for team in team_data_branchs:
            
            speakers_and_team = team.find_all("li", {"class": "list-group-item"})
            speakers = speakers_and_team[:2]
            team = speakers_and_team[2]

            for speaker in speakers:

                contents = speaker.contents
                name = contents[2].strip()
                speaks = contents[3].text.strip()
                
                team_data.append(name)
                team_data.append(speaks)
            
            team_contents = team.contents
            team_name_long = team_contents[1].text.strip()
            team_name_no_begining = team_name_long.split("Total for ", 1)[1]
            team_name = team_name_no_begining[::-1].split("( ", 1)[1][::-1]
            
            team_speaks = team_contents[3].text.strip()
            
            team_data.append(team_name)
            team_data.append(team_speaks)
        
        # make dataframe
        df = pd.DataFrame(columns = ["Round", "Room", "Motion", "PM", 
                                     "PM_speaks", "DPM", "DPM_speaks", "Team_OG",
                                     "Team_OG_speaks", "LO", "LO_speaks", "DLO",
                                     "DLO_speaks", "Team_OO", "Team_OO_speaks", "MG",
                                     "MG_speaks", "GW", "GW_speaks", "Team_CG",
                                     "Team_CG_speaks", "MO", "MO_speaks", "OW",
                                     "OW_speaks", "Team_CO", "Team_CO_speaks"])
        
        final_data = [round_] + [room] + [motion] + team_data
        
        df.loc[0] = final_data
        
        return df    
    
    def get_motions(self, path):
        
        soup = self.get_soup(path)
        
        # find rounds in soup
        rounds = soup.find_all('div', {'class': 'card mt-3'})
        
        # find the name of each round and find if they are an in or an out round
        round_name_text = [i.find('h4', {'class': "card-title mt-0 mb-2 d-inline-block"}).text.strip() for i in rounds]
        round_types = ["in-round" if i.startswith("Round") else "out-round" for i in round_name_text]
        
        # find motion for each round
        motions_text = [i.find('div', {'class': "mr-auto pr-3 lead"}).text.strip() for i in rounds]
        
        # make DataFrame
        df = pd.DataFrame({"round": round_name_text, "round_type": round_types, "motion": motions_text})
        
        return df
    
    def get_speaker_break(self, path):
        
        # get json string and split for judges and speakers
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        raw_data = json.loads(cut_tag_0)
        
        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data["head"]]
        table_data = [[k["text"] for k in i] for i in raw_data["data"]]
        df = pd.DataFrame(table_data, columns=column_headings)
        
        # add full team name, emoji and team members form popover for team name
        team_column = column_headings.index("Team")
        
        full_team_name = [j[team_column]["popover"]["title"] if "popover" in j[team_column] and "title" in j[team_column]["popover"] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 1, "Team_Long", full_team_name)
        
        emoji = [j[team_column]["emoji"] if "emoji" in j[team_column] else "" for j in raw_data["data"]]
        df.insert(df.columns.get_loc("Team") + 2, "Emoji", emoji)

        # make Rank and Break type float
        df["Rank"] = df["Rank"].apply(lambda x: x[:-1] if x[-1] == "=" else x)
        df["Rank"] = pd.to_numeric(df["Rank"], errors='coerce')
        
        # "(different break)" or "(withdrawn)" gets turned to NaNs
        df["Break"] = df["Break"].apply(lambda x: x[:-1] if x[-1] == "=" else x)
        df["Break"] = pd.to_numeric(df["Break"], errors='coerce')
        
        # make Pts, Spk, 1sts and 2nds type float
        if "Pts" in df.columns:
            df["Pts"] = pd.to_numeric(df["Pts"], errors='coerce')
            
        if "Spk" in df.columns:
            df["Spk"] = df["Spk"].str.split("<", 1).str[0]
            df["Spk"] = pd.to_numeric(df["Spk"], errors='coerce')

        if "1sts" in df.columns:
            df["1sts"] = pd.to_numeric(df["1sts"], errors='coerce')
                
        if "2nds" in df.columns:
            df["2nds"] = pd.to_numeric(df["2nds"], errors='coerce')

        return df

    def get_judge_break(self, path):
        
        # get json string and split for judges and speakers
        soup = self.get_soup(path)
        tags = soup.find_all('script')
        cut_tag_0 = tags[-4].text.split("[", 1)[1][:-10]
        raw_data = json.loads(cut_tag_0)

        # create dataframe from of table
        column_headings = [i["title"] if "title" in i else i["tooltip"] for i in raw_data["head"]]
        table_data = [[k["text"] if "text" in k else k["icon"] for k in i] for i in raw_data["data"]]
        df = pd.DataFrame(table_data, columns=column_headings) 
        
        # replace check in columns Member of the Adjudication Core and Independent Adjudicator                                         
        df["Member of the Adjudication Core"] = df["Member of the Adjudication Core"].replace({"check": True, "": False})
        df["Independent Adjudicator"] = df["Independent Adjudicator"].replace({"check": True, "": False})
        
        return df