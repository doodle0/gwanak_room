import pickle

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium, folium_static
import util

class FilterElem:
    def __init__(self, name, sql_name):
        self.name = name
        self.sql_name = sql_name

class RangeElem(FilterElem):
    def __init__(self, name, sql_name, typ, min_val, max_val):
        super().__init__(name, sql_name)
        self.typ = typ
        min_val, max_val = map(typ, (min_val, max_val))
        self.st_slider = st.slider(name, min_val, max_val, (min_val, max_val))

    def to_sql_clause(self):
        bt, tp = self.get_st_elem()
        return f'{self.sql_name} BETWEEN {bt} AND {tp}'

    def get_st_elem(self):
        return self.st_slider

class OptionElem(FilterElem):
    def __init__(self, name, sql_name, options):
        super().__init__(name, sql_name)
        self.options = options
        self.st_selectbox = st.selectbox(name, options)

    # def parse_input(self, inp: str):
    #     if inp not in self.options:
    #         raise ValueError()
    #     # self.val = inp.strip()

    def to_sql_clause(self):
        return f'{self.sql_name}="{self.get_st_elem()}"'

    def get_st_elem(self):
        return self.st_selectbox

def make_inp_vec(elem):
    near_stn = util.StnInfo.get_obj_by_id(elem['near_stn'])
    return pd.Series([
        elem['area'],
        elem['year'],
        elem['is_oneroom'],
        elem['is_officetel'],
        elem['elem_school'],
        elem['has_elevator'],
        # stn.id,
        near_stn.get_mean_tfc(),
        # stn.get_mean_tfc() / elem[11],
        # stn.get_mean_tfc() / elem[11] ** 2
    ])


# weights = pickle.load(open('weights.pkl', 'rb'))
weights = [1.399e+00, 1.207e+00, -1.086e+01, 7.967e-02, 1.541e+00, 1.118e+01, 4.425e-05, -2.998e-03, -2.402e+03]
def get_exp_monthly_rt(inp_vec, deposit):
    return float(weights @ pd.Series([*inp_vec, deposit, 1]).astype(float))

def check_interval_overlap(a: tuple, b: tuple):
    if a[0] > b[0]: a, b = b, a
    return b[0] <= a[1]

STATION_CAND = [stn.name for stn in util.StnInfo.STATIONS]

def input_filter():
    st.write('## 필터 입력')
    st.write('### 집 위치')
    stn_name = st.selectbox('기준 역', STATION_CAND)
    stn = util.StnInfo.get_obj_by_name(stn_name)
    stn_filter = [
        stn,
        RangeElem('역과의 거리', stn.sql_dist_name, float, 0, 1000)
    ]

    st.write('### 집 제원')
    db_filter = [
        RangeElem('면적(m^2)', 'area', float, 0, 100),
        RangeElem('사용승인연도', 'year', int, 1960, 2023),
        RangeElem('층번호', 'floor', int, 1, 30),
    ]

    st.write('### 희망 보증금, 월세 범위')
    search_filter = [
        RangeElem('보증금', 'deposit', int, 0, 10000),
        RangeElem('월세', 'monthly_rt', int, 0, 200)
    ]

    return {"stn_filter": stn_filter, "db_filter": db_filter, "search_filter": search_filter}

def print_filtered_result(sql, stn_filter, db_filter, search_filter):
    # st.write('# 입력 필터')
    # for ft in db_filter + search_filter:
    #     st.write(ft.to_sql_clause())

    stn, stn_dist = stn_filter
    deposit, monthly_rt = search_filter

    # DB의 모든 data 중 필터에 맞는 것 필터링 (SQLite)
    selection = [
        'addr',
        'name',
        'latitude',
        'longitude',
        'area',
        'year',
        'floor',
        'is_oneroom',
        'is_officetel',
        'elem_school',
        'has_elevator',
        'has_parking',
        'near_stn',
    ]

    sql.execute(f"""SELECT {', '.join(selection)} FROM building_rooms_view
        WHERE {' AND '.join(f.to_sql_clause() for f in db_filter + [stn_dist])}""")
    res = sql.fetchall()

    st.write('## 검색 결과')
    result = pd.DataFrame(columns=['주소', '건물명', 'lat', 'lng', '면적(m^2)', '사용승인연도', '층번호', '예상 월세'])
    for elem in res:
        dep_bt, dep_tp = deposit.get_st_elem()
        v = make_inp_vec(dict(zip(selection, elem)))
        exp_min_rt = get_exp_monthly_rt(v, deposit=dep_tp)
        exp_max_rt = get_exp_monthly_rt(v, deposit=dep_bt)
        # 사용자가 설정한 월세 필터 범위와 예측 월세 범위에 겹치는 부분이 있는지 확인
        if check_interval_overlap((exp_min_rt, exp_max_rt), monthly_rt.get_st_elem()):
            result.loc[len(result)] = [*elem[:7], f'{exp_min_rt:.0f}~{exp_max_rt:.0f}']
    st.write(result[['주소', '건물명', '면적(m^2)', '사용승인연도', '층번호', '예상 월세']])
    map_visualize(result, stn, stn_dist.get_st_elem()[1])

    return result

def map_visualize(result, stn: util.StnInfo, max_distance):  # max_distance는 필터링에서 설정한 역과의 최대 거리
    map = folium.Map(location=[stn.lat, stn.lng], zoom_start=14)  # 역의 위도 경도 가져오기, zoom_start 크기로 지도의 시작 줌인 정도를 정할 수 있음.
    for idx, row in result.iterrows():
        # 필터링 데이터의 위도 경도 가져오기
        lat_ = row['lat']
        lng_ = row['lng']

        # 필터링 데이터를 지도 위에 표시하기
        popup = folium.Popup(row['주소'], max_width=300)
        folium.Marker(location=(lat_, lng_),
                      radius=15,
                      popup=popup).add_to(map)

    folium.Circle(radius=max_distance,  # 여기서 max_distance의 단위는 m입니다.
                  location=(stn.lat, stn.lng),
                  color="#ff7800",
                  fill_color='#ffff00',
                  fill_opacity=0.2
                  ).add_to(map)  # max_distance를 바탕으로 필터링된 반경을 시각화
 
    folium_static(map)