import streamlit as st
import pandas as pd
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

    # def parse_input(self, inp: str):
    #     if not inp.strip():
    #         self.min_val, self.max_val = 0, math.inf
    #     elif '~' in inp:
    #         self.min_val, self.max_val = [s.strip() for s in inp.split('~')]
    #         self.min_val = self.typ(self.min_val) if self.min_val else 0
    #         self.max_val = self.typ(self.max_val) if self.max_val else math.inf
    #     else:
    #         self.min_val = self.max_val = float(inp.strip())

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


def get_exp_monthly_rt(near_stn, stn_dist, area, year, flr, deposit):
    return  -2188.02956787\
            +1.10811316e+00 * float(year)\
            +6.19664494e-01 * float(flr)\
            +1.26259579e+00 * float(area)\
            -2.73874393e-03 * float(deposit)\
            -4.29419137e-02 * float(stn_dist)\
            +(5.49190785e+00, -5.83814867e+00, 3.46240821e-01)[STATION_CAND.index(near_stn)]

def check_interval_overlap(a: tuple, b: tuple):
    if a[0] > b[0]: a, b = b, a
    return b[0] <= a[1]

STATION_CAND = [stn.name for stn in util.StnInfo.STATIONS]

def input_filter():
    st.write('## 필터 입력')
    st.write('### 집 제원')
    stn_name = st.selectbox('기준 역', STATION_CAND)
    stn_id = util.StnInfo.get_id_by_name(stn_name)
    db_filter = [
        RangeElem('역과의 거리', 'dist_' + stn_id, float, 0, 1000),
        RangeElem('면적(m^2)', 'area', float, 0, 100),
        RangeElem('사용승인연도', 'year', int, 1960, 2023),
        RangeElem('층번호', 'floor', int, 1, 30),
    ]
    st.write('### 희망 보증금, 월세 범위')
    search_filter = [
        RangeElem('보증금', 'deposit', int, 0, 10000),
        RangeElem('월세', 'monthly_rt', int, 0, 200)
    ]
    # for f in db_filter + search_filter:
#         while True:
#             try:
#                 inp = input(f'{f.name} 입력: ')
#                 f.parse_input(inp)
#                 break
#             except Exception:
#                 print('다시 입력하세요.')
    return stn_name, db_filter, search_filter

def print_filtered_result(sql, stn_name, db_filter, search_filter):
    # st.write('# 입력 필터')
    # for ft in db_filter + search_filter:
    #     st.write(ft.to_sql_clause())

    deposit, monthly_rt = search_filter

    # DB의 모든 data 중 필터에 맞는 것 필터링 (SQLite)
    sql.execute(f"""SELECT addr, name, {db_filter[0].sql_name}, area, year, floor FROM filter_view
        WHERE {' AND '.join(f.to_sql_clause() for f in db_filter)}""")
    res = sql.fetchall()

    st.write('## 검색 결과')
    result = pd.DataFrame(columns=['주소', '건물명', '역과의 거리', '면적(m^2)', '사용승인연도', '층번호', '예상 월세'])
    for elem in res:
        dep_bt, dep_tp = deposit.get_st_elem()
        exp_min_rt = get_exp_monthly_rt(stn_name, elem[2], elem[3], elem[4], elem[5], deposit=dep_tp)
        exp_max_rt = get_exp_monthly_rt(stn_name, elem[2], elem[3], elem[4], elem[5], deposit=dep_bt)
        # 사용자가 설정한 월세 필터 범위와 예측 월세 범위에 겹치는 부분이 있는지 확인
        if check_interval_overlap((exp_min_rt, exp_max_rt), monthly_rt.get_st_elem()):
            result.loc[len(result)] = list(elem) + [f'{exp_min_rt:.0f}~{exp_max_rt:.0f}']
    st.write(result)
    
        ################## 여기서부터 수정..코드 전체를 이해하지 못해서 주석처리나 ''' str ''' 형태는 수정이 필요합니다.
        ################## 그리고 지도가 어떻게 나올지 환경이 너무 달라서 확인을 못했습니다..

    return result



import folium
from streamlit_folium import st_folium, folium_static

def map_visualize(result, stn_name, max_distance):    #max_distance는 필터링에서 설정한 역과의 최대 거리
    map = folium.Map(location=['''stn.lat, stn.lon'''], zoom_start=14)     #역의 위도 경도 가져오기, zoom_start 크기로 지도의 시작 줌인 정도를 정할 수 있음.
    for idx, row in result.iterrows():

        # lat_ = row['lat']         #필터링 데이터의 위도 경도 가져오기
        # lon_ = row['lon']

         folium.Marker(location=['''lat_, lon_'''],
                      radius=15,
                      popup='''마커를 클릭했을 때 나오기 원하는 데이터를 str로''').add_to(map)   #필터링 데이터를 지도 위에 표시하기

    folium.Circle(radius='''max_distance''',        #여기서 max_distance의 단위는 m입니다.
                  location=['''stn.lat, stn.lon'''],
                  color="#ff7800",
                  fill_color='#ffff00',
                  fill_opacity=0.2
                  ).add_to(map)      #max_distance를 바탕으로 필터링된 반경을 시각화
 
    folium_static(map, width=700)
