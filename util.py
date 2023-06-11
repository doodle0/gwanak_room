import math

from sqlmanager import SQLManager
import requests
import pandas as pd
from geopy.distance import great_circle

class StnInfo:
    STATIONS = []

    def __init__(self, id, name, lat, lng, traffic):
        self.id = id
        self.sql_dist_name = 'dist_' + id
        self.name = name
        self.lat = lat
        self.lng = lng
        self.traffic = traffic

    def get_dist(self, point):
        return great_circle((self.lat, self.lng), point).meters
    
    def get_mean_tfc(self):
        return sum(self.traffic) / 2

    @classmethod
    def get_obj_by_name(cls, name):
        for stn in cls.STATIONS:
            if stn.name == name:
                return stn

    @classmethod
    def get_obj_by_id(cls, id_):
        for stn in cls.STATIONS:
            if stn.id == id_:
                return stn

for idx, row in pd.read_csv('subway.csv').iterrows():
    StnInfo.STATIONS.append(StnInfo(
        row.loc['stn_id'],
        row.loc['stn_name'],
        row.loc['latitude'],
        row.loc['longitude'],
        (row.loc['traffic_in'], row.loc['traffic_out'])
    ))


stn_query_vars = ','.join(stn.sql_dist_name for stn in StnInfo.STATIONS)
dataset_vars = \
    ['addr',
     'area',
     'year',
     'floor',
     'is_oneroom',
     'is_officetel',
     'elem_school',
     'has_elevator',
     'has_parking',
     'stn_id',
     'stn_dist',
     'stn_traffic',
     'stn_traffic_per_dist',
     'stn_traffic_per_dist^2',
     'deposit',
     'rate'
    ]


class KakaoAPIHelper:
    REST_API_KEY = '3d0c679fcd5a519ab27cadadfe03a4a3'

    @classmethod
    def get_latlng_by_address(cls, address):
        url = "https://dapi.kakao.com/v2/local/search/address.json"
        headers = {
            "Authorization": f"KakaoAK {cls.REST_API_KEY}"
        }
        params = {
            "query": address
        }

        response = requests.get(url, headers=headers, params=params)
        resp_json = response.json()
        if not resp_json['documents']:
            raise IndexError(f'Kakao API 요청 실패: {params=} {resp_json=}')
        if not resp_json['documents'][0]:
            raise Exception(f'해당 주소 검색 실패: {params=} {resp_json=}')
        first_result = resp_json['documents'][0]
        return float(first_result['y']), float(first_result['x'])


class CsvToDB:
    def __init__(self, filepath, sql: SQLManager):
        self.df = pd.read_csv(filepath)
        self.sql = sql
        self.map_db_to_csv_colname = {}

    def add_map(self, db_colname, csv_colname, typ):
        self.map_db_to_csv_colname[db_colname] = (csv_colname, typ)

    def rem_map(self, db_colname):
        del self.map_db_to_csv_colname[db_colname]

    def add_to_table_from_csv(self, tablename):
        for i in self.df.index:
            values = []
            for cn, typ in self.map_db_to_csv_colname.values():
                if typ == str:
                    values.append(f'"{self.df.loc[i][cn]}"')
                else:
                    values.append(str(typ(self.df.loc[i][cn])))
            print(values)
            self.sql.execute(f'insert into {tablename} values({", ".join(values)})')


def add_rooms():
    c2d = CsvToDB('../db-preproc-0612.csv', SQLManager('data.db'))

    # 결측치 제거
    c2d.df = c2d.df.dropna(subset=['사용승인일'])

    # 사용승인일에서 앞 4자리만 가져오기
    c2d.df['사용승인연도'] = c2d.df['사용승인일'].astype(str).str[:4]
    c2d.df['사용승인연도'] = pd.to_numeric(c2d.df['사용승인연도'], errors='coerce').astype(pd.Int64Dtype())

    c2d.add_map('addr', '대지위치', str)
    c2d.add_map('name', '건물명', str)
    c2d.add_map('floor', '층번호', int)
    c2d.add_map('area', '면적(m^2)', float)
    c2d.add_map('year', '사용승인연도', int)

    c2d.add_to_table_from_csv('rooms')
    c2d.sql.con.commit()

def add_buildings():
    c2d = CsvToDB('../db-preproc-0612.csv', SQLManager('data.db'))

    c2d.sql.execute('DELETE FROM buildings')
    # cr_tbl_locations = '''CREATE TABLE "locations" (
    #                         addr VARCHAR PRIMARY KEY, -- 지번주소
    #                         addr_rdnm VARCHAR, -- 도로명주소
    #                         latitude REAL,
    #                         longitude REAL,
    #                         elem_school INTEGER CHECK(elem_school IN (0, 1)), -- 근방 초등학교 유무
    #                         has_elevator INTEGER CHECK(has_elevator IN (0, 1)), -- 엘리베이터 유무
    #                         has_parking INTEGER CHECK(has_parking IN (0, 1)), -- 주차시설 유무\n''' + \
    #                     ',\n'.join(f'dist_{stn.id} REAL' for stn in StnInfo.STATIONS) + \
    #                     ')'
    # c2d.sql.execute('DROP TABLE locations')
    # print(cr_tbl_locations)
    # c2d.sql.execute(cr_tbl_locations)

    # 주차시설 유무
    c2d.df['주차시설유무'] = (c2d.df[['옥내기계식대수(대)',
                                '옥외기계식대수(대)',
                                '옥내자주식대수(대)',
                                '옥외자주식대수(대)']]).any(axis=1).astype(int)

    # 중복 제거
    c2d.df = c2d.df.drop_duplicates(subset=['대지위치'], keep='first')

    # 각 지하철역까지의 거리 구하기
    for stn in StnInfo.STATIONS:
        c2d.df[stn.sql_dist_name] = c2d.df[['Latitude', 'Longitude']].apply(lambda x: stn.get_dist(x), axis=1)

    # 가장 가까운 지하철역 구하기
    stn_dist_names = [stn.sql_dist_name for stn in StnInfo.STATIONS]
    c2d.df['near_stn'] = c2d.df[stn_dist_names].idxmin(axis=1).str.replace('dist_', '')
    print(c2d.df)

    c2d.add_map('addr', '대지위치', str)
    c2d.add_map('addr_rdnm', '도로명대지위치', str)
    c2d.add_map('latitude', 'Latitude', float)
    c2d.add_map('longitude', 'Longitude', float)
    c2d.add_map('elem_school', '초등학교 근방 여부', int)
    c2d.add_map('has_elevator', '엘리베이터 여부', int)
    c2d.add_map('has_parking', '주차시설유무', int)
    c2d.add_map('is_oneroom', '원룸여부', int)
    c2d.add_map('is_officetel', '오피스텔여부', int)
    for stn in StnInfo.STATIONS:
        c2d.add_map(stn.sql_dist_name, stn.sql_dist_name, float)
    c2d.add_map('near_stn', 'near_stn', str)

    c2d.add_to_table_from_csv('buildings')
    c2d.sql.con.commit()

def make_learning_data(out_filepath):
    c2d = CsvToDB('../2023-1-3rd-preproc.csv', SQLManager('data.db'))

    df, sql = c2d.df, c2d.sql
    df['addr'] = df['시군구'] + ' ' + df['번지'] + '번지'

    # 월세를 5단위로 반올림
    # df['월세(반올림)'] = 5 * (df['월세(만원)'] / 5).round(0).astype(int)

    found = pd.DataFrame(columns=dataset_vars)
    not_found = []
    for idx, row in df.iterrows():
        sql.execute(f'''SELECT {stn_query_vars}, near_stn,
                        is_oneroom, is_officetel, elem_school, has_elevator, has_parking
                        FROM buildings WHERE addr="{row['addr']}"''')
        res = sql.fetchall()
        if res:
            res = res[0]
            near_stn = StnInfo.get_obj_by_id(res[len(StnInfo.STATIONS)])
            near_stn_dist = res[StnInfo.STATIONS.index(near_stn)]
            found.loc[len(found)] = [row['addr'],
                                     row['전용면적(㎡)'],
                                     row['건축년도'],
                                     row['층'],
                                     res[-5],
                                     res[-4],
                                     res[-3],
                                     res[-2],
                                     res[-1],
                                     near_stn.id,
                                     near_stn_dist,
                                     near_stn.get_mean_tfc(),
                                     near_stn.get_mean_tfc() / near_stn_dist,
                                     near_stn.get_mean_tfc() / near_stn_dist ** 2,
                                     row['보증금(만원)'],
                                     row['월세(만원)']
                                    ]
        else:
            not_found.append(row['addr'])
    print(found, sep='\n')
    print(f'{len(found)=}, {len(not_found)=}')
    found.to_csv(out_filepath)

# add_buildings()
# make_learning_data('../out.csv')
