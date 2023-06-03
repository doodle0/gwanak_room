from sqlmanager import SQLManager
import requests
import pandas as pd
from geopy.distance import great_circle

class StnInfo:
    STATIONS = []

    def __init__(self, id, name, lat, lng):
        self.id = id
        self.name = name
        self.lat = lat
        self.lng = lng

    def get_dist(self, point):
        return great_circle((self.lat, self.lng), point).meters

    @classmethod
    def get_id_by_name(cls, name):
        for stn in cls.STATIONS:
            if stn.name == name:
                return stn.id

StnInfo.STATIONS = [
    StnInfo('227',  '낙성대역', 37.4763633, 126.965696),
    StnInfo('228', '서울대입구역', 37.4812428, 126.9516165),
    StnInfo('229', '봉천역', 37.4822895, 126.9428361),
    StnInfo('230', '신림역', 37.4843296, 126.9289318),
    StnInfo('S407', '당곡역', 37.4898620, 126.9277330),
    StnInfo('S409', '서원역', 37.4783067, 126.9330369),
    StnInfo('S410', '서울대벤처타운역', 37.4722275, 126.9341573),
    StnInfo('S411', '관악산역', 37.4687780, 126.9453106)
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
            self.sql.execute(f'insert into {tablename} values({" ,".join(values)})')


def add_rooms():
    c2d = CsvToDB('../elementary_contain.csv', SQLManager('data.db'))

    # 결측치 제거
    c2d.df = c2d.df.dropna(subset=['사용승인일'])

    # 사용승인일에서 앞 4자리만 가져오기
    c2d.df['사용승인연도'] = c2d.df['사용승인일'].astype(str).str[:4]
    c2d.df['사용승인연도'] = pd.to_numeric(c2d.df['사용승인연도'], errors='coerce').astype(pd.Int64Dtype())

    # 승강기 여부 확인
    c2d.df['승강기여부'] = c2d.df['승용승강기수'].clip(upper=1)

    c2d.add_map('addr', '대지위치', str)
    c2d.add_map('name', '건물명', str)
    c2d.add_map('floor', '층번호', int)
    c2d.add_map('is_oneroom', '원룸여부', int)
    c2d.add_map('is_officetel', '오피스텔여부', int)
    c2d.add_map('area', '면적(m^2)', float)
    c2d.add_map('year', '사용승인연도', int)
    c2d.add_map('has_elevator', '승강기여부', int)

    c2d.add_to_table_from_csv('rooms')
    c2d.sql.con.commit()

def add_locations():
    c2d = CsvToDB('../elementary_contain.csv', SQLManager('data.db'))

    # 중복 제거
    c2d.df = c2d.df.drop_duplicates(subset=['대지위치'], keep='first')

    # 각 지하철역까지의 거리 구하기
    for stn in StnInfo.STATIONS:
        c2d.df['dist_' + stn.id] = c2d.df[['Latitude', 'Longitude']].apply(lambda x: stn.get_dist(x), axis=1)

    c2d.add_map('addr', '대지위치', str)
    c2d.add_map('addr_rdnm', '도로명대지위치', str)
    c2d.add_map('latitude', 'Latitude', float)
    c2d.add_map('longitude', 'Longitude', float)
    c2d.add_map('elem_school', '초등학교 근방 여부', int)
    c2d.add_map('dist_227', 'dist_227', float)
    c2d.add_map('dist_228', 'dist_228', float)
    c2d.add_map('dist_229', 'dist_229', float)
    c2d.add_map('dist_230', 'dist_230', float)
    c2d.add_map('dist_S407', 'dist_S407', float)
    c2d.add_map('dist_S409', 'dist_S409', float)
    c2d.add_map('dist_S410', 'dist_S410', float)
    c2d.add_map('dist_S411', 'dist_S411', float)

    c2d.add_to_table_from_csv('locations')
    c2d.sql.con.commit()
