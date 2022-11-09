from gspread_dataframe import set_with_dataframe
from utils import GoogleSheet
from utils import get_gs_structure
import time
import re
import string
from datetime import datetime

sec = "시작"
global sec

print("************************", datetime.now().date(), datetime.now().time(), "함수 실행************************")
start_ = time.time()
# 구글시트 내 필요한 시트의 url, id 가져오기
structure = get_gs_structure()

print("준비 끝")
lap = time.time()-start_
print("준비까지 : ", lap, "초")

def importrange(copy_ssname, copy_wsname, paste_ssname, paste_wsname, range,
                 paste = "A1", structure = structure,
                reverse_row = False, reverse_col = False,
                transpose = False):
    def col2num(col):
        num = 0
        for c in col:
            if c in string.ascii_letters:
                num = num * 26 + (ord(c.upper()) - ord('A')) + 1
        if num == 0:
            num = None
        else:
            pass
        return num

    def range_breakdown(range_list):
        rangefrom = range_list[0]
        rangeto = range_list[1]

        fromrow = re.sub(r'[^0-9]', '', rangefrom)
        fromcol = col2num(rangefrom.replace(fromrow, ""))
        if fromrow == "":
            fromrow = None
        else:
            fromrow = int(fromrow)

        torow = re.sub(r'[^0-9]', '', rangeto)
        tocol = col2num(rangeto.replace(torow, ""))
        if torow == "":
            torow = None
        else:
            torow = int(torow)

        return fromrow, fromcol, torow, tocol
    def celladdress_breakdown(celladdress):
        row = re.sub(r'[^0-9]', '', celladdress)
        col = col2num(celladdress.replace(row,""))
        if row == "": row = None
        else : row = int(row)
        if col == "": col = None
        else : col = int(col)

        return row, col

    default_gsurl = 'https://docs.google.com/spreadsheets/d/'
    gs = GoogleSheet()

    from_data = gs.open_sheet(default_gsurl + structure[copy_ssname]["id"], copy_wsname)
    to_worksheet = gs.auth.open_by_url(default_gsurl + structure[paste_ssname]["id"]).get_worksheet_by_id(
        structure[paste_ssname]["worksheets"][paste_wsname])


    copy_fromrow_gs, copy_fromcolumn_gs, copy_torow_gs, copy_tocolumn_gs = range_breakdown(range)
    paste_fromrow_py, paste_fromcolumn_py = celladdress_breakdown(paste)

    if copy_fromrow_gs is None:
        copy_fromrow_py = copy_fromrow_gs
    elif copy_fromrow_gs > 0:
        copy_fromrow_py = copy_fromrow_gs - 1
    else:
        copy_fromrow_py = None

    if copy_fromcolumn_gs is None:
        copy_fromcolumn_py = copy_fromcolumn_gs
    elif copy_fromcolumn_gs > 0:
        copy_fromcolumn_py = copy_fromcolumn_gs - 1
    else:
        copy_fromcolumn_py = None

    if copy_torow_gs is None:
        copy_torow_py = copy_torow_gs
    elif copy_torow_gs > 0:
        copy_torow_py = copy_torow_gs
    else:
        copy_torow_py = None

    if copy_tocolumn_gs is None:
        copy_tocolumn_py = copy_tocolumn_gs
    elif copy_tocolumn_gs > 0:
        copy_tocolumn_py = copy_tocolumn_gs
    else:
        copy_tocolumn_py = None

    pre_data = from_data.iloc[copy_fromrow_py:copy_torow_py, copy_fromcolumn_py:copy_tocolumn_py]

    if reverse_row:
        pre_data = pre_data.loc[::-1]
    else:
        pass

    if reverse_col:
        pre_data = pre_data.loc[:, ::-1]
    else:
        pass

    if transpose:
        paste_data = pre_data.transpose()
    else:
        paste_data = pre_data

    set_with_dataframe(to_worksheet, paste_data,
                       include_index=False,
                       include_column_header=False,
                       row=paste_fromrow_py,
                       col=paste_fromcolumn_py)

def work():
    ## AOQ_RAW - > 주문량 예측 Promo
    start = time.time()
    importrange(copy_ssname = "AOQ_RAW", copy_wsname= "AOQ_firstRe", paste_ssname="주문량 예측 Promo", paste_wsname="AOQ_firstRe(importrange)", range=["2","4"], paste = "A1")
    lap_1 = time.time()-start
    sec = "AOQ_RAW - > 주문량 예측 Promo - AOQ_firstRe(importrange)"
    #print("AOQ_RAW - > 주문량 예측 Promo - AOQ_firstRe(importrange) : ", lap_1, "초")

    # start = time.time()
    # importrange(copy_ssname="수요예측 주문량", copy_wsname="Onetime_N1N2_RR", paste_ssname="주문량 예측 Promo", paste_wsname="Promo Reference", range=["J3","J368"], paste="C10", transpose=True )
    # lap = time.time()-start
    # print("복사 2 : ", lap, "초")

    ## 주석풀면됨
    ## '주문량 예측 시트'에 정보 넣기
    start = time.time()
    importrange(copy_ssname="주문량 예측 Promo", copy_wsname="Promo Plan (UA)", paste_ssname="주문량 예측", paste_wsname="신규유입자 계산", range=["A2","D7"], paste="A1")
    lap_2 = time.time()-start
    sec = "주문량 예측 Promo - > 주문량 예측 - 신규유입자 계산 _ 1"
    #print("주문량 예측 Promo - > 주문량 예측 - 신규유입자 계산 _ 1 : ", lap_2, "초")

    start = time.time()
    importrange(copy_ssname="주문량 예측 Promo", copy_wsname="Promo Reference", paste_ssname="주문량 예측", paste_wsname="신규유입자 계산", range=["B3","C3"], paste="H1", transpose = True)
    lap_3 = time.time()-start
    sec = "주문량 예측 Promo - > 주문량 예측 - 신규유입자 계산 _ 2"
    #print("주문량 예측 Promo - > 주문량 예측 - 신규유입자 계산 _ 2 : ", lap_3, "초")

    start = time.time()
    importrange(copy_ssname="주문량 예측 Promo", copy_wsname="Promo 첫전환 주문량 예측", range=["NJ2","ABK7"], paste_ssname="주문량 예측", paste_wsname="주문량 예측 시트", paste="B23")
    lap_4 = time.time()-start
    sec = "주문량 예측 Promo - > 주문량 예측 - 주문량 예측 시트 _ 1"
    #print("주문량 예측 Promo - > 주문량 예측 - 주문량 예측 시트 _ 1 : ", lap_4, "초")

    start = time.time()
    importrange(copy_ssname="주문량 예측 Promo", copy_wsname="Re_Promo_주문량_예측", range=["E2","NF2"], paste_ssname="주문량 예측", paste_wsname="주문량 예측 시트", paste="B36")
    lap_5 = time.time()-start
    sec = "주문량 예측 Promo - > 주문량 예측 - 주문량 예측 시트 _ 2"
    #print("주문량 예측 Promo - > 주문량 예측 - 주문량 예측 시트 _ 2 : ", lap_5, "초")

    start = time.time()
    ## 'SKU별 수요량 예측'시트에 정보 넣기
    importrange(copy_ssname="주문량 예측", copy_wsname="주문량 예측 시트", range=["A","NC"], paste_ssname="SKU별 수요량 예측", paste_wsname="주문량 예측", paste="A1")
    lap_6 = time.time()-start
    sec = "주문량 예측 Promo - > SKU별 수요량 예측 - 주문량 예측"
    #print("주문량 예측 Promo - > SKU별 수요량 예측 - 주문량 예측 : ", lap_6, "초")

    print("복사 끝")
    lap_fin = time.time()-start_
    #print("총 시간 : ", lap_fin, "초")

    return [lap_1, lap_2, lap_3, lap_4, lap_5, lap_6, lap_fin, sec]

try:
    worktime = work()
    print(worktime[-2], "초 걸려 작업 완료")
except Exception as e:
    print("==============", sec, "에서 문제가 생겼습니다.==============")
