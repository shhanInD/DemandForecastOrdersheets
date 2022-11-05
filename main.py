from gspread_dataframe import set_with_dataframe
from utils import GoogleSheet
from utils import get_gs_structure
import time
import re
import string

# 구글시트 내 필요한 시트의 url, id 가져오기
structure = get_gs_structure()

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
        return int(row), int(col)

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
        copy_torow_py = copy_torow_gs + 1
    else:
        copy_torow_py = None

    if copy_tocolumn_gs is None:
        copy_tocolumn_py = copy_tocolumn_gs
    elif copy_tocolumn_gs > 0:
        copy_tocolumn_py = copy_tocolumn_gs + 1
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

# AOQ_RAW(AOQ_by_seg) -> 주문량 예측 Promo(AOQ_by_seg)
# importrange(copy_ssname = "AOQ_RAW", copy_wsname= "AOQ_by_seg", paste_ssname="주문량 예측 Promo", paste_wsname="AOQ_by_seg(importrange)", rows_between=[2,14])
# time.sleep(1.5)
# importrange(copy_ssname = "AOQ_RAW", copy_wsname= "AOQ_firstRe", paste_ssname="주문량 예측 Promo", paste_wsname="AOQ_firstRe(importrange)", rows_between=[2,4])
# time.sleep(1.5)
importrange(copy_ssname="수요예측 주문량", copy_wsname="Onetime_N1N2_RR", paste_ssname="주문량 예측 Promo", paste_wsname="Promo Reference", range=["J3","J368"], paste="C10", transpose=True )
# AOQ_RAW(AOQ_firstRe) -> 주문량 예측 Promo(AOQ_firstRe)