from gspread_dataframe import set_with_dataframe
from utils import GoogleSheet
from utils import get_gs_structure
import time

# 구글시트 내 필요한 시트의 url, id 가져오기
structure = get_gs_structure()

def importrange(copy_ssname, copy_wsname, paste_ssname, paste_wsname, rows_between = [None,None], cols_between = [None,None],
                 paste = [1,1], structure = structure):
    default_gsurl = 'https://docs.google.com/spreadsheets/d/'
    gs = GoogleSheet()

    from_data = gs.open_sheet(default_gsurl + structure[copy_ssname]["id"], copy_wsname)
    to_worksheet = gs.auth.open_by_url(default_gsurl + structure[paste_ssname]["id"]).get_worksheet_by_id(
        structure[paste_ssname]["worksheets"][paste_wsname])

    copy_fromrow_gs, copy_torow_gs = rows_between
    copy_fromcolumn_gs, copy_tocolumn_gs = cols_between
    paste_fromrow_py, paste_fromcolumn_py = paste

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

    set_with_dataframe(to_worksheet, from_data.iloc[copy_fromrow_py:copy_torow_py, copy_fromcolumn_py:copy_tocolumn_py],
                       include_index=False, include_column_header=False, row=paste_fromrow_py, col=paste_fromcolumn_py)

# AOQ_RAW(AOQ_by_seg) -> 주문량 예측 Promo(AOQ_by_seg)
importrange(copy_ssname = "AOQ_RAW", copy_wsname= "AOQ_by_seg", paste_ssname="주문량 예측 Promo", paste_wsname="AOQ_by_seg(importrange)", rows_between=[2,14])
time.sleep(1.5)
importrange(copy_ssname = "AOQ_RAW", copy_wsname= "AOQ_firstRe", paste_ssname="주문량 예측 Promo", paste_wsname="AOQ_firstRe(importrange)", rows_between=[2,4])
time.sleep(1.5)
# AOQ_RAW(AOQ_firstRe) -> 주문량 예측 Promo(AOQ_firstRe)