MARKET_INDECES = [
    '0#.SPX',    # S&P 500 works (no %)
    '0#.SPSUP',  # S&P 1500 works (no %)
    '0#.DJI',   # Dow Jones
    '0#.STOXX50',   # works (no %)
    '0#.FTSE',  # works (with %)
    '0#.HSLI',  # HK (Hang Seng Comp LargeCap)    works (no %)
    '0#.xu030', # BIST30 Index      works (with %)
    '0#.N225' #  works (no %) only 'DSPLY_NAME' works
    '0#.SPCOMP',    # also S&P 1500     doesn't work
    '0#.RUA',   # Russel 3000       doesn't work
    '0#.AORD',  # doesn't work
    '0#.AXJO',  # S&P/ASX200 doesn't work
    '.dMIWO000S0P'  # MSCI Small Cap doesn't work
]


MANUAL_ASSET_CODE_MAPPING = {
    'GM.N^F09':  ['4298546138', 
                  '4295903360' # General Motors finance 
                  ],
    'CSCO.OQ': [
        '4295905952',
        # '5080018615'  # 2022
    ],
    'T^K05': [
        '4295904853',
        '4295903035'  #inactive
    ],
}
