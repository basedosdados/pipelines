"""Source URLs and constants for the DIME v4.0 onboarding.

Every file is published from the DIME landing page at
https://data.stanford.edu/dime under the ODC-BY 1.0 licence. The links
themselves resolve to Dropbox; ``dl=1`` makes them return the file rather
than the preview page.

Row counts come from the DIME v4.0 codebook (December 2024) and are used to
check the ingest rather than to drive it. They are counts of physical lines,
not of CSV records: the header is one line, and some fields contain newlines
inside quotes. The 1980 file, for instance, has 447,970 lines but 447,962
records, a gap of 7 embedded newlines plus the header. So the codebook figure
is an upper bound on the record count, not an equality to assert.
"""

LANDING_PAGE = "https://data.stanford.edu/dime"
CODEBOOK = "https://bit.ly/dimeV4codebook"
LICENSE = "ODC-BY 1.0"

CONTRIBUTION_URLS = {
    1980: "https://www.dropbox.com/scl/fi/fgk4kchmhr86e7k89fzzu/contribDB_1980.csv.gz?rlkey=9q1etiitvzzqwwomtsi7l4kfd&dl=1",
    1982: "https://www.dropbox.com/scl/fi/26dmphuq84b8zmhelkmiq/contribDB_1982.csv.gz?rlkey=cnslknt934m0yke0c2534e42d&dl=1",
    1984: "https://www.dropbox.com/scl/fi/zcmgqxdowr3gki1vc5lrc/contribDB_1984.csv.gz?rlkey=gga6sfzi43tigr26ungftgrxd&dl=1",
    1986: "https://www.dropbox.com/scl/fi/wp4k9zuzm6ty6afs1x1oh/contribDB_1986.csv.gz?rlkey=tvgt92s1emtw7ok4i78l7jcxf&dl=1",
    1988: "https://www.dropbox.com/scl/fi/oot3plqbdrrqierfx9mdz/contribDB_1988.csv.gz?rlkey=4lhyc989ss5w40i76stthmnzv&dl=1",
    1990: "https://www.dropbox.com/scl/fi/dgq2raktk8yuccs37epb2/contribDB_1990.csv.gz?rlkey=qn38uwkteldub8gw1zkowfoym&dl=1",
    1992: "https://www.dropbox.com/scl/fi/g8cmdl0glg2czam7orwrf/contribDB_1992.csv.gz?rlkey=9cw2e21ki1dhlqm78j5huq2pg&dl=1",
    1994: "https://www.dropbox.com/scl/fi/xhspr7kwlt7fnn8lrcasp/contribDB_1994.csv.gz?rlkey=ev222r36r2fb24ouhbvaufsj9&dl=1",
    1996: "https://www.dropbox.com/scl/fi/566kucvtzsojyarj5nqy3/contribDB_1996.csv.gz?rlkey=anhxz6y8ez309mngxv2r5fo2n&dl=1",
    1998: "https://www.dropbox.com/scl/fi/b3mav3barc0dwiry5d773/contribDB_1998.csv.gz?rlkey=nhwn0dqk2apumrsj6p3crz2em&dl=1",
    2000: "https://www.dropbox.com/scl/fi/98mnrmknv4cd5a7tjxa2p/contribDB_2000.csv.gz?rlkey=qntafhlgk9sq1lu20u3ners5z&dl=1",
    2002: "https://www.dropbox.com/scl/fi/7gnjh8vb32s1eq9hebcln/contribDB_2002.csv.gz?rlkey=ojw6jjycw6mjhwq7r5hslwd0u&dl=1",
    2004: "https://www.dropbox.com/scl/fi/746km8razol0c8u65l154/contribDB_2004.csv.gz?rlkey=r3c01s6r9w08ju20d53pre1by&dl=1",
    2006: "https://www.dropbox.com/scl/fi/rsdniza4ux83n9riqngxi/contribDB_2006.csv.gz?rlkey=rr0rua9e9xd6wfqz09z8jmq5v&dl=1",
    2008: "https://www.dropbox.com/scl/fi/xqofzs1jshzzdksm7uror/contribDB_2008.csv.gz?rlkey=lrvar7w01ngeowjrjrlz2uoqw&dl=1",
    2010: "https://www.dropbox.com/scl/fi/lyembrg3vmj3lzjzg3a62/contribDB_2010.csv.gz?rlkey=f4erj4h8fdq7pbqacb4spib3o&dl=1",
    2012: "https://www.dropbox.com/scl/fi/dx8tafolqtrgp2dbn4fg6/contribDB_2012.csv.gz?rlkey=sslqxjhubk9745pfb0shcq5k4&dl=1",
    2014: "https://www.dropbox.com/scl/fi/g0omy5h86mddmwcai43fk/contribDB_2014.csv.gz?rlkey=btee8x45og1vphwpvnfe9qttg&dl=1",
    2016: "https://www.dropbox.com/scl/fi/qg5vezrx876cmu7u9hehr/contribDB_2016.csv.gz?rlkey=dsl4htd0ovr8hyn7xwctel0a0&dl=1",
    2018: "https://www.dropbox.com/scl/fi/sk2fbjbrq7hgdqfnern2g/contribDB_2018.csv.gz?rlkey=qsk4o1wjc8p1bwozuuk4bwq01&dl=1",
    2020: "https://www.dropbox.com/scl/fi/rnmdp79g0ewbf9j68tz1s/contribDB_2020.csv.gz?rlkey=v3y2xuvnmqueaiwkllls81mul&dl=1",
    2022: "https://www.dropbox.com/scl/fi/odu6raws98gu1xdmx0ql3/contribDB_2022.csv.gz?rlkey=bvrmhaftpp2sa6tv3lu120v44&dl=1",
    2024: "https://www.dropbox.com/scl/fi/p3adbtd50033ilt5ir3n2/contribDB_2024.csv.gz?rlkey=gt8l9j6xoi6h07syr94f33oyv&dl=1",
}

RECIPIENT_URL = "https://www.dropbox.com/scl/fi/pauqwdprfq2wn5db9oa9b/dime_recipients_all_1979_2024.csv.gz?rlkey=c80m4bvdbr14gyrr469sksfgq&dl=1"
CONTRIBUTOR_URL = "https://www.dropbox.com/scl/fi/c5z45dm2g8u9ihfi7uce8/dime_contributors_1979_2024.csv.gz?rlkey=janwvetndyxe4t8tm2v5a6wbu&dl=1"

# Codebook row counts, header line included.
CODEBOOK_ROWS = {
    1980: 447970,
    1982: 314178,
    1984: 434646,
    1986: 475688,
    1988: 654047,
    1990: 1031190,
    1992: 1507890,
    1994: 1790543,
    1996: 3168963,
    1998: 5695396,
    2000: 7138968,
    2002: 11253509,
    2004: 16865847,
    2006: 21019196,
    2008: 24951708,
    2010: 24621247,
    2012: 37695364,
    2014: 33467487,
    2016: 52704059,
    2018: 45450271,
    2020: 224227056,
    2022: 143540873,
    2024: 202886167,
}

# Figures the codebook states in prose, for sanity-checking the tables that have
# no per-file row count. Section 1 of the v4.0 codebook: "ideal point estimates
# for 173,171 candidates and 42,702 political committees as recipients and 41.5
# million individuals and 3.3 million organizations as donors."
#
# These describe rows *included in the CFscore scaling*, so they are lower
# bounds on the tables here: `recipient` is built from dime_recipients_all,
# which also carries recipients excluded from the scaling, and it is
# recipient-by-cycle rather than one row per recipient. Treat a large shortfall
# as a signal, not an equality to assert.
CODEBOOK_SCALED_CANDIDATES = 173_171
CODEBOOK_SCALED_COMMITTEES = 42_702
CODEBOOK_INDIVIDUAL_DONORS = 41_500_000
CODEBOOK_ORGANIZATION_DONORS = 3_300_000

# Cycles whose published file holds MORE records than the codebook lists.
#
# The v4.0 codebook is dated 29 December 2024 and its file listing was evidently
# written before the last rebuild of the two most recent cycles: the November
# 2024 election kept generating FEC filings after publication, which is why 2024
# diverges eight times more than 2022.
#
# 2022 is confirmed, not inferred. The file was re-downloaded (13,321,011,803
# bytes, the same size as the original fetch) and counted two independent ways:
#
#   physical lines                     144,246,131
#   minus the header and 220 newlines
#     inside quoted fields                    -221
#   csv records (python csv module)    144,245,910   == what DuckDB loaded
#
# So the file really does carry 144,246,131 lines where the codebook claims
# 143,540,873. Both cycles also show perfect transaction_id uniqueness with no
# blank ids and no wrong-cycle rows, the opposite of what a record-splitting bug
# would leave behind.
VERIFIED_ABOVE_CODEBOOK = {
    2022: 144_245_910,
    2024: 210_900_861,
}
