PLANE_ICON_URL = "https://i.postimg.cc/FKKdwDDn/aircraft.png"

DASHBOARD_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    background-color: #080d14;
    color: #c8d8e8;
    font-family: 'Barlow', sans-serif;
}

/* Header */
.sp-header {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 18px 0 10px 0;
    border-bottom: 1px solid #1a2a3a;
    margin-bottom: 20px;
}
.sp-logo {
    font-family: 'Share Tech Mono', monospace;
    font-size: 28px;
    color: #00e5ff;
    letter-spacing: 3px;
    text-shadow: 0 0 20px #00e5ff88;
}
.sp-tagline {
    font-size: 12px;
    color: #4a6a8a;
    letter-spacing: 2px;
    text-transform: uppercase;
    margin-top: 2px;
}
.sp-pulse {
    width: 10px; height: 10px;
    background: #00ff88;
    border-radius: 50%;
    box-shadow: 0 0 0 0 #00ff8866;
    animation: pulse 2s infinite;
    margin-left: auto;
    margin-right: 8px;
}
.sp-live {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    color: #00ff88;
    letter-spacing: 2px;
}
@keyframes pulse {
    0%   { box-shadow: 0 0 0 0 #00ff8866; }
    70%  { box-shadow: 0 0 0 10px #00ff8800; }
    100% { box-shadow: 0 0 0 0 #00ff8800; }
}

/* KPI cards */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 20px;
}
.kpi-card {
    background: #0d1825;
    border: 1px solid #1a2e42;
    border-radius: 8px;
    padding: 16px 20px;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
}
.kpi-card.flights::before  { background: linear-gradient(90deg, #00e5ff, #0077ff); }
.kpi-card.seismic::before  { background: linear-gradient(90deg, #ff6b35, #ff0055); }
.kpi-card.weather::before  { background: linear-gradient(90deg, #7b61ff, #00e5ff); }
.kpi-card.risk::before     { background: linear-gradient(90deg, #ff0055, #ff6b35); }

.kpi-label {
    font-size: 10px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #4a6a8a;
    margin-bottom: 8px;
}
.kpi-value {
    font-family: 'Share Tech Mono', monospace;
    font-size: 32px;
    font-weight: 700;
    line-height: 1;
}
.kpi-value.flights  { color: #00e5ff; text-shadow: 0 0 20px #00e5ff55; }
.kpi-value.seismic  { color: #ff6b35; text-shadow: 0 0 20px #ff6b3555; }
.kpi-value.weather  { color: #7b61ff; text-shadow: 0 0 20px #7b61ff55; }
.kpi-value.risk     { color: #ff0055; text-shadow: 0 0 20px #ff005555; }
.kpi-sub {
    font-size: 11px;
    color: #4a6a8a;
    margin-top: 4px;
}

/* Section labels */
.section-label {
    font-family: 'Share Tech Mono', monospace;
    font-size: 10px;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #4a6a8a;
    margin-bottom: 8px;
    border-left: 2px solid #1a2e42;
    padding-left: 10px;
}

/* Tab styling */
div[data-testid="stTabs"] button {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #4a6a8a !important;
}
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #00e5ff !important;
    border-bottom: 2px solid #00e5ff;
}

/* Toggle buttons */
div[data-testid="stCheckbox"] label {
    font-size: 12px !important;
    letter-spacing: 1px;
    color: #8aa0b8 !important;
}

/* Metric tweaks */
div[data-testid="stMetric"] {
    background: #0d1825;
    border: 1px solid #1a2e42;
    border-radius: 8px;
    padding: 12px 16px;
}

/* Table styling */
div[data-testid="stDataFrame"] {
    border: 1px solid #1a2e42;
    border-radius: 8px;
}

/* Scrollbar */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #080d14; }
::-webkit-scrollbar-thumb { background: #1a2e42; border-radius: 4px; }

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #0d1825;
    border-right: 1px solid #1a2e42;
}

/* Selectbox / slider */
div[data-testid="stSlider"] label { font-size: 11px; color: #4a6a8a; }

/* Timestamp */
.sp-ts {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    color: #2a4a6a;
    text-align: right;
    padding-top: 4px;
}

/* Analytics card */
.analytics-card {
    background: #0d1825;
    border: 1px solid #1a2e42;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 12px;
}

/* Mapa pydeck */
[data-testid="stPydeckChart"] {
    border-radius: 12px;
    overflow: hidden;
}
</style>
"""

COUNTRY_ISO3 = {
    "United States": "USA",
    "China": "CHN",
    "Germany": "DEU",
    "United Kingdom": "GBR",
    "France": "FRA",
    "Russia": "RUS",
    "India": "IND",
    "Brazil": "BRA",
    "Canada": "CAN",
    "Australia": "AUS",
    "Japan": "JPN",
    "Netherlands": "NLD",
    "Spain": "ESP",
    "Italy": "ITA",
    "Turkey": "TUR",
    "Mexico": "MEX",
    "South Korea": "KOR",
    "Indonesia": "IDN",
    "Argentina": "ARG",
    "Poland": "POL",
    "Ukraine": "UKR",
    "Norway": "NOR",
    "Sweden": "SWE",
    "Switzerland": "CHE",
    "Belgium": "BEL",
    "Austria": "AUT",
    "Portugal": "PRT",
    "Greece": "GRC",
    "Denmark": "DNK",
    "Finland": "FIN",
    "Romania": "ROU",
    "Czech Republic": "CZE",
    "Hungary": "HUN",
    "Thailand": "THA",
    "Malaysia": "MYS",
    "Singapore": "SGP",
    "United Arab Emirates": "ARE",
    "Saudi Arabia": "SAU",
    "Qatar": "QAT",
    "South Africa": "ZAF",
    "Egypt": "EGY",
    "Morocco": "MAR",
    "Nigeria": "NGA",
    "Kenya": "KEN",
    "Ethiopia": "ETH",
    "New Zealand": "NZL",
    "Pakistan": "PAK",
    "Bangladesh": "BGD",
    "Philippines": "PHL",
    "Vietnam": "VNM",
    "Colombia": "COL",
    "Chile": "CHL",
    "Peru": "PER",
    "Venezuela": "VEN",
    "Israel": "ISR",
    "Iran": "IRN",
    "Iraq": "IRQ",
    "Kazakhstan": "KAZ",
    "Uzbekistan": "UZB",
    "Bulgaria": "BGR",
    "Serbia": "SRB",
    "Croatia": "HRV",
    "Slovakia": "SVK",
    "Slovenia": "SVN",
    "Lithuania": "LTU",
    "Latvia": "LVA",
    "Estonia": "EST",
    "Belarus": "BLR",
    "Moldova": "MDA",
    "Albania": "ALB",
    "North Macedonia": "MKD",
    "Bosnia and Herzegovina": "BIH",
    "Montenegro": "MNE",
    "Iceland": "ISL",
    "Luxembourg": "LUX",
    "Malta": "MLT",
    "Cyprus": "CYP",
    "Ireland": "IRL",
    "Panama": "PAN",
    "Costa Rica": "CRI",
    "Guatemala": "GTM",
    "Ecuador": "ECU",
    "Bolivia": "BOL",
    "Paraguay": "PRY",
    "Uruguay": "URY",
    "Cuba": "CUB",
    "Dominican Republic": "DOM",
    "Jamaica": "JAM",
    "Honduras": "HND",
    "El Salvador": "SLV",
    "Nicaragua": "NIC",
    "Trinidad and Tobago": "TTO",
    "Oman": "OMN",
    "Kuwait": "KWT",
    "Bahrain": "BHR",
    "Jordan": "JOR",
    "Lebanon": "LBN",
    "Yemen": "YEM",
    "Syria": "SYR",
    "Afghanistan": "AFG",
    "Myanmar": "MMR",
    "Cambodia": "KHM",
    "Laos": "LAO",
    "Sri Lanka": "LKA",
    "Nepal": "NPL",
    "Mongolia": "MNG",
    "Ghana": "GHA",
    "Ivory Coast": "CIV",
    "Senegal": "SEN",
    "Tanzania": "TZA",
    "Uganda": "UGA",
    "Mozambique": "MOZ",
    "Zimbabwe": "ZWE",
    "Zambia": "ZMB",
    "Angola": "AGO",
    "Cameroon": "CMR",
    "Congo": "COG",
    "Tunisia": "TUN",
    "Algeria": "DZA",
    "Libya": "LBY",
    "Sudan": "SDN",
}

# ── ISO alpha-3 → ISO numeric (para lookup de Vega-Lite world-110m) ───────────
ISO3_TO_NUMERIC = {
    "USA": 840,
    "CHN": 156,
    "DEU": 276,
    "GBR": 826,
    "FRA": 250,
    "RUS": 643,
    "IND": 356,
    "BRA": 76,
    "CAN": 124,
    "AUS": 36,
    "JPN": 392,
    "NLD": 528,
    "ESP": 724,
    "ITA": 380,
    "TUR": 792,
    "MEX": 484,
    "KOR": 410,
    "IDN": 360,
    "ARG": 32,
    "POL": 616,
    "UKR": 804,
    "NOR": 578,
    "SWE": 752,
    "CHE": 756,
    "BEL": 56,
    "AUT": 40,
    "PRT": 620,
    "GRC": 300,
    "DNK": 208,
    "FIN": 246,
    "ROU": 642,
    "CZE": 203,
    "HUN": 348,
    "THA": 764,
    "MYS": 458,
    "SGP": 702,
    "ARE": 784,
    "SAU": 682,
    "QAT": 634,
    "ZAF": 710,
    "EGY": 818,
    "MAR": 504,
    "NGA": 566,
    "KEN": 404,
    "ETH": 231,
    "NZL": 554,
    "PAK": 586,
    "BGD": 50,
    "PHL": 608,
    "VNM": 704,
    "COL": 170,
    "CHL": 152,
    "PER": 604,
    "VEN": 862,
    "ISR": 376,
    "IRN": 364,
    "IRQ": 368,
    "KAZ": 398,
    "UZB": 860,
    "BGR": 100,
    "SRB": 688,
    "HRV": 191,
    "SVK": 703,
    "SVN": 705,
    "LTU": 440,
    "LVA": 428,
    "EST": 233,
    "BLR": 112,
    "MDA": 498,
    "ALB": 8,
    "MKD": 807,
    "BIH": 70,
    "MNE": 499,
    "ISL": 352,
    "LUX": 442,
    "MLT": 470,
    "CYP": 196,
    "IRL": 372,
    "PAN": 591,
    "CRI": 188,
    "GTM": 320,
    "ECU": 218,
    "BOL": 68,
    "PRY": 600,
    "URY": 858,
    "CUB": 192,
    "DOM": 214,
    "JAM": 388,
    "HND": 340,
    "SLV": 222,
    "NIC": 558,
    "TTO": 780,
    "OMN": 512,
    "KWT": 414,
    "BHR": 48,
    "JOR": 400,
    "LBN": 422,
    "YEM": 887,
    "SYR": 760,
    "AFG": 4,
    "MMR": 104,
    "KHM": 116,
    "LAO": 418,
    "LKA": 144,
    "NPL": 524,
    "MNG": 496,
    "GHA": 288,
    "CIV": 384,
    "SEN": 686,
    "TZA": 834,
    "UGA": 800,
    "MOZ": 508,
    "ZWE": 716,
    "ZMB": 894,
    "AGO": 24,
    "CMR": 120,
    "COG": 178,
    "TUN": 788,
    "DZA": 12,
    "LBY": 434,
    "SDN": 729,
}
