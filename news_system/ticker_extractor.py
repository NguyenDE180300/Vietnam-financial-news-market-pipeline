"""Deterministic ticker and company-alias extraction."""
from __future__ import annotations

import re


DEFAULT_ALIASES = {
    "ACB": ["ACB", "Ngân hàng Á Châu", "Asia Commercial Bank"],
    "BID": ["BID", "BIDV", "Ngân hàng Đầu tư và Phát triển Việt Nam"],
    "BSR": ["BSR", "Bình Sơn Refining", "Lọc hóa dầu Bình Sơn"],
    "CTG": ["CTG", "VietinBank", "Ngân hàng Công Thương Việt Nam"],
    "DIG": ["DIG", "DIC Corp", "DIC"],
    "DXG": ["DXG", "Đất Xanh", "Đất Xanh Group"],
    "FPT": ["FPT", "Tập đoàn FPT", "Công ty Cổ phần FPT"],
    "HAG": ["HAG", "Hoàng Anh Gia Lai", "HAGL"],
    "HPG": ["HPG", "Hòa Phát", "Tập đoàn Hòa Phát"],
    "MWG": ["MWG", "Thế Giới Di Động", "Mobile World"],
    "NLG": ["NLG", "Nam Long", "Nam Long Group"],
    "PVD": ["PVD", "PV Drilling", "Khoan Dầu khí"],
    "SSI": ["SSI", "Chứng khoán SSI", "SSI Securities"],
    "TCB": ["TCB", "Techcombank", "Ngân hàng Kỹ thương"],
    "VCB": ["VCB", "Vietcombank", "Ngân hàng Ngoại thương"],
    "VHM": ["VHM", "Vinhomes"],
    "VIC": ["VIC", "Vingroup", "Tập đoàn Vingroup"],
    "VNM": ["VNM", "Vinamilk", "Công ty Cổ phần Sữa Việt Nam"],
    # VN30 and frequently mentioned Vietnamese tickers.  The symbol itself
    # is always an alias; company-name aliases are added where unambiguous.
    "BCM": ["BCM", "Becamex IDC"],
    "BVH": ["BVH", "Bảo Việt", "Tập đoàn Bảo Việt"],
    "GAS": ["GAS", "PV Gas", "Tổng Công ty Khí Việt Nam"],
    "GVR": ["GVR", "Cao su Việt Nam", "Tập đoàn Công nghiệp Cao su Việt Nam"],
    "HDB": ["HDB", "HDBank", "Ngân hàng Phát triển TP.HCM"],
    "MBB": ["MBB", "MB Bank", "Ngân hàng Quân đội"],
    "MSN": ["MSN", "Masan", "Tập đoàn Masan"],
    "PLX": ["PLX", "Petrolimex", "Tập đoàn Xăng dầu Việt Nam"],
    "POW": ["POW", "PV Power", "Điện lực Dầu khí"],
    "SAB": ["SAB", "Sabeco", "Tổng Công ty Bia Rượu"],
    "SHB": ["SHB", "Ngân hàng Sài Gòn - Hà Nội"],
    "SSB": ["SSB", "SeABank", "Ngân hàng Đông Nam Á"],
    "STB": ["STB", "Sacombank", "Ngân hàng Sài Gòn Thương Tín"],
    "TPB": ["TPB", "TPBank", "Ngân hàng Tiên Phong"],
    "VIB": ["VIB", "Ngân hàng Quốc tế Việt Nam"],
    "VJC": ["VJC", "Vietjet", "Vietjet Air"],
    "VPB": ["VPB", "VPBank", "Ngân hàng Việt Nam Thịnh Vượng"],
    "VRE": ["VRE", "Vincom Retail"],
    # Common large-cap and news-heavy tickers outside VN30.
    "ANV": ["ANV", "Nam Việt"],
    "CEO": ["CEO", "CEO Group"],
    "CII": ["CII", "Đầu tư Hạ tầng Kỹ thuật TP.HCM"],
    "DBC": ["DBC", "Dabaco"],
    "DGC": ["DGC", "Hóa chất Đức Giang"],
    "DGW": ["DGW", "Digiworld"],
    "DPM": ["DPM", "Đạm Phú Mỹ"],
    "DPR": ["DPR", "Cao su Đồng Phú"],
    "HCM": ["HCM", "Chứng khoán HSC"],
    "KDH": ["KDH", "Khang Điền"],
    "KBC": ["KBC", "Kinh Bắc"],
    "MSB": ["MSB", "Ngân hàng Hàng hải"],
    "NT2": ["NT2", "Điện lực Dầu khí Nhơn Trạch 2"],
    "PC1": ["PC1", "PC1 Group"],
    "REE": ["REE", "Cơ điện lạnh REE"],
    "SBT": ["SBT", "Thành Thành Công - Biên Hòa"],
    "SZC": ["SZC", "Sonadezi Châu Đức"],
    "VCI": ["VCI", "Chứng khoán Vietcap"],
    "VND": ["VND", "Chứng khoán VNDirect"],
    "VOS": ["VOS", "Vận tải biển Việt Nam"],
    "VIX": ["VIX", "Chứng khoán VIX"],
    "VTP": ["VTP", "Viettel Post"],
    # Additional listed symbols. Keep aliases to symbols or distinctive company
    # names; short/common Vietnamese words are intentionally not used.
    "AAA": ["AAA", "An Phat Bioplastics"],
    "ACV": ["ACV", "Airports Corporation of Vietnam"],
    "BWE": ["BWE", "Biwase"],
    "CMG": ["CMG", "CMC Corporation"],
    "CSV": ["CSV", "Southern Basic Chemicals"],
    "CTR": ["CTR", "Viettel Construction"],
    "EIB": ["EIB", "Eximbank"],
    "FRT": ["FRT", "FPT Retail", "Long Châu"],
    "GEX": ["GEX", "Gelex"],
    "HNX": ["HNX"],
    "HVN": ["HVN", "Vietnam Airlines"],
    "KDC": ["KDC", "Kido Group"],
    "LPB": ["LPB", "LPBank", "LienVietPostBank"],
    "NAB": ["NAB", "Nam A Bank"],
    "OCB": ["OCB", "Orient Commercial Bank"],
    "PDR": ["PDR", "Phat Dat Real Estate"],
    "SHS": ["SHS", "Chứng khoán Sài Gòn - Hà Nội"],
    "VHC": ["VHC", "Vinh Hoan"],
    "VSC": ["VSC", "Vietnam Container Shipping"],
    "VSH": ["VSH", "Vinh Son - Song Hinh"],
}

# VN30 constituent snapshot used for dataset construction. The basket is
# reviewed periodically by HOSE, so keep this as a versioned config rather
# than silently assuming it is permanent.
VN30_TICKERS = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
    "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB",
    "TCB", "TPB", "VCB", "VIB", "VHM", "VIC", "VJC", "VNM", "VPB", "VRE",
]


class TickerExtractor:
    def __init__(self, aliases: dict[str, list[str]] | None = None):
        self.aliases = aliases or DEFAULT_ALIASES
        self.valid = set(self.aliases)
        self._alias_patterns = [
            (ticker, re.compile(r"(?<!\w)" + re.escape(alias) + r"(?!\w)", re.I))
            for ticker, names in self.aliases.items()
            for alias in names
        ]

    def extract(self, text: str) -> list[str]:
        found: dict[str, int] = {}
        # Prefixes such as HPG/VCB: are handled first.
        for match in re.finditer(r"(?<!\w)([A-Z]{2,4}(?:/[A-Z]{2,4})+):", text or ""):
            for ticker in match.group(1).split("/"):
                if ticker in self.valid:
                    found.setdefault(ticker, match.start())
        for ticker, pattern in self._alias_patterns:
            match = pattern.search(text or "")
            if match:
                found.setdefault(ticker, match.start())
        return [ticker for ticker, _ in sorted(found.items(), key=lambda item: item[1])]
