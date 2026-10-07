from .cleaner import clean_text


RULES = {
    "CỔ TỨC": ("cổ tức", "chia cổ tức", "dividend"),
    "MUA BÁN/ PHÁT HÀNH": ("chào bán", "phát hành", "tăng vốn", "mua bán"),
    "CHUYỂN ĐỘNG THỊ TRƯỜNG": ("vn-index", "thị trường", "khối ngoại", "thanh khoản"),
    "CHỨNG KHOÁN/ TÀI CHÍNH": ("ngân hàng", "chứng khoán", "tài chính", "room tín dụng"),
    "VIỆT NAM": ("chính phủ", "thủ tướng", "kinh tế vĩ mô", "xuất khẩu"),
}


def categorize(title: str, summary: str = "") -> str:
    text = clean_text(f"{title} {summary}").lower()
    for category, keywords in RULES.items():
        if any(keyword in text for keyword in keywords):
            return category
    return "DOANH NGHIỆP"
