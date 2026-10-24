import re

from src.normalize.adapters.common import fold

# Danh mục chuẩn 2 cấp áp lên mọi nguồn (tiếng Việt/Anh/Bồ Đào Nha/Indonesia). Quy tắc theo từ khoá, khớp đầu tiên thắng,
# nên thứ tự quan trọng (áo khoác trước áo, dép/giày trước các từ chung chung).
RULES: list[tuple[str, str, str]] = [
    ("tops", "t_shirts", r"\b(vest tops?|strap tops?|tank tops?)\b"),  # "vest top" của H&M là áo ba lỗ, không phải áo vest
    ("underwear_swim", "swimwear", r"\b(swim\w*|boi|bikini|do boi|beachwear|mayo)\b"),
    ("underwear_swim", "underwear", r"\b(do lot|underwear|boxer|briefs?|bras?|lingerie|lingeri\w*|panty|panties|thong|cueca|calcinha|sutia)\b"),
    ("sleepwear", "sleepwear", r"\b(do ngu|pyjamas?|pajamas?|sleepwear|nightwear|nightgown|loungewear|do mac nha|piyama|daster)\b"),
    ("footwear", "sneakers", r"\b(sneakers?|giay the thao|giay chay|running shoes?|trainers?|tenis)\b"),
    ("footwear", "boots", r"\b(boots?|bot|giay cao co|botas?)\b"),
    ("footwear", "sandals_slippers", r"\b(dep|sandals?|slippers?|flip ?flops?|clogs?|sandalias?|chinelos?)\b"),
    ("footwear", "formal_shoes", r"\b(giay tay|giay da|loafers?|oxford|derby|dress shoes?|heels?|pumps?|wedges?|sapatos?|sepatu)\b"),
    ("footwear", "other_footwear", r"\b(giay|shoes?|footwear|calcados?)\b"),
    ("bags", "backpacks", r"\b(balo|backpacks?|rucksack|mochilas?)\b"),
    ("bags", "wallets", r"\b(vi nam|vi da|wallets?|card holder|purse|carteiras?)\b"),
    ("bags", "handbags", r"\b(tui|handbags?|tote|clutch|crossbody|satchel|duffle|bags?|bolsas?|luggage|tas)\b"),
    ("accessories", "hats_caps", r"\b(mu|non|hats?|caps?|beanies?|bucket|fedora|bonnet)\b"),
    ("accessories", "belts", r"\b(that lung|day lung|belts?|cintos?)\b"),
    ("accessories", "eyewear", r"\b(kinh|glasses|sunglasses|eyewear|oculos)\b"),
    ("accessories", "scarves_gloves", r"\b(khan|scarf|scarves|gloves?|gang tay|shawl)\b"),
    ("accessories", "socks", r"\b(tat|vo|socks?|meias?|stockings?|tights?)\b"),
    ("accessories", "jewelry_watches", r"\b(jewel\w*|ring|necklace|bracelet|earrings?|dong ho|watch\w*|vong|nhan|day chuyen|joias?|relogios?)\b"),
    ("accessories", "ties", r"\b(ca vat|cravat|ties?|bow ?tie|no co|pocket square)\b"),
    ("accessories", "other_accessories", r"\b(phu kien|accessor\w*|acessorios?)\b"),
    ("dresses_skirts", "dresses", r"\b(dam|dress\w*|jumpsuit|romper|vestidos?|gamis|kebaya)\b"),
    ("dresses_skirts", "skirts", r"\b(vay|chan vay|skirts?|skort|saias?|rok)\b"),
    ("outerwear", "jackets", r"\b(ao khoac|khoac|jackets?|windbreaker|bomber|parka|puffer|anorak|varsity|fleece|jaquetas?|jaket|outerwear|ao gio|ao du|outer)\b"),
    ("outerwear", "coats_blazers", r"\b(blazer|vest|ao vest|coats?|trench|overcoat|gile|gilet|waistcoat|cardigan|casacos?)\b"),
    ("tops", "hoodies_sweaters", r"\b(hoodies?|sweatshirts?|sweaters?|jumper|ao len|ao ni|knit\w*|pullover|moletom|sueter)\b"),
    ("tops", "t_shirts", r"\b(t ?shirts?|tees?|ao thun|ao phong|tshirts?|camisetas?|kaos|singlet|tank|ba lo|ao ba lo|croptops?|crop top)\b"),
    ("tops", "shirts_blouses", r"\b(ao so mi|so mi|shirts?|blouses?|polo|camisas?|kemeja|ao kieu|tunic|henley)\b"),
    ("bottoms", "jeans", r"\b(jeans?|denim|quan bo|quan jean)\b"),
    ("bottoms", "shorts", r"\b(shorts?(?!\s*sleeve)|quan dui|quan short|bermudas?|trunks?)\b"),
    ("bottoms", "trousers", r"\b(trousers?|pants?|chinos?|joggers?|sweatpants|leggings?|quan|culottes?|cargo|calcas?|celana|kulot)\b"),
    ("activewear", "activewear", r"\b(sports?wear|activewear|gym|yoga|training|the thao|athletic|workout)\b"),
    ("sets", "sets", r"\b(set|bo quan ao|combo|co ?ord|matching|suit|bo vest|conjunto)\b"),
    ("tops", "other_tops", r"\b(ao|top|tops|camisa|baju|atasan)\b"),
    ("bottoms", "other_bottoms", r"\b(bottoms?|bawahan)\b"),
]
COMPILED = [(l1, l2, re.compile(p)) for l1, l2, p in RULES]
# Olist: chỉ vài danh mục là thời trang; còn lại (đồ gia dụng, điện tử…) là non_fashion.
OLIST_FASHION = re.compile(r"fashio|fashion|underwear|luggage|watches|calcados|bolsas|roupa")


def std_category(source: str, *texts) -> tuple[str, str]:
    """(category_l1, category_l2) từ các chuỗi theo thứ tự ưu tiên (đường dẫn danh mục trước, tên sản phẩm sau)."""
    if source == "olist":
        t = fold(" ".join(str(x) for x in texts if x))
        if not OLIST_FASHION.search(t):
            return "non_fashion", "non_fashion"
    for text in texts:
        if not text:
            continue
        # Đi từ cấp sâu nhất của đường dẫn lên: "T-Shirts, Sweats & Fleece > T-Shirts" phải ra áo thun, không phải áo khoác.
        for seg in reversed(str(text).split(" > ")):
            t = fold(seg).replace("_", " ").replace("-", " ")
            for l1, l2, rx in COMPILED:
                if rx.search(t):
                    return l1, l2
    return "other", "other"


def clean_brand(b: str | None) -> str | None:
    """Bỏ khẩu hiệu, tên miền, hậu tố cửa hàng: 'Gymshark | Be a visionary.' → 'Gymshark', 'YaMe.vn' → 'YaMe'."""
    if not b:
        return None
    s = re.split(r"\s+[|–—:]\s+|\s+-\s+", str(b).strip())[0]
    s = re.sub(r"\.(com\.vn|com|vn|co|net)\b", "", s, flags=re.I)
    s = re.sub(r"\b(official store|official|chinh hang|flagship store|store|shop)\b", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -|.,")
    return s or None
