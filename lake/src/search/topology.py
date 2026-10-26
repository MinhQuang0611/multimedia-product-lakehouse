from src.common.core import SOURCES

# Phân mảnh theo nguồn (mỗi nguồn là một cửa hàng/hệ thống bán độc lập; một sản phẩm thuộc đúng một nguồn nên
# không có bản trùng giữa các node). Cổng và cách chia phải khớp docker-compose.search.yml ở thư mục gốc repo.
IMAGE_SOURCES = [s for s in SOURCES if s != "olist"]  # Olist không có ảnh
TOPOLOGIES: dict[str, dict[str, tuple[int, list[str] | None]]] = {
    "n1": {"node-all": (18711, None)},
    "n3": {"node-a": (18721, ["tiki", "brands"]), "node-b": (18722, ["zara", "uniqlo"]), "node-c": (18723, ["hm", "shopee"])},
    "n6": {f"node-{s}": (18730 + i, [s]) for i, s in enumerate(IMAGE_SOURCES, 1)},
}


for _name, _nodes in TOPOLOGIES.items():
    # Thêm nguồn mới vào SOURCES mà quên chia vào node thì kết quả thiếu nguồn đó trong im lặng (partial vẫn False).
    _cover = [s for _, src in _nodes.values() for s in (src or IMAGE_SOURCES)]
    assert sorted(_cover) == sorted(IMAGE_SOURCES), f"topology {_name} không chia đúng mỗi nguồn một lần: {_cover}"


def node_urls(topology: str, host: str = "127.0.0.1") -> dict[str, str]:
    return {name: f"http://{host}:{port}" for name, (port, _) in TOPOLOGIES[topology].items()}
