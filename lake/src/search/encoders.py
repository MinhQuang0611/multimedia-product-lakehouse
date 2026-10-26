from pathlib import Path

import numpy as np
import torch
from PIL import Image

# Tên ngắn → repo Hugging Face. Đổi model là đổi không gian vector: embedding của hai model không bao giờ so với nhau.
MODELS = {
    "dinov2_b14": "facebook/dinov2-base",  # tự giám sát, đặc trưng thuần thị giác
    "siglip2_b16": "google/siglip2-base-patch16-224",  # ảnh–văn bản, đa ngôn ngữ
    "fashion_siglip": "Marqo/marqo-fashionSigLIP",  # SigLIP tinh chỉnh trên dữ liệu thời trang
    "clip_b32": "openai/clip-vit-base-patch32",  # mốc so sánh
}


def hf_revision(repo: str) -> str | None:
    # open_clip không ghi lại commit của trọng số: lấy tên thư mục snapshot (= commit) trong cache Hugging Face.
    try:
        from huggingface_hub import snapshot_download

        return Path(snapshot_download(repo, allow_patterns=["open_clip_config.json"])).name
    except Exception:
        return None


def l2(x: torch.Tensor) -> np.ndarray:
    x = x.float()
    return torch.nn.functional.normalize(x, dim=-1).cpu().numpy().astype(np.float32)


class Encoder:
    """Bọc một model: ảnh (và văn bản nếu có) → vector float32 đã L2-normalize."""

    def __init__(self, name: str, device: str | None = None):
        self.name, self.repo = name, MODELS[name]
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.dtype = torch.float16 if self.device == "cuda" else torch.float32
        self.open_clip = name == "fashion_siglip"
        if self.open_clip:
            import open_clip

            model, _, self.prep = open_clip.create_model_and_transforms(f"hf-hub:{self.repo}")
            self.tok = open_clip.get_tokenizer(f"hf-hub:{self.repo}")
            self.revision = hf_revision(self.repo)
        else:
            from transformers import AutoImageProcessor, AutoModel, AutoProcessor

            model = AutoModel.from_pretrained(self.repo)
            self.proc = (AutoImageProcessor if name.startswith("dinov2") else AutoProcessor).from_pretrained(self.repo)
            self.revision = getattr(model.config, "_commit_hash", None)
        self.model = model.to(self.device, self.dtype).eval()
        self.has_text = not name.startswith("dinov2")

    @property
    def version(self) -> str:
        return f"{self.name}:{self.repo}@{(self.revision or 'unknown')[:12]}"

    @torch.inference_mode()
    def images(self, ims: list[Image.Image]) -> np.ndarray:
        if self.open_clip:
            x = torch.stack([self.prep(im) for im in ims])
        else:
            x = self.proc(images=ims, return_tensors="pt")["pixel_values"]
        x = x.to(self.device, self.dtype)
        if self.open_clip:
            f = self.model.encode_image(x)
        elif self.name.startswith("dinov2"):
            f = self.model(pixel_values=x).pooler_output  # token CLS sau layernorm
        else:
            f = self.model.get_image_features(pixel_values=x)
        return l2(f)

    @torch.inference_mode()
    def texts(self, texts: list[str]) -> np.ndarray:
        if not self.has_text:
            raise ValueError(f"{self.name} không có nhánh văn bản")
        if self.open_clip:
            return l2(self.model.encode_text(self.tok(texts).to(self.device)))
        # SigLIP 2 học trên văn bản viết thường, pad cố định 64 token; processor không tự hạ chữ (tên viết hoa
        # như của Zara làm R@10 rơi từ 81% xuống 16%). CLIP tự hạ chữ trong tokenizer, pad theo câu dài nhất.
        if "siglip" in self.name:
            texts = [t.lower() for t in texts]
            kw = {"padding": "max_length", "max_length": 64}
        else:
            kw = {"padding": True}
        t = self.proc(text=texts, truncation=True, return_tensors="pt", **kw).to(self.device)
        return l2(self.model.get_text_features(**t))
