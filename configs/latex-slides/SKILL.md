---
name: latex-weekly-slides
description: Tạo và sửa slide báo cáo tuần bằng LaTeX Beamer theo template PTIT/RIPS (16:9, XeLaTeX), kèm script thuyết trình dạng bullet và bản văn nói. Dùng khi người dùng yêu cầu làm slide, sửa slide, dịch slide, viết script thuyết trình, hoặc đưa slide lên Overleaf.
---

# Slide báo cáo tuần bằng LaTeX (Beamer)

Skill này gom toàn bộ yêu cầu người dùng đã đưa ra khi làm slide (09/2026 – 10/2026). Phần **Quy tắc** là yêu
cầu đã nói rõ, bắt buộc. Phần **Template** là bộ preamble của deck cuối cùng người dùng đã chấp nhận, dùng lại
nguyên trạng.

Trả lời người dùng bằng **tiếng Việt**. Nội dung slide viết bằng **tiếng Anh** (trừ khi được yêu cầu bản dịch).

---

## 1. Quy tắc nội dung (quan trọng nhất)

### 1.1 Không suy diễn, không bịa
- Slide và script **chỉ** được chứa: định nghĩa, số liệu có trong file kết quả, và điều kiểm được trực tiếp trong code.
- **Không** tự thêm cơ chế giải thích ("vì X nên Y"), không thêm mũi tên kết luận ("→ nút thắt nằm ở…") nếu không có
  dữ liệu chống lưng. Nếu thật sự cần, gắn nhãn rõ **"Interpretation (to be tested)" / "giả thuyết"** và hỏi trước.
- **Không** đưa lên slide khái niệm, công thức hay đại lượng mà người dùng chưa duyệt hoặc không có nguồn
  (ví dụ: tự đặt một công thức Δ rồi trình bày như thể đã chốt). Mỗi khái niệm phải trả lời được câu "lấy ở đâu ra".
- **Không** trộn khái niệm của thiết kế cũ (đã bị bỏ) vào thiết kế hiện tại.
- Không có kết quả thì ghi là chưa có. Không bịa số, run, citation.
- Khi người dùng hỏi một khái niệm trên slide: trả lời đúng định nghĩa, không vẽ thêm cơ chế.
- Mọi con số trên slide phải đối chiếu được với file kết quả gốc; vẽ lại hình thì đọc từ đúng file summary cũ,
  **không chạy thêm thực nghiệm**.
- Kết quả từ bản chạy lại (adapted run) phải ghi rõ "adapted local run, not an official reproduction".

### 1.2 Trọng tâm của báo cáo tuần
- Báo cáo tuần **tập trung vào hướng nghiên cứu của mình** (ý tưởng, phương pháp, thiết kế thực nghiệm, kết quả).
  Phần "vấn đề / motivation" chỉ cần ngắn (khoảng 1 slide), không trình bày chi tiết.
- **Không** có slide "Next steps".
- **Không** có slide kể quá trình (timeline, "how the direction evolved", "design revisions", so sánh thiết kế cũ vs mới).
  Chỉ trình bày thiết kế **hiện tại**.
- Phần literature làm **gọn** (ưu tiên một bảng tổng hợp: Area / State of the art / Gap), không mỗi paper một slide.
- Không ghi dòng mô tả "phụ đề" thừa trên trang tiêu đề (ví dụ dòng liệt kê chủ đề dưới title — đã bị yêu cầu xóa).

### 1.3 Thuật ngữ
- Dùng **thuật ngữ tiếng Anh chuẩn** trong lĩnh vực (execution accuracy, schema linking, self-correction,
  natural direct/indirect effect, …). Không dịch thuật ngữ, không tự chế từ.

### 1.4 Hình vẽ
- Hình lấy từ chỗ khác hoặc hình cũ khó hiểu → **vẽ lại** cho dễ hiểu, nhãn tiếng Anh.
- **Không để hai hình trùng nội dung / trùng đóng góp** (cùng một kết quả nhìn từ hai phía, hoặc một số đã có ở hình
  khác). Mỗi kết quả chỉ xuất hiện ở **một** hình.
- Hình phải có thông tin; hình chỉ có 1–2 điểm dữ liệu thì thay bằng sơ đồ (TikZ) ghi số trực tiếp hoặc bỏ.
- Không đặt cạnh nhau hai tỉ lệ khác mẫu số khiến người xem so sánh sai.
- Khi người dùng nói một hình "không hay": đề xuất hình thay thế trả lời thẳng câu hỏi nghiên cứu của slide đó.
- Sơ đồ đơn giản (pipeline, DAG) vẽ bằng **TikZ** ngay trong `.tex`; biểu đồ số liệu vẽ bằng matplotlib trong
  `figures/gen_figures.py` (xem §3.4), xuất PDF.

---

## 2. Quy tắc trình bày

### 2.1 Bố cục slide
- Trang 1 là **title slide có tiêu đề thật** (không để trang trắng), không có footer.
- Trang 2 là **Table of Contents**.
- **Số trang phải đúng nội dung** (kiểm tra không bị lệch, ví dụ bắt đầu từ 2).
- Tiêu đề frame dạng **"N. Section – Subtitle"** (macro `\ftitle{…}`), theo file `example.pdf` của người dùng.
- Cuối deck có slide **References** theo chuẩn **IEEE** (đánh số [1], [2], … liên tục, theo thứ tự xuất hiện).

### 2.2 Những thứ KHÔNG được có
- **Không** hộp màu (tcolorbox block) đặt cạnh nhau kiểu hai cột "Consumer is a tool / Consumer is a human".
  Dùng gạch đầu dòng thường, đồng nhất cả deck.
- **Không** khung takeaway / kết luận màu ở cuối slide.
- **Không** dòng ghi đường dẫn file nguồn (`notes/...`, `results/...`) trên slide.
- **Không** chú thích nhỏ ở cuối slide, **trừ dòng citation**.
- Ý từ chú thích bị bỏ mà vẫn cần nói (caveat, sensitivity) → chuyển sang script để nói miệng.

### 2.3 Citation trên slide
- Slide nào cite paper thì **cuối slide đó** có một dòng chữ nhỏ màu xám liệt kê đúng các paper được cite:
  `[1] DIN-SQL -- Pourreza & Rafiei, NeurIPS 2023; [2] ...` (macro `\reflist{…}`).
- Trong thân slide gọi `\rf{1}` → `[1]` màu xám.
- Ưu tiên paper peer-reviewed; paper chỉ có arXiv phải ghi "chưa xác nhận venue"; không dùng nguồn người dùng đã loại.

### 2.4 Header / footer (template PTIT – RIPS)
- Header: trái là logo PTIT + "HỌC VIỆN CÔNG NGHỆ BƯU CHÍNH VIỄN THÔNG / Posts and Telecommunications Institute of
  Technology" (đỏ); phải là "VIỆN KHOA HỌC KỸ THUẬT BƯU ĐIỆN / Research Institute of Posts and Telecommunications"
  + logo RIPS (xanh). Font header/footer là **Arial** (không để lỗi font, không lệch).
- Footer: đường kẻ đỏ mảnh, trái "#MakeInPTIT" (đỏ, nghiêng đậm), phải ô đỏ chứa số trang.
- Khi người dùng đưa slide mẫu (ví dụ `example.pdf`) để tham khảo: **chỉ lấy format phần nội dung**, giữ nguyên
  header/footer của template này.

### 2.5 Font và kích thước (theo deck đã duyệt)
- Beamer 16:9, `14pt`, trang 22cm × 12.375cm.
- Font nội dung: **Times New Roman**; font header/footer: **Arial**; mono: DejaVu Sans Mono.
- Tiêu đề frame 18pt đậm; chữ thân 14pt; dòng citation 9pt xám; chữ trên hình matplotlib 14pt.
- Bảng lấn lề → thu nhỏ chữ riêng bảng đó; cột chữ bị giãn xấu → căn trái.
- Màu: đỏ PTIT `RGB(204,24,30)`, xanh RIPS `RGB(0,78,158)`; chữ `RGB(20,20,20)`; xám `RGB(85,85,85)`;
  màu dữ liệu `#1F5FA8` (xanh), `#2E8B57` (lục), `#D55E00` (cam), `#7A7A7A` (xám).
- Bullet: tam giác đỏ `▸`; enumerate: số đỏ đậm. Nhấn số liệu bằng `\hl{…}` (đỏ đậm), dùng tiết chế.

---

## 3. Quy trình làm việc

### 3.1 Cấu trúc thư mục
```
slides/
  YYYY-MM-DD_<topic>/          # một deck = một thư mục, ngày = ngày báo cáo
    main.tex                    # bản tiếng Anh (bản trình bày)
    main_vi.tex                 # bản tiếng Việt nếu được yêu cầu
    figures/                    # hình + gen_figures.py
    logos/ptit.png, logos/rips.png
    script.md                   # script bullet
    script_van_noi.md           # script văn nói
    .gitignore                  # *.aux *.log *.nav *.out *.snm *.toc *.xdv *.fls *.fdb_latexmk
```
- Tuần mới: copy deck gần nhất làm khung, giữ nguyên preamble.
- Trước khi sửa lớn, lưu bản cũ (scratchpad) để khôi phục khi người dùng muốn.

### 3.2 Build
- `latexmk -xelatex main.tex` (bắt buộc XeLaTeX vì fontspec + tiếng Việt ở header).
- **Luôn build lại PDF sau mỗi lần sửa** — người dùng mở PDF để kiểm, không chỉ xem `.tex`.
- Sau build: kiểm log không có `Overfull`/tràn khỏi footer, không thiếu glyph tiếng Việt, số trang đúng.
  Xem lại từng trang (render ảnh) để phát hiện nhãn đè mũi tên, nhãn trục bị cắt, chữ sát footer.

### 3.3 Overleaf
- Upload: `main.tex` + thư mục `figures/` + thư mục `logos/` (đóng gói thành `overleaf_upload.zip`), chọn compiler
  **XeLaTeX**.
- Nếu hình nằm ở thư mục phân tích ngoài deck: dùng script `sync_figures.sh` copy vào `figures/`
  (không trỏ đường dẫn ra ngoài deck, vì Overleaf không thấy).
- Overleaf có thể không có Times New Roman/Arial; nếu lỗi font thì thay bằng `TeX Gyre Termes` / `TeX Gyre Heros`.

### 3.4 Vẽ hình (matplotlib)
```python
RED, BLUE, GREEN, ORANGE, GRAY = "#CC181E", "#1F5FA8", "#2E8B57", "#D55E00", "#7A7A7A"
INK, MUTED = "#141414", "#555555"
W = 20.2 / 2.54  # bề rộng nội dung slide (inch) -> chữ 14pt giữ đúng 14pt khi \includegraphics[width=\linewidth]
plt.rcParams.update({
    "font.family": "serif", "font.serif": ["Times New Roman"], "mathtext.fontset": "stix",
    "font.size": 14, "axes.labelsize": 14, "axes.titlesize": 14, "axes.titleweight": "bold",
    "xtick.labelsize": 14, "ytick.labelsize": 14,
    "axes.spines.top": False, "axes.spines.right": False, "axes.edgecolor": "#9A9A9A",
    "text.color": INK, "axes.labelcolor": INK, "xtick.color": MUTED, "ytick.color": INK,
    "pdf.fonttype": 42, "ps.fonttype": 42, "savefig.dpi": 300,
})
```
- Docstring đầu file ghi rõ nguồn dữ liệu và "no model/API calls".
- Ghi CI (khoảng tin cậy) lên hình khi có.

### 3.5 Bản dịch tiếng Việt
- Khi được yêu cầu dịch: tạo **file riêng** `main_vi.tex` / `main_vi.pdf`, giữ `main.tex` tiếng Anh nguyên vẹn.
- Dịch tiêu đề, mục lục, chữ, bảng. **Giữ tiếng Anh**: hình/sơ đồ, thuật ngữ kỹ thuật, dòng citation, References.
- Sửa bản tiếng Anh thì phải nhắc người dùng là bản tiếng Việt không tự cập nhật.

---

## 4. Script thuyết trình

Khi người dùng xin script, làm **hai file**:

### 4.1 `script.md` — bullet chi tiết
- **Không viết thành đoạn văn**. Mỗi slide là các ý chính chi tiết dạng bullet, khoảng 3–4 ý (ít/nhiều tùy nội dung
  slide; slide nhiều nội dung có thể 5–7 ý, có ý con).
- Mỗi bullet có đủ số liệu + định nghĩa để nhìn vào là nói được; có CI và số đếm khi có.
- Đầu file: ký hiệu mức claim và setting chung của mọi số liệu. Ví dụ:
  ```
  # Script thuyết trình — deck DD/MM (N slide)

  Ký hiệu mức claim: **[A]** kết quả can thiệp, nói thẳng · **[B]** kết quả quan sát, nói "nhất quán với / đi kèm với".
  Mọi số: <pipeline> adapted, <dataset>, `<model>`, <số lần chạy>, CI 95% bootstrap theo case.

  ---

  ## Slide 3 — <Frame title> [A]
  - ...
  ```
- Các caveat đã bỏ khỏi slide (do bỏ chú thích nhỏ) → ghi thành bullet riêng để nói miệng.
- Ghi rõ chỗ **không được nói quá** (ví dụ không lấy tỉ số hai tỉ lệ khác mẫu số).
- Slide giả thuyết ghi rõ "đây là giả thuyết, chưa kiểm chứng".
- Cuối file: mục `## Kết` gồm vài bullet chốt, mỗi bullet gắn mức claim.
- Số slide trong script phải khớp deck hiện tại; sửa slide thì sửa script theo.

### 4.2 `script_van_noi.md` — văn nói
- Viết lại **từ đúng các bullet** trong `script.md`, **không thêm nội dung mới**.
- Văn nói, xưng "em", báo cáo với thầy; tự nhiên, không văn viết.
- Đầu file ghi: "Viết lại từ các bullet trong `script.md`, không thêm nội dung mới. Nếu sửa bullet thì sửa cả file này."

---

## 5. Checklist trước khi giao
- [ ] Trang 1 có tiêu đề, trang 2 là Table of Contents, số trang đúng.
- [ ] Header/footer đúng font Arial, không lệch, logo hiển thị.
- [ ] Không có: hộp màu cạnh nhau, takeaway box, đường dẫn file, chú thích nhỏ (trừ citation), slide next step,
      slide quá trình, thiết kế cũ.
- [ ] Mỗi slide cite paper có dòng `\reflist` cuối slide; slide References IEEE cuối deck.
- [ ] Mọi số đối chiếu được với file kết quả; không có câu suy diễn không nhãn "giả thuyết".
- [ ] Không có hai hình trùng nội dung.
- [ ] Thuật ngữ tiếng Anh chuẩn.
- [ ] Build XeLaTeX sạch, không tràn, đã xem lại từng trang.
- [ ] Script (nếu có) khớp số slide và nội dung deck.
- [ ] Báo lại cho người dùng: đã đổi gì, đã bỏ gì, ý nào bị mất theo phần đã bỏ, chỗ nào cần họ quyết.

---

## 6. Template preamble (dán nguyên)

Logo đặt tại `logos/ptit.png`, `logos/rips.png`; thiếu file thì tự vẽ chấm tròn màu thay thế.

```latex
\documentclass[aspectratio=169, 14pt]{beamer}
\usepackage{fontspec}
\setmainfont{Times New Roman}
\setsansfont{Arial}
\setmonofont{DejaVu Sans Mono}[Scale=MatchLowercase]
\newfontfamily\chromefont{Arial}
\renewcommand{\normalsize}{\fontsize{14}{17}\selectfont}
\geometry{paperwidth=22cm,paperheight=12.375cm}
\usepackage[english]{babel}
\usepackage{graphicx, booktabs, tikz, xcolor, tcolorbox, amsmath, amssymb, array}
\usetikzlibrary{calc, positioning, arrows.meta, fit}
\tcbuselibrary{skins}

\definecolor{ptitred}{RGB}{204,24,30}
\definecolor{ripsblue}{RGB}{0,78,158}
\definecolor{ink}{RGB}{20,20,20}
\definecolor{muted}{RGB}{85,85,85}
\definecolor{cblue}{HTML}{1F5FA8}
\definecolor{cgreen}{HTML}{2E8B57}
\definecolor{corange}{HTML}{D55E00}
\definecolor{cgray}{HTML}{7A7A7A}
\graphicspath{{figures/}}

\usetheme{default}
\usefonttheme{serif}
\setbeamertemplate{navigation symbols}{}
\setbeamercolor{normal text}{fg=ink}
\setbeamercolor{structure}{fg=ptitred}
\setbeamertemplate{itemize item}{\raisebox{0.12ex}{\color{ptitred}\scriptsize$\blacktriangleright$}}
\setbeamertemplate{itemize subitem}{\raisebox{0.12ex}{\color{ptitred}\tiny$\blacktriangleright$}}
\setbeamertemplate{enumerate item}{\color{ptitred}\bfseries\insertenumlabel.}
\setbeamerfont{frametitle}{size=\fontsize{18}{21}, series=\bfseries}
\setbeamercolor{frametitle}{fg=ink}
\setbeamersize{text margin left=0.9cm, text margin right=0.9cm}

% ---------------------------------------------------------------- header / footer
\newcommand{\logotext}[3]{\chromefont\begin{tabular}{@{}l@{}}{\fontsize{7.4}{8.6}\selectfont\bfseries\color{#1} #2}\\[0.5pt]{\fontsize{6.8}{8}\selectfont\color{ink} #3}\end{tabular}}
\newcommand{\logoimg}[2]{\IfFileExists{logos/#1}{\includegraphics[height=0.68cm]{logos/#1}}{\tikz\fill[#2] (0,0) circle (0.32cm);}}
\newcommand{\ptitheader}{\begin{tikzpicture}[remember picture, overlay]\coordinate (L) at ($(current page.north west)+(0.3cm,-0.62cm)$);\coordinate (R) at ($(current page.north east)+(-0.3cm,-0.62cm)$);\node[anchor=west, inner sep=0pt] (ll) at (L) {\logoimg{ptit.png}{ptitred}};\node[anchor=west, inner sep=0pt] at ($(ll.east)+(0.12cm,0)$) {\logotext{ptitred}{HỌC VIỆN CÔNG NGHỆ BƯU CHÍNH VIỄN THÔNG}{Posts and Telecommunications Institute of Technology}};\node[anchor=east, inner sep=0pt] (rt) at (R) {\logotext{ripsblue}{VIỆN KHOA HỌC KỸ THUẬT BƯU ĐIỆN}{Research Institute of Posts and Telecommunications}};\node[anchor=east, inner sep=0pt] at ($(rt.west)+(-0.12cm,0)$) {\logoimg{rips.png}{ripsblue}};\end{tikzpicture}}
\setbeamertemplate{headline}{\ptitheader\vspace{1.1cm}}
\setbeamertemplate{footline}{\begin{tikzpicture}[remember picture, overlay]\draw[ptitred, line width=0.6pt] ($(current page.south west)+(0,0.55cm)$) -- ($(current page.south east)+(0,0.55cm)$);\node[anchor=south west, xshift=0.3cm, yshift=0.12cm] at (current page.south west) {\chromefont\itshape\bfseries\color{ptitred}\fontsize{10}{12}\selectfont \#MakeInPTIT};\node[anchor=south east, fill=ptitred, text=white, minimum width=0.55cm, minimum height=0.8cm, inner sep=0pt, xshift=-0.45cm] at (current page.south east) {\chromefont\fontsize{10}{12}\selectfont\insertframenumber};\end{tikzpicture}\vspace{0.65cm}}
\setbeamertemplate{frametitle}{\vspace{0.3cm}\hspace{0.1cm}\insertframetitle\par\vspace{0.1cm}}
\setbeamertemplate{section in toc}{\color{ink}\inserttocsectionnumber.~\inserttocsection\par\vspace{4pt}}

% ---------------------------------------------------------------- content styles
\newcommand{\ftitle}[1]{\insertsectionnumber.~\insertsection{} -- #1}   % "N. Section -- Subtitle"
\newcommand{\rf}[1]{{\color{muted}[#1]}}                                  % in-text cite
\newcommand{\reflist}[1]{\par\vfill{\fontsize{9}{10.8}\selectfont\color{muted}\raggedright #1\par}} % cite line, bottom of slide
\newcommand{\hl}[1]{\textbf{\color{ptitred}#1}}                          % highlight a number
\tikzset{
  stage/.style={draw=#1, fill=#1!8, line width=0.9pt, rounded corners=3pt, align=center,
                minimum height=1.45cm, text width=2.75cm, font=\normalsize},
  flow/.style={-{Latex[length=2.2mm]}, line width=0.9pt, ptitred},
  var/.style={circle, draw=#1, fill=#1!10, line width=1pt, minimum size=1.05cm, font=\large},
}
```

### Khung tài liệu

```latex
\begin{document}

% ---- Title (không footer)
{\setbeamertemplate{footline}{}
\begin{frame}[plain]
  \begin{tikzpicture}[remember picture, overlay]
    \fill[ptitred] (current page.south west) rectangle ($(current page.south east)+(0,2.6cm)$);
    \node[anchor=north west, text width=18cm, align=left, inner sep=0pt]
      at ($(current page.north west)+(1.0cm,-3.1cm)$)
      {{\fontsize{26}{31}\selectfont\bfseries\color{ink} <Title line 1>\\ <Title line 2>}};
    \node[anchor=west, text=white, font=\Large\bfseries] at ($(current page.south west)+(1.0cm,1.6cm)$) {<Presenter name>};
    \node[anchor=west, text=white, font=\normalsize] at ($(current page.south west)+(1.0cm,0.95cm)$) {Weekly report};
  \end{tikzpicture}
\end{frame}}

% ---- Table of Contents
\begin{frame}{Table of Contents}
  \vspace{0.6cm}
  {\fontsize{15}{19}\selectfont\hspace{0.6cm}\begin{minipage}{0.8\textwidth}\tableofcontents\end{minipage}}
\end{frame}

\section{<Section>}
\begin{frame}{\ftitle{<Subtitle>}}
  \vspace{0.3cm}
  \begin{itemize}\setlength{\itemsep}{6pt}
    \item ... \rf{1}
  \end{itemize}
  \reflist{[1] <Short name> -- <Authors>, <Venue Year>}
\end{frame}

% ---- Hình kết quả: full width, không caption/takeaway
\begin{frame}{\ftitle{<Result>}}
  \centering\includegraphics[width=\linewidth]{fig_xxx.pdf}\par
\end{frame}

% ---- References (IEEE)
\section*{References}
\begin{frame}{References}
  \vspace{0.2cm}
  {\fontsize{11}{13.5}\selectfont\raggedright
  [1] A. Author and B. Author, ``Title,'' Venue, Year.\par\vspace{3pt}
  }
\end{frame}

\end{document}
```
