import sqlite3
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm
import sys
import re
import os
from urllib.parse import unquote
import requests

def is_annotation(text):
    t = text.strip()
    # 纯数字，如卷号
    if t.isdigit():
        return False
    # 单个字符
    if len(t) == 1:
        return False
    # doi
    if (re.match(r"^doi\s*:", t, re.I)
        or re.match(r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$", t, re.I)
        or t.lower().startswith("doi:")
        or re.match(r"^https?://(dx\.)?doi\.org/", t, re.I)):
        return False
    return True

def extract_annotation(main_text):
    """
    main_text: BeautifulSoup Tag, e.g. <p class="c-article-references__text">
    返回 (annotation字符串, 去除annotation之后的main text字符串)
    自动保留 in vivo、CD 4+ 这种结构
    """
    b_tags = list(main_text.find_all("b"))
    start_idx = None
    for idx, b in enumerate(b_tags):
        txt = b.get_text(" ", strip=True)
        if is_annotation(txt):
            start_idx = idx
            break
    if start_idx is None:
        return "", str(main_text)

    # 收集注释内容
    annotation_fragments = []
    cur = b_tags[start_idx]
    # 记录所有要删除的节点
    annotation_nodes = []
    while cur:
        if isinstance(cur, Tag):
            if cur.name == "b":
                annotation_fragments.append(cur.get_text(" ", strip=True))
                annotation_nodes.append(cur)
            elif cur.name in ("sup", "sub"):
                annotation_fragments.append(cur.get_text(" ", strip=True))
                annotation_nodes.append(cur)
        cur = cur.next_sibling

    annotation = " ".join(annotation_fragments).replace("  ", " ").strip()

    # 制作 main_text 的副本并删除注释内容
    main_text_copy = main_text.__copy__()
    for node in annotation_nodes:
        # 只删除当前main_text_copy下的节点
        tag_in_copy = main_text_copy.find(attrs={"id": node.get("id")}) if node.has_attr("id") else None
        if tag_in_copy:
            tag_in_copy.decompose()
        else:
            # fallback: 按内容查找
            for b in main_text_copy.find_all(node.name):
                if b.get_text(" ", strip=True) == node.get_text(" ", strip=True):
                    b.decompose()
    bib = main_text_copy.get_text(" ", strip=True)

    return annotation, bib

def extract_doi_from_google_scholar(ref):
    # 查找Google Scholar链接
    gs_link = ref.find("a", href=re.compile(r"^http://scholar\.google\.com/scholar_lookup\?"))
    if not gs_link or "href" not in gs_link.attrs:
        return None
    href = gs_link["href"]
    # 查找 &doi=...&，也兼容最后一个参数
    m = re.search(r"[?&]doi=([^&]+)", href)
    if not m:
        return None
    doi = m.group(1)
    # URL解码
    return unquote(doi)

def query_doi_from_crossref(bib):
    url = "https://api.crossref.org/works"
    params = {
        "rows": 1,
        "query.bibliographic": bib,
        "select": "DOI",
        "mailto": "zq.lyu@my.cityu.edu.hk",

    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        items = r.json().get("message", {}).get("items", [])
        if items:
            return items[0].get("DOI", "")
        return None
    except Exception as e:
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_annotations_all.py <db_num>")
        sys.exit(1)
    db_num = sys.argv[1]
    db_path = f'data/paper_html_10.1038/paper_html_10.1038_{db_num}.db'
    conn = sqlite3.connect(db_path)
    os.makedirs(f"data/paper_html_10.1038/doi_annotation/{db_num}", exist_ok=True)
    cursor = conn.cursor()

    # 获取所有DOI
    cursor.execute("SELECT doi FROM paper_html")
    doi_list = [row[0] for row in cursor.fetchall()]

    with tqdm(doi_list, desc="Processing DOIs in DB #" + db_num) as pbar:
        file_count = 0  # 文件计数
        pbar.set_postfix_str(f"files written: {file_count}")  # 主动设置初始状态
        for doi in pbar:
            cursor.execute("SELECT html FROM paper_html WHERE doi = ?", (doi,))
            result = cursor.fetchone()
            if not result:
                continue
            html_content = result[0]
            try:
                soup = BeautifulSoup(html_content, "html.parser")
            except Exception as e:
                print(f"[WARN] Failed to parse html for DOI: {doi}: {e}", file=sys.stderr)
                continue

            references_section = soup.find("ol", class_="c-article-references")
            if not references_section:
                continue
            references = references_section.find_all("li")
            extracted = []
            for ref in references:
                main_text = ref.find("p", class_="c-article-references__text")
                if main_text is not None:
                    annotation, bib = extract_annotation(main_text)
                    if len(annotation.split()) >= 10: # 要求annotation长度大于10字符
                        doi_extracted = extract_doi_from_google_scholar(ref) # 提取Google Scholar DOI
                        if doi_extracted:
                            extracted.append({"doi": doi_extracted, "annotation": annotation})
                        else: # 如果没有找到Google Scholar DOI，尝试从CrossRef查询
                            doi_crossrefed = query_doi_from_crossref(bib)
                            if doi_crossrefed:
                                extracted.append({"doi": doi_crossrefed, "annotation": annotation})

            if extracted:
                out_path = f"data/paper_html_10.1038/doi_annotation/{db_num}/{doi.split('/')[-1]}.tsv"
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write("doi\tannotation\n")
                    for item in extracted:
                        f.write(f"{item['doi']}\t{item['annotation']}\n")
                file_count += 1
                pbar.set_postfix_str(f"files written: {file_count}")

    conn.close()