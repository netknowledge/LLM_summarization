import sqlite3
from bs4 import BeautifulSoup, Tag
import re
from urllib.parse import unquote
import requests
import time

def is_annotation(text):
    t = text.strip()
    if t.isdigit():
        return False
    if len(t) == 1:
        return False
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

db_name = 'paper_html_10.1038_4.db'
conn = sqlite3.connect('data/paper_html_10.1038/' + db_name)
cursor = conn.cursor()

doi = '10.1038/35036035'
# db4: 35036035
# db7: ni.1714
# db8: nrmicro2383
# db11: s41929-020-0428-y, s41598-022-11149-0, s41598-022-16615-3, s41598-022-09137-5, s41598-022-09849-8, 
cursor.execute("SELECT html FROM paper_html WHERE doi = ?", (doi,))
result = cursor.fetchone()

if result:
    html_content = result[0]
    print("找到对应的 DOI 记录，开始解析 HTML...")
    soup = BeautifulSoup(html_content, "html.parser")

    references_section = soup.find("ol", class_="c-article-references")

    if references_section:
        print("找到 References 部分，开始提取数据...")
        references = references_section.find_all("li")
        extracted = []
        for ref in references:
            main_text = ref.find("p", class_="c-article-references__text")
            if main_text is not None:
                annotation, bib = extract_annotation(main_text)
                if len(annotation.split()) >= 10:
                    print(f"提取到的 bib: {bib}")
                    print(f"提取到的 annotation: {annotation}")
                    doi_extracted = extract_doi_from_google_scholar(ref) # 提取Google Scholar DOI
                    if doi_extracted:
                        print("从Google Scholar提取DOI成功！")
                        extracted.append({"doi": doi_extracted, "annotation": annotation})
                    else: # 如果没有找到Google Scholar DOI，尝试从CrossRef查询
                        print(f"未找到Google Scholar DOI，尝试从CrossRef查询...")
                        doi_crossrefed = query_doi_from_crossref(bib)
                        time.sleep(1/50)  # 保证请求频率不超过50次/秒
                        if doi_crossrefed:
                            print("从CrossRef查询DOI成功！")
                            extracted.append({"doi": doi_crossrefed, "annotation": annotation})

        if len(extracted) != 0:
            out_path = f"data/paper_html_10.1038/doi_annotation/demo/{doi.split('/')[-1]}.tsv"
            with open(out_path, "w", encoding="utf-8") as f:
                f.write("doi\tannotation\n")
                for item in extracted:
                    f.write(f"{item['doi']}\t{item['annotation']}\n")
            print(f"已保存带annotation的reference条目到 {doi.split('/')[-1]}.tsv")
        else:
            print("没有找到同时带DOI和annotation的reference条目，结束。")
    else:
        print("未找到 References 部分，结束。")
else:
    print("未在DB中找到对应的 DOI 记录，结束")

conn.close()