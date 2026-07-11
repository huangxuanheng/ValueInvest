import akshare as ak
import inspect, os, sys

# 遍历 akshare 目录下所有 .py 文件，找到定义 hash_code 或 get_token_lg 的文件
ak_dir = os.path.dirname(ak.__file__)
print(f"akshare 目录: {ak_dir}")

hits = []
for root, dirs, files in os.walk(ak_dir):
    for fn in files:
        if not fn.endswith(".py"):
            continue
        fpath = os.path.join(root, fn)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                txt = f.read()
            if "hash_code" in txt or "get_token_lg" in txt:
                hits.append(fpath)
                # 打印一小段上下文
                if "hash_code" in txt:
                    idx = txt.find("hash_code")
                    snippet = txt[max(0, idx-80):idx+200].replace("\n", " ")
                    print(f"[hash_code 命中] {fpath} ...{snippet}...")
                if "get_token_lg" in txt:
                    idx = txt.find("def get_token_lg")
                    snippet = txt[max(0, idx-20):idx+400].replace("\n", " ")
                    print(f"[get_token_lg 命中] {fpath} ...{snippet}...")
        except Exception:
            pass
print(f"\n共命中 {len(hits)} 个文件")
for h in hits:
    print(" -", h)
