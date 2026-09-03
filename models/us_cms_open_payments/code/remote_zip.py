import struct
import subprocess
import sys
import zlib


def rng(url, start, end=None):
    r = f"{start}-" + ("" if end is None else str(end))
    return subprocess.run(
        ["curl", "-s", "-m", "300", "-r", r, url], capture_output=True
    ).stdout


def size(url):
    out = subprocess.run(
        ["curl", "-sI", "-m", "60", url], capture_output=True
    ).stdout.decode(errors="replace")
    for line in out.splitlines():
        if line.lower().startswith("content-length"):
            return int(line.split(":")[1].strip())
    raise SystemExit("no content-length")


def central_dir(url):
    n = size(url)
    tail = rng(url, max(0, n - 200000))
    # locate EOCD
    i = tail.rfind(b"PK\x05\x06")
    if i < 0:
        raise SystemExit("no EOCD")
    cd_size, cd_off = struct.unpack("<II", tail[i + 12 : i + 20])
    if cd_off == 0xFFFFFFFF:
        j = tail.rfind(b"PK\x06\x06")
        cd_size, cd_off = struct.unpack("<QQ", tail[j + 40 : j + 56])
    cd = rng(url, cd_off, cd_off + cd_size - 1)
    out = []
    p = 0
    while p < len(cd) and cd[p : p + 4] == b"PK\x01\x02":
        (method,) = struct.unpack("<H", cd[p + 10 : p + 12])
        csz, usz = struct.unpack("<II", cd[p + 20 : p + 28])
        nlen, elen, clen = struct.unpack("<HHH", cd[p + 28 : p + 34])
        (lho,) = struct.unpack("<I", cd[p + 42 : p + 46])
        name = cd[p + 46 : p + 46 + nlen].decode(errors="replace")
        extra = cd[p + 46 + nlen : p + 46 + nlen + elen]
        if 0xFFFFFFFF in (csz, usz, lho):
            q = 0
            while q + 4 <= len(extra):
                hid, hsz = struct.unpack("<HH", extra[q : q + 4])
                body = extra[q + 4 : q + 4 + hsz]
                k = 0
                if hid == 1:
                    if usz == 0xFFFFFFFF:
                        (usz,) = struct.unpack("<Q", body[k : k + 8])
                        k += 8
                    if csz == 0xFFFFFFFF:
                        (csz,) = struct.unpack("<Q", body[k : k + 8])
                        k += 8
                    if lho == 0xFFFFFFFF:
                        (lho,) = struct.unpack("<Q", body[k : k + 8])
                        k += 8
                q += 4 + hsz
        out.append(
            dict(name=name, method=method, csize=csz, usize=usz, lho=lho)
        )
        p += 46 + nlen + elen + clen
    return out


def head_of(url, ent, nbytes=200000):
    lh = rng(url, ent["lho"], ent["lho"] + 29)
    nlen, elen = struct.unpack("<HH", lh[26:30])
    data_off = ent["lho"] + 30 + nlen + elen
    raw = rng(url, data_off, data_off + nbytes)
    if ent["method"] == 0:
        return raw
    d = zlib.decompressobj(-15)
    try:
        return d.decompress(raw)
    except Exception:
        return b""


if __name__ == "__main__":
    url = sys.argv[1]
    ents = central_dir(url)
    for e in ents:
        print(f"{e['name']}\tcsize={e['csize']}\tusize={e['usize']}")
    if len(sys.argv) > 2:
        want = sys.argv[2]
        for e in ents:
            if want in e["name"]:
                b = head_of(url, e)
                print("\n=== HEADER", e["name"])
                print(b.split(b"\n")[0].decode(errors="replace"))
