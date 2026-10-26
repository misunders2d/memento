"""Build-time native media assets only. No codecs/model are executed.

Input is the installed public wheels from the qualified baseline library stage.
Output contains no Python modules, model weights, fixture data or private reports.
"""
import argparse, hashlib, io, json, shutil, tarfile, tempfile
from pathlib import Path
from urllib.request import urlopen
FILES = {'av.libs/libSvtAv1Enc-cc8e453e.so.4.2.0': {'name': 'libSvtAv1Enc-cc8e453e.so.4.2.0', 'sha256': 'bfd8be97db5aa29114f16ca0c2ce5e931215440156fceeb8e9270ed99b02acb8'}, 'av.libs/libavcodec-d61378e8.so.63.1.102': {'name': 'libavcodec-d61378e8.so.63.1.102', 'sha256': '5ff458a6833094996f24c1928afe67dac1ec87cdb7c9e58d13a44703994bc3ed'}, 'av.libs/libavformat-7f5186bf.so.63.1.102': {'name': 'libavformat-7f5186bf.so.63.1.102', 'sha256': '4a54860701b836b3dd701f07dd48068af3e2891e68e00c4a0779b73af4ad9bf4'}, 'av.libs/libavutil-8f0ab4ee.so.61.1.102': {'name': 'libavutil-8f0ab4ee.so.61.1.102', 'sha256': '61bb48e8fa35650e5b7f710500feeb39a4243cf1937d2774a3db0c5f9aef908d'}, 'av.libs/libdav1d-0fe430b9.so.7.0.0': {'name': 'libdav1d-0fe430b9.so.7.0.0', 'sha256': '819965f42b8ab0c0bead2f0d7689bfe68b89758d78d70239e97ac05dd998fb18'}, 'av.libs/libdrm-b0291a67.so.2.4.0': {'name': 'libdrm-b0291a67.so.2.4.0', 'sha256': 'e6315a0e5efd1918cd918755b09c188cb74c89e5882e966cd3111dc9015a6b1e'}, 'av.libs/libgmp-8c6c054f.so.10.5.0': {'name': 'libgmp-8c6c054f.so.10.5.0', 'sha256': '9bef5e914c9932edd5890bbbff5fedc348a14d184cfea96f20b6e7c4e1b96a1c'}, 'av.libs/libgnutls-e2125fb5.so.30.42.0': {'name': 'libgnutls-e2125fb5.so.30.42.0', 'sha256': 'f594c721830c189c27aa8866c021f76872a3afb2444cef89998ebc3c78385b6f'}, 'av.libs/libhogweed-e2118ea2.so.7.0': {'name': 'libhogweed-e2118ea2.so.7.0', 'sha256': '28a6c71f005dbbae53f722a9ce113c480165a5ea7f1d7224fd01a0d34c20aa07'}, 'av.libs/libnettle-6eb03162.so.9.0': {'name': 'libnettle-6eb03162.so.9.0', 'sha256': 'c6fcd40f3302da8bd6983caa33d73166206100a632c9322d0d076bfbdf60353d'}, 'av.libs/libopus-114e2b1a.so.0.11.1': {'name': 'libopus-114e2b1a.so.0.11.1', 'sha256': '6b52801726dae225720c84ce12eaa7831af05dcea7be46f61b10d4a9fc653ce1'}, 'av.libs/libsharpyuv-48a062bd.so.0.1.2': {'name': 'libsharpyuv-48a062bd.so.0.1.2', 'sha256': 'ecd8dd0fb4aed6144752f0a8aed49d463fdd01562dfd4bade29858457412cc0a'}, 'av.libs/libswresample-d0a74e06.so.7.1.102': {'name': 'libswresample-d0a74e06.so.7.1.102', 'sha256': 'fcc73f5c140afb861890cc110dbc2237ab27a5b95a18e7d10937b965afcfad3f'}, 'av.libs/libswscale-cd7831fd.so.10.1.102': {'name': 'libswscale-cd7831fd.so.10.1.102', 'sha256': 'a91d27bdb4b169cb30f688e2df9f8d13ee27814aacf4040e1b666e5f30fbbc65'}, 'av.libs/libunistring-09f6121d.so.5.2.1': {'name': 'libunistring-09f6121d.so.5.2.1', 'sha256': '4ee9e7d5ad934d77e1cc3f8e97ebb4f00b73bceb819481e8de516902c1ef46f2'}, 'av.libs/libvpl-e4e9cae8.so.2.16': {'name': 'libvpl-e4e9cae8.so.2.16', 'sha256': 'e785c031664bc2371406743993e512f43768789706eea631f0f579c8a663bf0c'}, 'av.libs/libvpx-e67815b1.so.12.1.0': {'name': 'libvpx-e67815b1.so.12.1.0', 'sha256': '3f00682bbcf52605df96e34c43558f51713b514eaa9e684449063ab4afdaa81b'}, 'av.libs/libwebp-0c5118d0.so.7.2.0': {'name': 'libwebp-0c5118d0.so.7.2.0', 'sha256': '759cd01a1be4510788207f7c27bd571378732689ee65a90090771a63bd45a274'}, 'av.libs/libwebpmux-e1880640.so.3.1.2': {'name': 'libwebpmux-e1880640.so.3.1.2', 'sha256': '8bb9b22c7fd4bcd4aef03d2ce6fe26e7300c62f1a6b6f2a04d5f0abe63bf71ef'}, 'av.libs/libx264-d6533a8d.so.165': {'name': 'libx264-d6533a8d.so.165', 'sha256': 'd160fece833d838690a21850fb11aa0cc2b63e5b6b99d5f5f7e4dfc7a969a874'}, 'av.libs/libx265-eebb7db1.so.217': {'name': 'libx265-eebb7db1.so.217', 'sha256': 'a8251bd1c08f242a10ed8a103d01da3cabf990bf356c193cc655faa1eab4c422'}, 'pillow_heif.libs/libde265-91206b2b.so.0.2.3': {'name': 'libde265-91206b2b.so.0.2.3', 'sha256': '4675f11969823d3035c9d2b36d8d06b821617606da6f82182886a1c8438b4e48'}, 'pillow_heif.libs/libheif-e72ea93c.so.1.23.6': {'name': 'libheif-e72ea93c.so.1.23.6', 'sha256': '27f15a811099395c2662baf13ad2e8bdecebaa49fbb9b1eded4a6bc9b6f551f7'}, 'pillow_heif.libs/libx265-4883ecb5.so.217': {'name': 'libx265-4883ecb5.so.217', 'sha256': 'b4be99b048721e3f27e79ca819a267651e7213831aeab15d9b0b433381212e74'}, 'pypdfium2_raw/libpdfium.so': {'name': 'libpdfium.so', 'sha256': 'fd4de9f91f183476d04b1bc3fd17dacd95aba349c55d2c376512d95e082b441e'}, 'pillow.libs/libavif-8a7f9d56.so.16.4.2': {'name': 'libavif-8a7f9d56.so.16.4.2', 'sha256': '3a8819e60f0fd7c716ef14776085a81c941db354002a9f9a679da38447e5507f'}}
ALIASES = {'libturbojpeg.so.0': 'libturbojpeg.so.0.5.0', 'libheif.so.1': 'libheif-e72ea93c.so.1.23.6', 'libavcodec.so.63': 'libavcodec-d61378e8.so.63.1.102', 'libavformat.so.63': 'libavformat-7f5186bf.so.63.1.102', 'libavutil.so.61': 'libavutil-8f0ab4ee.so.61.1.102', 'libswresample.so.7': 'libswresample-d0a74e06.so.7.1.102', 'libswscale.so.10': 'libswscale-cd7831fd.so.10.1.102', 'libavif.so.16': 'libavif-8a7f9d56.so.16.4.2'}
LICENSE_SOURCES = [{'file': 'licenses/upstream/FFmpeg-COPYING.LGPLv3', 'url': 'https://raw.githubusercontent.com/FFmpeg/FFmpeg/n9.0.2/COPYING.LGPLv3', 'bytes': 7651, 'sha256': 'da7eabb7bafdf7d3ae5e9f223aa5bdc1eece45ac569dc21b3b037520b4464768'}, {'file': 'licenses/upstream/FFmpeg-COPYING.GPLv3', 'url': 'https://raw.githubusercontent.com/FFmpeg/FFmpeg/n9.0.2/COPYING.GPLv3', 'bytes': 35147, 'sha256': '8ceb4b9ee5adedde47b31e975c1d90c73ad27b6b165a1dcd80c7c545eb65b903'}, {'file': 'licenses/upstream/libheif-COPYING', 'url': 'https://raw.githubusercontent.com/strukturag/libheif/v1.23.6/COPYING', 'bytes': 44366, 'sha256': 'fa81ce652315b013359d6e8e4744335f31a50c7c192907176d3632f78a3b4596'}, {'file': 'licenses/upstream/libde265-COPYING', 'url': 'https://raw.githubusercontent.com/strukturag/libde265/v1.1.3/COPYING', 'bytes': 44339, 'sha256': '02cc1585a20677992e0ba578fa692635dc193735f2691dc81de924b51c4e8020'}, {'file': 'licenses/upstream/x265-COPYING', 'url': 'https://raw.githubusercontent.com/Multicorewareinc/x265/4.3/COPYING', 'bytes': 18120, 'sha256': 'd8afb1bcc7a2cfc603683b168d6987ef0a48e59e0da3693bf55c5d33b67e2b49'}, {'file': 'licenses/upstream/libavif-LICENSE', 'url': 'https://raw.githubusercontent.com/AOMediaCodec/libavif/v1.4.2/LICENSE', 'bytes': 20415, 'sha256': '165abf92cc04b39e80d29cadea7a6a7e8fddf59407d4ad2616507a7ebe8216f9'}]
TURBO_URL = "https://github.com/libjpeg-turbo/libjpeg-turbo/releases/download/3.2.0/libjpeg-turbo-official_3.2.0_amd64.deb"
TURBO_SHA = "21297da4a4eb34ebefc54afca5d8dd86c0fdd6a9dfe49b1b962c5d1eeeafd8ec"
TURBO_LIBRARY_SHA = "cc9cc06afa9307a76612d413128cf56fcbc90f6790581e5d885fd474509b73e0"
def digest(data): return hashlib.sha256(data).hexdigest()
def fetch(url, maximum):
    with urlopen(url, timeout=30) as response: data = response.read(maximum + 1)
    if len(data) > maximum: raise RuntimeError("public asset exceeds frozen size")
    return data

def assemble(packages, output, fetcher=fetch):
    lib = output / "lib"; lib.mkdir(parents=True, exist_ok=True)
    licenses = output / "licenses"; licenses.mkdir(exist_ok=True)
    for relative, item in FILES.items():
        source = packages / relative; data = source.read_bytes()
        if digest(data) != item["sha256"]: raise RuntimeError("wheel-native checksum mismatch: " + relative)
        (lib / item["name"]).write_bytes(data)
    data = fetcher(TURBO_URL, 727160)
    if len(data) != 727160 or digest(data) != TURBO_SHA: raise RuntimeError("TurboJPEG package checksum mismatch")
    if data[:8] != b"!<arch>\n": raise RuntimeError("expected Debian ar container")
    at = 8; found = False
    while at < len(data):
        header = data[at:at+60]; name = header[:16].decode().strip().rstrip("/"); size = int(header[48:58])
        body = data[at+60:at+60+size]; at += 60 + size + size % 2
        if name.startswith("data.tar"):
            with tarfile.open(fileobj=io.BytesIO(body), mode="r:*") as archive:
                for member in archive.getmembers():
                    if not member.isfile(): continue
                    basename = Path(member.name).name
                    if basename == "libturbojpeg.so.0.5.0":
                        content = archive.extractfile(member).read()
                        if digest(content) != TURBO_LIBRARY_SHA: raise RuntimeError("TurboJPEG library checksum mismatch")
                        (lib / basename).write_bytes(content); found = True
                    elif "/doc/" in member.name and basename in ["README.ijg", "LICENSE.md"]:
                        target = licenses / "turbojpeg"; target.mkdir(exist_ok=True)
                        (target / basename).write_bytes(archive.extractfile(member).read())
    if not found: raise RuntimeError("TurboJPEG library missing")
    for alias, filename in ALIASES.items():
        destination = lib / alias
        if destination.exists() or destination.is_symlink(): destination.unlink()
        destination.symlink_to(filename)
    for distribution in ["av-19.0.1.dist-info", "pillow_heif-1.8.1.dist-info", "pypdfium2-5.14.0.dist-info", "pillow-12.3.0.dist-info"]:
        source = packages / distribution / "licenses"
        if not source.is_dir(): raise RuntimeError("wheel license directory missing")
        shutil.copytree(source, licenses / distribution, dirs_exist_ok=True)
    for item in LICENSE_SOURCES:
        content = fetcher(item["url"], item["bytes"])
        if digest(content) != item["sha256"]: raise RuntimeError("upstream license checksum mismatch")
        destination = output / item["file"]; destination.parent.mkdir(exist_ok=True, parents=True); destination.write_bytes(content)
    receipt = {"libraries": {p.name: digest(p.read_bytes()) for p in lib.iterdir() if p.is_file() and not p.is_symlink()},
               "aliases": ALIASES, "scope": "Only pinned public native SDKs/licenses; no Python decoder/model/weights"}
    (output / "native-assets.json").write_text(json.dumps(receipt, indent=2) + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--site-packages", type=Path, required=True); parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(); assemble(args.site_packages, args.output)
