"""Build-only verified native SDK/runtime preparation. Never imports wheels.

All downloads are public fixed HTTPS artifacts. No model weights, accounts,
Python inference or codec execution. --source-packages reuses a qualified local
wheel extraction; the same27 native-file hashes remain mandatory.
"""
import argparse, hashlib, io, json, os, shutil, subprocess, tarfile, tempfile, zipfile
from pathlib import Path, PurePosixPath
from urllib.request import urlopen
import assemble_native_media as media

WHEELS = [{'project': 'av', 'version': '19.0.1', 'filename': 'av-19.0.1-cp312-abi3-manylinux_2_28_x86_64.whl', 'url': 'https://files.pythonhosted.org/packages/c8/97/5fb45934ac64e8afc2c6869a7dcb8cb2af1ddab09a725367548856cbb59f/av-19.0.1-cp312-abi3-manylinux_2_28_x86_64.whl', 'bytes': 34983451, 'sha256': '1bea5b6134209305199bce7627ac3d33964de2cf2b09c77d08e7f67cf8bd4170'}, {'project': 'pillow', 'version': '12.3.0', 'filename': 'pillow-12.3.0-cp314-cp314-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl', 'url': 'https://files.pythonhosted.org/packages/5c/44/c85361f65dbe00eea8576ee467c768d25129989efb76e94f205e9ca9bb46/pillow-12.3.0-cp314-cp314-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl', 'bytes': 6936962, 'sha256': '251bf95b67017e27b13d82f5b326234ca62d70f9cf4c2b9032de2358a3b12c7b'}, {'project': 'pillow-heif', 'version': '1.8.1', 'filename': 'pillow_heif-1.8.1-cp314-cp314-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl', 'url': 'https://files.pythonhosted.org/packages/51/5a/0b3961c9a0bd7f54c65aa8cf06ac2ff806850d9d14fae78a3835148488b9/pillow_heif-1.8.1-cp314-cp314-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl', 'bytes': 5708943, 'sha256': '6045ef6f9bd7107713b95c8b1ac02418fee08f5b116a9e3cd1e11a5d95007f38'}, {'project': 'pypdfium2', 'version': '5.14.0', 'filename': 'pypdfium2-5.14.0-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl', 'url': 'https://files.pythonhosted.org/packages/4f/a3/c9cc797fc8bdfb8f37b9b0f8b9d02a5fc196b2015f408d53624cab5b0519/pypdfium2-5.14.0-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl', 'bytes': 3799581, 'sha256': '11f281613fa22313d9c7ab89947665e84eccf8ebe40e1198a84a88352305648d'}]
FFMPEG = {'url': 'https://ffmpeg.org/releases/ffmpeg-9.0.2.tar.xz', 'bytes': 12040788, 'sha256': '8c3850283eb25fa026482078a04051e0be17347b09ef81a0849bec15a96e002e', 'primary_source': 'officialFFmpeg release, exactversion9.0.2; checksumfrozenfromHTTPS notclaimingvendorpublishedchecksum'}
ORT = {'url': 'https://github.com/microsoft/onnxruntime/releases/download/v1.31.0/onnxruntime-linux-x64-1.31.0.tgz', 'bytes': 11408003, 'sha256': 'cc5c72baf5ae5c8238a6841f0897227be2d02826b9cf98eaf02fdefaeeb03a57', 'library_path_in_archive': 'onnxruntime-linux-x64-1.31.0/lib/libonnxruntime.so.1.31.0', 'library_sha256': '469849977cb60744ea160927e1a3a0643fd4c55c0aff743251c6d293402e334e', 'license': 'MIT', 'notice_files': [{'path_in_archive': 'onnxruntime-linux-x64-1.31.0/LICENSE', 'bytes': 1073, 'sha256': '2f07c72751aed99790b8a4869cf2311df85a860b22ded05fa22803587a48922c'}, {'path_in_archive': 'onnxruntime-linux-x64-1.31.0/ThirdPartyNotices.txt', 'bytes': 338088, 'sha256': '143764b952fdb1a7c69ce653bfba74a7744d6a8a573bfb73e235fba356c83de3'}, {'path_in_archive': 'onnxruntime-linux-x64-1.31.0/Privacy.md', 'bytes': 6571, 'sha256': 'ea592b16d154ff2de759ba5210350c64593c378c1bbd0a2aa4ddef777edbf358'}]}

def digest(data): return hashlib.sha256(data).hexdigest()
def checked(spec, cache):
    path=cache/spec['sha256']
    if path.exists():
        data=path.read_bytes()
    else:
        with urlopen(spec['url'],timeout=30)as response:
            data=response.read(spec['bytes']+1)
        if len(data)!=spec['bytes']or digest(data)!=spec['sha256']:
            raise RuntimeError('public native artifact size/SHA mismatch')
        cache.mkdir(parents=True,exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=cache,delete=False)as out:
            temporary=Path(out.name);out.write(data);out.flush();os.fsync(out.fileno())
        temporary.replace(path)
    if len(data)!=spec['bytes']or digest(data)!=spec['sha256']:
        raise RuntimeError('cached native artifact size/SHA mismatch')
    return data

def extract_wheels(cache, packages):
    selected=set(media.FILES)
    total=0
    for spec in WHEELS:
        with zipfile.ZipFile(io.BytesIO(checked(spec,cache)))as archive:
            for item in archive.infolist():
                parts=PurePosixPath(item.filename).parts
                license_file=len(parts)>=3 and parts[0].endswith('.dist-info')and parts[1]=='licenses'
                if item.filename not in selected and not license_file:continue
                if item.is_dir():continue
                if PurePosixPath(item.filename).is_absolute()or'..'in parts or(item.external_attr>>16)&0o170000==0o120000:
                    raise RuntimeError('unsafe native wheel member')
                total+=item.file_size
                if item.file_size>128*1024*1024 or total>256*1024*1024:
                    raise RuntimeError('native wheel extraction exceeds bound')
                destination=packages.joinpath(*parts);destination.parent.mkdir(parents=True,exist_ok=True)
                destination.write_bytes(archive.read(item))

def prepare_ort(cache, runtime, sdk):
    prefix='onnxruntime-linux-x64-1.31.0/'
    library=prefix+'lib/libonnxruntime.so.1.31.0'
    lib=runtime/'lib';lib.mkdir(parents=True,exist_ok=True)
    licenses=runtime/'licenses';licenses.mkdir(exist_ok=True)
    notices={v['path_in_archive']:v for v in ORT['notice_files']}
    found=set()
    with tarfile.open(fileobj=io.BytesIO(checked(ORT,cache)),mode='r:gz')as archive:
        for member in archive:
            if not member.isfile():continue
            name=member.name
            if name==library:
                data=archive.extractfile(member).read()
                if digest(data)!=ORT['library_sha256']:raise RuntimeError('ORT DLL checksum mismatch')
                (lib/'libonnxruntime.so.1.31.0').write_bytes(data);found.add(name)
            elif name in notices:
                data=archive.extractfile(member).read();spec=notices[name]
                if len(data)!=spec['bytes']or digest(data)!=spec['sha256']:raise RuntimeError('ORT notice checksum mismatch')
                (licenses/Path(name).name).write_bytes(data);found.add(name)
            elif name.startswith(prefix+'include/')and name.endswith('.h'):
                relative=PurePosixPath(name.removeprefix(prefix+'include/'))
                if relative.is_absolute()or'..'in relative.parts:raise RuntimeError('unsafe ORT header')
                target=sdk/'include/onnxruntime'/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(archive.extractfile(member).read())
    if found!={library,*notices}:raise RuntimeError('ORT DLL/notices incomplete')
    (lib/'libonnxruntime.so.1').symlink_to('libonnxruntime.so.1.31.0')

def prepare_sdk(cache, sdk, runtime, media_runtime, work):
    # Official FFmpeg target installs public/generated headers and pkgconfig;
    # it does not build any codec library. Link only the qualified native DLLs.
    source=work/'ffmpeg';source.mkdir()
    with tarfile.open(fileobj=io.BytesIO(checked(FFMPEG,cache)),mode='r:xz')as archive:
        for member in archive.getmembers():
            parts=PurePosixPath(member.name).parts
            if not parts or parts[0]!='ffmpeg-9.0.2' or member.issym()or member.islnk():raise RuntimeError('unsafe FFmpeg source member')
        archive.extractall(source,filter='data')
    source=source/'ffmpeg-9.0.2'
    subprocess.run(['./configure','--prefix='+str(sdk),'--libdir='+str(sdk/'lib'),'--pkgconfigdir='+str(sdk/'lib/pkgconfig'),
        '--disable-everything','--disable-programs','--disable-doc','--disable-debug','--disable-x86asm','--disable-network','--disable-autodetect',
        '--disable-avdevice','--disable-avfilter','--enable-shared','--disable-static'],cwd=source,check=True)
    subprocess.run(['make','-j2','install-headers'],cwd=source,check=True)
    lib=sdk/'lib';lib.mkdir(exist_ok=True)
    for alias in ['avcodec','avformat','avutil','swscale','swresample']:
        filename=next(name for name in media.ALIASES if name.startswith('lib'+alias+'.so.'))
        (lib/('lib'+alias+'.so')).symlink_to(media_runtime/'lib'/filename)
    (lib/'libheif.so').symlink_to(media_runtime/'lib/libheif.so.1')
    (lib/'libturbojpeg.so').symlink_to(media_runtime/'lib/libturbojpeg.so.0')
    # These two Rust sys crates use pinned pregenerated declarations; pkgconfig
    # supplies exact runtime versions/link paths, not distro codec headers.
    for name,version,link in [('libheif','1.23.6','heif'),('libturbojpeg','3.2.0','turbojpeg')]:
        (lib/'pkgconfig'/f'{name}.pc').write_text(f'prefix={sdk}\nlibdir=${{prefix}}/lib\nincludedir=${{prefix}}/include\nName: {name}\nDescription: Pinned qualified native runtime\nVersion: {version}\nLibs: -L${{libdir}} -l{link}\nCflags: -I${{includedir}}\n')

def main():
    p=argparse.ArgumentParser();p.add_argument('--cache',type=Path,required=True);p.add_argument('--output',type=Path);p.add_argument('--sdk',type=Path);p.add_argument('--source-packages',type=Path);p.add_argument('--extract-wheels-to',type=Path)
    args=p.parse_args();args.cache.mkdir(parents=True,exist_ok=True)
    if args.extract_wheels_to:
        extract_wheels(args.cache,args.extract_wheels_to)
        return
    if args.output is None or args.sdk is None:p.error('--output and --sdk are required for SDK preparation')
    args.output.mkdir(parents=True,exist_ok=True);args.sdk.mkdir(parents=True,exist_ok=True)
    specs={v['url']:v for v in media.LICENSE_SOURCES}
    specs[media.TURBO_URL]={'url':media.TURBO_URL,'bytes':727160,'sha256':media.TURBO_SHA}
    def fetch(url,maximum):
        spec=specs[url]
        if spec['bytes']!=maximum:raise RuntimeError('artifact bound differs')
        return checked(spec,args.cache)
    with tempfile.TemporaryDirectory(prefix='memento-native-sdk-')as temporary:
        work=Path(temporary);packages=args.source_packages or(work/'packages')
        if args.source_packages is None:extract_wheels(args.cache,packages)
        media_runtime=args.output/'media';media.assemble(packages,media_runtime,fetcher=fetch)
        prepare_ort(args.cache,args.output/'ort',args.sdk)
        prepare_sdk(args.cache,args.sdk,args.output/'ort',media_runtime,work)
    (args.output/'MODEL-SOURCES.txt').write_text('FullFP32 ONNX export: onnx-community/embeddinggemma-2-ONNX@daa72c51243991dfcaf9f9137d2c573d8f7790c0.\nBase/tokenizer: google/embeddinggemma-2@914f7f89142e33e77833254d9c9b90c3cef7303b.\nBoth pinned repository model cards declare Apache-2.0. Native verified bootstrap obtains exact assets/card into instance persistent model cache; image contains no model weights.\nORT1.31.0 MIT/license and ThirdPartyNotices retained in ort/licenses. Media vendor licenses retained in media/licenses.\n')
    print(json.dumps({'native_libraries':27,'model_weights_downloaded':False,'ort':'1.31.0','ffmpeg_headers':'9.0.2','output':str(args.output)}))
if __name__=='__main__':main()
