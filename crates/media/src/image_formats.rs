//! The baseline's native AVIF converter plus FFmpeg JPEG2000. No Python path.
//! C ABI prefixes below follow libavif1.4.2 avif.h (BSD-2-Clause, Joe Drago).
//! Orientation/conversion follows Pillow12.3 _avif.c; see licenses/Pillow.
use crate::{Error, Result, images};
use image::{DynamicImage, GrayImage, ImageBuffer, RgbImage, RgbaImage, metadata::Orientation};
use std::{
    ffi::{CStr, c_char},
    path::Path,
    sync::OnceLock,
};

#[repr(C)]
struct Data {
    data: *mut u8,
    size: usize,
}
// Allocated/freed only by libavif. Only this stable leading portion is viewed.
#[repr(C)]
struct Image {
    width: u32,
    height: u32,
    depth: u32,
    yuv_format: i32,
    yuv_range: i32,
    chroma_position: i32,
    planes: [*mut u8; 3],
    row_bytes: [u32; 3],
    owns_yuv: i32,
    alpha: *mut u8,
    alpha_row: u32,
    owns_alpha: i32,
    premultiplied: i32,
    icc: Data,
    primaries: u16,
    transfer: u16,
    matrix: u16,
    clli: [u16; 2],
    transform_flags: u32,
    pasp: [u32; 2],
    clap: [u32; 8],
    angle: u8,
    axis: u8,
}
#[repr(C)]
struct Decoder {
    codec: i32,
    threads: i32,
    source: i32,
    progressive: i32,
    incremental: i32,
    ignore_exif: i32,
    ignore_xmp: i32,
    size_limit: u32,
    dimension_limit: u32,
    count_limit: u32,
    strict: u32,
    image: *mut Image,
}
#[repr(C)]
struct Rgb {
    width: u32,
    height: u32,
    depth: u32,
    format: i32,
    upsampling: i32,
    downsampling: i32,
    avoid_yuv: i32,
    ignore_alpha: i32,
    premultiplied: i32,
    is_float: i32,
    threads: i32,
    pixels: *mut u8,
    row_bytes: u32,
}
struct Api {
    create: unsafe extern "C" fn() -> *mut Decoder,
    destroy: unsafe extern "C" fn(*mut Decoder),
    set_io: unsafe extern "C" fn(*mut Decoder, *const u8, usize) -> i32,
    parse: unsafe extern "C" fn(*mut Decoder) -> i32,
    nth: unsafe extern "C" fn(*mut Decoder, u32) -> i32,
    defaults: unsafe extern "C" fn(*mut Rgb, *const Image),
    convert: unsafe extern "C" fn(*const Image, *mut Rgb) -> i32,
    _library: libloading::Library,
}
fn api() -> Result<&'static Api> {
    static API: OnceLock<std::result::Result<Api, ()>> = OnceLock::new();
    API.get_or_init(|| unsafe {
        let library = libloading::Library::new("libavif.so.16").map_err(|_| ())?;
        let version: unsafe extern "C" fn() -> *const c_char =
            *library.get(b"avifVersion\0").map_err(|_| ())?;
        let value = version();
        if value.is_null() || CStr::from_ptr(value).to_bytes() != b"1.4.2" {
            return Err(());
        }
        Ok(Api {
            create: *library.get(b"avifDecoderCreate\0").map_err(|_| ())?,
            destroy: *library.get(b"avifDecoderDestroy\0").map_err(|_| ())?,
            set_io: *library.get(b"avifDecoderSetIOMemory\0").map_err(|_| ())?,
            parse: *library.get(b"avifDecoderParse\0").map_err(|_| ())?,
            nth: *library.get(b"avifDecoderNthImage\0").map_err(|_| ())?,
            defaults: *library.get(b"avifRGBImageSetDefaults\0").map_err(|_| ())?,
            convert: *library.get(b"avifImageYUVToRGB\0").map_err(|_| ())?,
            _library: library,
        })
    })
    .as_ref()
    .map_err(|_| Error::Decode("Pinned libavif1.4.2 runtime unavailable"))
}
struct OwnedDecoder<'a> {
    ptr: *mut Decoder,
    api: &'a Api,
}
impl Drop for OwnedDecoder<'_> {
    fn drop(&mut self) {
        unsafe { (self.api.destroy)(self.ptr) }
    }
}
fn orientation(image: &Image) -> Orientation {
    let mirrored = image.transform_flags & 8 != 0;
    let value = if image.transform_flags & 4 != 0 {
        match (image.angle, mirrored, image.axis != 0) {
            (1, true, true) => 7,
            (1, true, false) => 5,
            (1, false, _) => 8,
            (2, true, true) => 4,
            (2, true, false) => 2,
            (2, false, _) => 3,
            (3, true, true) => 5,
            (3, true, false) => 7,
            (3, false, _) => 6,
            _ => {
                if mirrored {
                    if image.axis != 0 { 2 } else { 4 }
                } else {
                    1
                }
            }
        }
    } else if mirrored {
        if image.axis != 0 { 2 } else { 4 }
    } else {
        1
    };
    Orientation::from_exif(value).unwrap()
}
pub(crate) fn avif(data: &[u8]) -> Result<(DynamicImage, Orientation)> {
    let api = api()?;
    // The input byte slice outlives decoder IO and all conversion calls. Every
    // native allocation is owned by this guard; RGB pixels are Rust-owned.
    unsafe {
        let ptr = (api.create)();
        if ptr.is_null() {
            return Err(Error::Decode("AVIF decoder allocation failed"));
        }
        let decoder = OwnedDecoder { ptr, api };
        (*ptr).threads = 1;
        (*ptr).size_limit = 178_956_970;
        // Exact Pillow12.3 compatibility: permit missing pixi and invalid clap;
        // neither changes pixel crop or underlying byte integrity checks.
        (*ptr).strict &= !(1 | 2);
        if (api.set_io)(ptr, data.as_ptr(), data.len()) != 0 || (api.parse)(ptr) != 0 {
            return Err(Error::Decode("unreadable AVIF"));
        }
        if (*ptr).image.is_null() {
            return Err(Error::Decode("AVIF image missing"));
        }
        images::check_size((*(*ptr).image).width, (*(*ptr).image).height)?;
        if (api.nth)(ptr, 0) != 0 {
            return Err(Error::Decode("AVIF decode failed"));
        }
        let image = &*(*ptr).image;
        images::check_size(image.width, image.height)?;
        let mut rgb: Rgb = std::mem::zeroed();
        (api.defaults)(&mut rgb, image);
        rgb.depth = 8;
        rgb.threads = 1;
        let channels = if !image.alpha.is_null() {
            rgb.format = 1;
            4
        } else if image.yuv_format == 4 {
            rgb.format = 7;
            1
        } else {
            rgb.format = 0;
            3
        };
        let mut pixels = vec![0; image.width as usize * image.height as usize * channels];
        rgb.pixels = pixels.as_mut_ptr();
        rgb.row_bytes = image.width * channels as u32;
        if (api.convert)(image, &mut rgb) != 0 {
            return Err(Error::Decode("AVIF RGB conversion failed"));
        }
        let output = match channels {
            4 => DynamicImage::ImageRgba8(
                RgbaImage::from_raw(image.width, image.height, pixels)
                    .ok_or(Error::Decode("invalid AVIF RGBA"))?,
            ),
            1 => DynamicImage::ImageLuma8(
                GrayImage::from_raw(image.width, image.height, pixels)
                    .ok_or(Error::Decode("invalid AVIF gray"))?,
            ),
            _ => DynamicImage::ImageRgb8(
                RgbImage::from_raw(image.width, image.height, pixels)
                    .ok_or(Error::Decode("invalid AVIF RGB"))?,
            ),
        };
        let orientation = orientation(image);
        drop(decoder);
        Ok((output, orientation))
    }
}

pub(crate) fn jpeg2000(path: &Path) -> Result<DynamicImage> {
    use ffmpeg_next::{self as ff, codec, frame, media};
    let mut input = crate::av::open(std::fs::File::open(path)?)?;
    let stream = input
        .streams()
        .best(media::Type::Video)
        .ok_or(Error::Decode("JPEG2000 image missing"))?;
    if stream.parameters().id() != codec::Id::JPEG2000 {
        return Err(Error::Decode("invalid JPEG2000 codec"));
    }
    let index = stream.index();
    let mut context = codec::context::Context::from_parameters(stream.parameters())?;
    context.set_threading(codec::threading::Config::count(1));
    let mut decoder = context.decoder().video()?;
    images::check_size(decoder.width(), decoder.height())?;
    let mut decoded = frame::Video::empty();
    for (stream, packet) in input.packets() {
        if stream.index() != index {
            continue;
        }
        decoder.send_packet(&packet)?;
        if decoder.receive_frame(&mut decoded).is_ok() {
            break;
        }
    }
    if decoded.width() == 0 {
        decoder.send_eof()?;
        decoder.receive_frame(&mut decoded)?;
    }
    images::check_size(decoded.width(), decoded.height())?;
    if matches!(
        decoded.format(),
        ff::format::Pixel::GRAY16LE | ff::format::Pixel::GRAY16BE
    ) {
        let be = decoded.format() == ff::format::Pixel::GRAY16BE;
        let mut values = Vec::with_capacity(decoded.width() as usize * decoded.height() as usize);
        for y in 0..decoded.height() as usize {
            for b in decoded.data(0)
                [y * decoded.stride(0)..y * decoded.stride(0) + decoded.width() as usize * 2]
                .chunks_exact(2)
            {
                values.push(if be {
                    u16::from_be_bytes([b[0], b[1]])
                } else {
                    u16::from_le_bytes([b[0], b[1]])
                });
            }
        }
        // Use the shared Pillow I;16->RGB clamp in images::white, also used by
        // actual PNG/TIFF Luma16 inputs. Do not scale these values to8bits.
        return Ok(DynamicImage::ImageLuma16(
            ImageBuffer::from_raw(decoded.width(), decoded.height(), values)
                .ok_or(Error::Decode("invalid JPEG2000 gray"))?,
        ));
    }
    let mut scaler = ff::software::scaling::Context::get(
        decoded.format(),
        decoded.width(),
        decoded.height(),
        ff::format::Pixel::RGBA,
        decoded.width(),
        decoded.height(),
        ff::software::scaling::Flags::BICUBIC,
    )?;
    let mut output = frame::Video::empty();
    scaler.run(&decoded, &mut output)?;
    let mut pixels = Vec::with_capacity(output.width() as usize * output.height() as usize * 4);
    for y in 0..output.height() as usize {
        pixels.extend_from_slice(
            &output.data(0)
                [y * output.stride(0)..y * output.stride(0) + output.width() as usize * 4],
        );
    }
    Ok(DynamicImage::ImageRgba8(
        RgbaImage::from_raw(output.width(), output.height(), pixels)
            .ok_or(Error::Decode("invalid JPEG2000 RGBA"))?,
    ))
}

#[test]
fn pinned_avif_c_abi_prefix_layout() {
    // Independently measured against official v1.4.2 avif.h by the frozen C
    // offsetof probe; opaque owners are never allocated using these prefixes.
    assert_eq!(std::mem::offset_of!(Image, transform_flags), 116);
    assert_eq!(std::mem::offset_of!(Image, angle), 160);
    assert_eq!(std::mem::offset_of!(Decoder, image), 48);
    assert_eq!(std::mem::size_of::<Rgb>(), 64);
    assert_eq!(std::mem::offset_of!(Rgb, pixels), 48);
}
