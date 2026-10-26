//! Native preparation and JPEG previews. Thumbnail filtering/reduction follows
//! Pillow12.3.0 Image.py, Resample.c and Reduce.c. Copyright (c)1995-2009 Fredrik
//! Lundh;1997-2009 Secret Labs AB and Pillow contributors. See licenses/Pillow.
use crate::{Error, Result};
use image::{DynamicImage, ImageDecoder, ImageReader, RgbImage, metadata::Orientation};
use libheif_rs::{ColorSpace, HeifContext, LibHeif, RgbChroma};
use std::{fs, io::Read, path::Path};
const MAX_PIXELS: u64 = 178_956_970;
const RAW: &[&str] = &[
    "cr2", "cr3", "crw", "nef", "nrw", "arw", "srf", "sr2", "dng", "raf", "orf", "rw2", "pef",
    "srw", "raw", "rwl", "3fr", "erf", "kdc", "mrw", "x3f",
];
pub struct PreparedImage {
    pub rgb: RgbImage,
    pub original_width: u32,
    pub original_height: u32,
}
pub(crate) fn check_size(w: u32, h: u32) -> Result<()> {
    if w == 0 || h == 0 || w as u64 * h as u64 > MAX_PIXELS {
        Err(Error::Decode("invalid or oversized image"))
    } else {
        Ok(())
    }
}
fn bytes(path: &Path) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    fs::File::open(path)?
        .take(1024 * 1024 * 1024 + 1)
        .read_to_end(&mut out)?;
    if out.len() > 1024 * 1024 * 1024 {
        return Err(Error::Decode("image file too large"));
    }
    Ok(out)
}
fn jpeg_decode(data: &[u8], max_px: Option<u32>) -> Result<RgbImage> {
    let mut decoder =
        turbojpeg::Decompressor::new().map_err(|_| Error::Decode("JPEG decoder unavailable"))?;
    let header = decoder
        .read_header(data)
        .map_err(|_| Error::Decode("unreadable JPEG"))?;
    check_size(header.width as u32, header.height as u32)?;
    let mut factor = turbojpeg::ScalingFactor::ONE;
    if let Some(max) = max_px {
        for (div, f) in [
            (8, turbojpeg::ScalingFactor::ONE_EIGHTH),
            (4, turbojpeg::ScalingFactor::ONE_QUARTER),
            (2, turbojpeg::ScalingFactor::ONE_HALF),
        ] {
            if header.width / div >= max as usize && header.height / div >= max as usize {
                factor = f;
                break;
            }
        }
    }
    decoder
        .set_scaling_factor(factor)
        .map_err(|_| Error::Decode("JPEG scaling unavailable"))?;
    let (w, h) = (factor.scale(header.width), factor.scale(header.height));
    let mut rgb = vec![0; w * h * 3];
    decoder
        .decompress(
            data,
            turbojpeg::Image {
                pixels: rgb.as_mut_slice(),
                width: w,
                pitch: w * 3,
                height: h,
                format: turbojpeg::PixelFormat::RGB,
            },
        )
        .map_err(|_| Error::Decode("unreadable JPEG"))?;
    RgbImage::from_raw(w as u32, h as u32, rgb).ok_or(Error::Decode("invalid JPEG shape"))
}
pub fn jpeg(rgb: &RgbImage) -> Result<Vec<u8>> {
    let mut encoder =
        turbojpeg::Compressor::new().map_err(|_| Error::Decode("JPEG encoder unavailable"))?;
    encoder
        .set_quality(90)
        .map_err(|_| Error::Decode("invalid JPEG quality"))?;
    encoder
        .set_subsamp(turbojpeg::Subsamp::Sub2x2)
        .map_err(|_| Error::Decode("invalid JPEG chroma"))?;
    encoder
        .compress_to_vec(turbojpeg::Image {
            pixels: rgb.as_raw().as_slice(),
            width: rgb.width() as usize,
            pitch: rgb.width() as usize * 3,
            height: rgb.height() as usize,
            format: turbojpeg::PixelFormat::RGB,
        })
        .map_err(|_| Error::Decode("JPEG encode failed"))
}
pub fn jpeg_backed(rgb: &RgbImage) -> Result<RgbImage> {
    jpeg_decode(&jpeg(rgb)?, None)
}
fn white(image: DynamicImage) -> RgbImage {
    match image {
        DynamicImage::ImageLuma16(input) => {
            let mut output = RgbImage::new(input.width(), input.height());
            for (x, y, p) in input.enumerate_pixels() {
                let n = p[0].min(255) as u8;
                output.put_pixel(x, y, image::Rgb([n, n, n]));
            }
            output
        }
        input => {
            let mut rgba = input.to_rgba8();
            for p in rgba.pixels_mut() {
                let a = p[3] as u32;
                for c in 0..3 {
                    let v = p[c] as u32 * a + 255 * (255 - a);
                    p[c] = ((v + 128 + ((v + 128) >> 8)) >> 8) as u8;
                }
                p[3] = 255;
            }
            DynamicImage::ImageRgba8(rgba).to_rgb8()
        }
    }
}
fn heif(path: &Path) -> Result<(DynamicImage, Option<Orientation>)> {
    let mut context =
        HeifContext::read_from_file(path.to_str().ok_or(Error::Decode("invalid image path"))?)
            .map_err(|_| Error::Decode("unreadable HEIF"))?;
    context.set_max_decoding_threads(1);
    let handle = context
        .primary_image_handle()
        .map_err(|_| Error::Decode("HEIF primary image missing"))?;
    check_size(handle.width(), handle.height())?;
    let lib = LibHeif::new();
    let image = lib
        .decode(&handle, ColorSpace::Rgb(RgbChroma::Rgba), None)
        .map_err(|_| Error::Decode("HEIF decode failed"))?;
    let plane = image
        .planes()
        .interleaved
        .ok_or(Error::Decode("HEIF RGB plane missing"))?;
    let mut pixels = Vec::with_capacity(image.width() as usize * image.height() as usize * 4);
    for y in 0..image.height() as usize {
        pixels.extend_from_slice(
            &plane.data[y * plane.stride..y * plane.stride + image.width() as usize * 4],
        );
    }
    // pillow-heif1.8.1 plugin calls set_orientation(info), resetting EXIF
    // orientation to1. Libheif already honors the container irot/imir transform;
    // applying the stale metadata here would rotate the decoded pixels twice.
    let rgba = image::RgbaImage::from_raw(image.width(), image.height(), pixels)
        .ok_or(Error::Decode("invalid HEIF shape"))?;
    Ok((DynamicImage::ImageRgba8(rgba), None))
}
pub fn prepare(path: &Path, filename: &str, max_px: u32, min_side: u32) -> Result<PreparedImage> {
    if max_px == 0 {
        return Err(Error::Decode("invalid thumbnail size"));
    }
    let ext = filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if RAW.contains(&ext.as_str()) {
        return Err(Error::Decode("RAW image"));
    }
    // Capture stubs can lack a suffix. Select these formats from their bytes,
    // as Pillow does, rather than trusting an attachment filename.
    let mut header = Vec::new();
    fs::File::open(path)?.take(4096).read_to_end(&mut header)?;
    let brands = if header.get(4..8) == Some(b"ftyp") {
        let end = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        &header[8..end.min(header.len()).max(8)]
    } else {
        &[]
    };
    let avif = brands
        .chunks_exact(4)
        .any(|b| matches!(b, b"avif" | b"avis"));
    let jp2 = header.starts_with(b"\0\0\0\x0cjP  \r\n\x87\n")
        || header.starts_with(&[0xff, 0x4f, 0xff, 0x51]);
    let is_heif = brands
        .chunks_exact(4)
        .any(|b| matches!(b, b"heic" | b"heix" | b"hevc" | b"hevx" | b"heim" | b"heis"));
    let (mut image, orientation, original) = if avif {
        let (image, orientation) = crate::image_formats::avif(&bytes(path)?)?;
        let original = (image.width(), image.height());
        (image, Some(orientation), original)
    } else if jp2 {
        let image = crate::image_formats::jpeg2000(path)?;
        let original = (image.width(), image.height());
        (image, None, original)
    } else if is_heif || matches!(ext.as_str(), "heic" | "heif") {
        let (image, orientation) = heif(path)?;
        let original = (image.width(), image.height());
        (image, orientation, original)
    } else {
        let mut decoder = ImageReader::open(path)?
            .with_guessed_format()?
            .into_decoder()?;
        let original = decoder.dimensions();
        check_size(original.0, original.1)?;
        let orientation = decoder.orientation()?;
        let data = bytes(path)?;
        let image = if data.starts_with(&[255, 216]) {
            DynamicImage::ImageRgb8(jpeg_decode(&data, Some(max_px))?)
        } else {
            DynamicImage::from_decoder(decoder)?
        };
        (image, Some(orientation), original)
    };
    if original.0.max(original.1) < min_side {
        return Err(Error::Decode("small image"));
    }
    if let Some(orientation) = orientation {
        image.apply_orientation(orientation);
    }
    Ok(PreparedImage {
        rgb: thumbnail(white(image), max_px),
        original_width: original.0,
        original_height: original.1,
    })
}

fn target(w: u32, h: u32, max: u32) -> (u32, u32) {
    if w <= max && h <= max {
        return (w, h);
    }
    let aspect = w as f64 / h as f64;
    if 1.0 >= aspect {
        let n = max as f64 * aspect;
        let floor = n.floor();
        let ceil = n.ceil();
        let x = if (aspect - floor / max as f64).abs() <= (aspect - ceil / max as f64).abs() {
            floor
        } else {
            ceil
        };
        (x.max(1.0) as u32, max)
    } else {
        let n = max as f64 / aspect;
        let floor = n.floor();
        let ceil = n.ceil();
        let error = |v: f64| {
            if v == 0.0 {
                0.0
            } else {
                (aspect - max as f64 / v).abs()
            }
        };
        let y = if error(floor) <= error(ceil) {
            floor
        } else {
            ceil
        };
        (max, y.max(1.0) as u32)
    }
}
struct Coeff {
    first: usize,
    weights: Vec<i32>,
}
fn coefficients(input: usize, end: f64, output: usize) -> Vec<Coeff> {
    let scale = end as f32 as f64 / output as f64;
    let filterscale = scale.max(1.0);
    let support = 3.0 * filterscale;
    let inv = 1.0 / filterscale;
    (0..output)
        .map(|o| {
            let center = (o as f64 + 0.5) * scale;
            let first = ((center - support + 0.5) as isize).max(0) as usize;
            let last = ((center + support + 0.5) as usize).min(input);
            let mut weights: Vec<f64> = (first..last)
                .map(|i| {
                    let x = (i as f64 - center + 0.5) * inv;
                    fn sinc(x: f64) -> f64 {
                        if x == 0.0 {
                            1.0
                        } else {
                            let p = x * std::f64::consts::PI;
                            p.sin() / p
                        }
                    }
                    if (-3.0..3.0).contains(&x) {
                        sinc(x) * sinc(x / 3.0)
                    } else {
                        0.0
                    }
                })
                .collect();
            let sum: f64 = weights.iter().sum();
            if sum != 0.0 {
                for w in &mut weights {
                    *w /= sum;
                }
            }
            Coeff {
                first,
                weights: weights
                    .into_iter()
                    .map(|v| {
                        if v < 0.0 {
                            (v * (1u32 << 22) as f64 - 0.5) as i32
                        } else {
                            (v * (1u32 << 22) as f64 + 0.5) as i32
                        }
                    })
                    .collect(),
            }
        })
        .collect()
}
fn axis(input: RgbImage, out: u32, end: f64, horizontal: bool) -> RgbImage {
    let (w, h) = input.dimensions();
    let count = if horizontal { w } else { h };
    if out == count && end == count as f64 {
        return input;
    }
    let coeff = coefficients(count as usize, end, out as usize);
    let (ow, oh) = if horizontal { (out, h) } else { (w, out) };
    let mut result = RgbImage::new(ow, oh);
    for y in 0..oh {
        for x in 0..ow {
            let c = &coeff[if horizontal { x } else { y } as usize];
            let mut pixel = [0u8; 3];
            for channel in 0..3 {
                let mut sum = 1i64 << 21;
                for (i, weight) in c.weights.iter().enumerate() {
                    let (px, py) = if horizontal {
                        ((c.first + i) as u32, y)
                    } else {
                        (x, (c.first + i) as u32)
                    };
                    sum += input.get_pixel(px, py)[channel] as i64 * *weight as i64;
                }
                pixel[channel] = (sum >> 22).clamp(0, 255) as u8;
            }
            result.put_pixel(x, y, image::Rgb(pixel));
        }
    }
    result
}
fn reduce(input: RgbImage, fx: u32, fy: u32) -> RgbImage {
    let (w, h) = input.dimensions();
    let mut out = RgbImage::new(w.div_ceil(fx), h.div_ceil(fy));
    for y in 0..out.height() {
        for x in 0..out.width() {
            let x0 = x * fx;
            let y0 = y * fy;
            let nx = fx.min(w - x0);
            let ny = fy.min(h - y0);
            let n = nx * ny;
            let multiplier = ((1u64 << 32) as f32 / (n * 256) as f32) as u32;
            let mut p = [0u8; 3];
            for c in 0..3 {
                let mut sum = n / 2;
                for yy in y0..y0 + ny {
                    for xx in x0..x0 + nx {
                        sum += input.get_pixel(xx, yy)[c] as u32;
                    }
                }
                p[c] = (sum.wrapping_mul(multiplier) >> 24) as u8;
            }
            out.put_pixel(x, y, image::Rgb(p));
        }
    }
    out
}
pub fn thumbnail(mut input: RgbImage, max: u32) -> RgbImage {
    let (w, h) = input.dimensions();
    let (ow, oh) = target(w, h, max);
    if (w, h) == (ow, oh) {
        return input;
    }
    let fx = ((w as f64 / ow as f64 / 2.0) as u32).max(1);
    let fy = ((h as f64 / oh as f64 / 2.0) as u32).max(1);
    let end_x = w as f64 / fx as f64;
    let end_y = h as f64 / fy as f64;
    if fx > 1 || fy > 1 {
        input = reduce(input, fx, fy);
    }
    if input.height() > input.width().saturating_mul(100) && oh < input.height() {
        input = axis(input, oh, end_y, false);
        axis(input, ow, end_x, true)
    } else {
        input = axis(input, ow, end_x, true);
        axis(input, oh, end_y, false)
    }
}

/// Pillow L conversion and BOX resize, before JPEG roundtrip as in shot detection.
pub fn average_hash(rgb: &RgbImage) -> u64 {
    let (w, h) = rgb.dimensions();
    let gray: Vec<u8> = rgb
        .pixels()
        .map(|p| {
            ((19595u32 * p[0] as u32 + 38470u32 * p[1] as u32 + 7471u32 * p[2] as u32 + 32768)
                >> 16) as u8
        })
        .collect();
    fn axis(data: &[u8], w: usize, h: usize, horizontal: bool) -> Vec<u8> {
        let input = if horizontal { w } else { h };
        let scale = input as f64 / 8.0;
        let support = 0.5 * scale.max(1.0);
        let mut out = vec![0; if horizontal { 8 * h } else { w * 8 }];
        for i in 0..8 {
            let center = (i as f64 + 0.5) * scale;
            let first = ((center - support + 0.5) as isize).max(0) as usize;
            let last = ((center + support + 0.5) as usize).min(input);
            let weights: Vec<f64> = (first..last)
                .map(|n| {
                    let x = (n as f64 - center + 0.5) / scale.max(1.0);
                    if x > -0.5 && x <= 0.5 { 1.0 } else { 0.0 }
                })
                .collect();
            let sum: f64 = weights.iter().sum();
            let coeff: Vec<i32> = weights
                .into_iter()
                .map(|v| (v / sum * (1u32 << 22) as f64 + 0.5) as i32)
                .collect();
            for other in 0..if horizontal { h } else { w } {
                let mut value = 1i64 << 21;
                for (n, weight) in coeff.iter().enumerate() {
                    let offset = if horizontal {
                        other * w + first + n
                    } else {
                        (first + n) * w + other
                    };
                    value += data[offset] as i64 * *weight as i64;
                }
                let offset = if horizontal {
                    other * 8 + i
                } else {
                    i * w + other
                };
                out[offset] = (value >> 22).clamp(0, 255) as u8;
            }
        }
        out
    }
    let small = axis(
        &axis(&gray, w as usize, h as usize, true),
        8,
        h as usize,
        false,
    );
    let mean = small.iter().map(|p| *p as usize).sum::<usize>() / 64;
    small.iter().enumerate().fold(0u64, |bits, (i, p)| {
        bits | u64::from(*p as usize > mean) << i
    })
}
pub fn rotate_ccw(rgb: RgbImage, degrees: i32) -> RgbImage {
    let angle = degrees.rem_euclid(360);
    match angle {
        0 => return rgb,
        90 => return image::imageops::rotate270(&rgb),
        180 => return image::imageops::rotate180(&rgb),
        270 => return image::imageops::rotate90(&rgb),
        _ => (),
    }
    let radians = -(angle as f64).to_radians();
    let c = (radians.cos() * 1e15).round() / 1e15;
    let s = (radians.sin() * 1e15).round() / 1e15;
    let (w, h) = rgb.dimensions();
    let (cx, cy) = (w as f64 / 2.0, h as f64 / 2.0);
    let mut tx = c * (-cx) + s * (-cy) + cx;
    let mut ty = -s * (-cx) + c * (-cy) + cy;
    let mut xx = Vec::new();
    let mut yy = Vec::new();
    for (x, y) in [
        (0.0, 0.0),
        (w as f64, 0.0),
        (w as f64, h as f64),
        (0.0, h as f64),
    ] {
        xx.push(c * x + s * y + tx);
        yy.push(-s * x + c * y + ty);
    }
    let nw = (xx.iter().copied().fold(f64::NEG_INFINITY, f64::max).ceil()
        - xx.iter().copied().fold(f64::INFINITY, f64::min).floor()) as u32;
    let nh = (yy.iter().copied().fold(f64::NEG_INFINITY, f64::max).ceil()
        - yy.iter().copied().fold(f64::INFINITY, f64::min).floor()) as u32;
    let (dx, dy) = (
        -((nw as f64 - w as f64) / 2.0),
        -((nh as f64 - h as f64) / 2.0),
    );
    tx += c * dx + s * dy;
    ty += -s * dx + c * dy;
    let mut out = RgbImage::new(nw, nh);
    for y in 0..nh {
        for x in 0..nw {
            let a = c * (x as f64 + 0.5) + s * (y as f64 + 0.5) + tx;
            let b = -s * (x as f64 + 0.5) + c * (y as f64 + 0.5) + ty;
            if a >= 0.0 && b >= 0.0 && a < w as f64 && b < h as f64 {
                out.put_pixel(x, y, *rgb.get_pixel(a as u32, b as u32));
            }
        }
    }
    out
}
