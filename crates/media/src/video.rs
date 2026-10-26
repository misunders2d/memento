//! Shot sampling preserves actual decoded timestamps and display rotation.
use crate::{Error, Result, av, images};
use ffmpeg_next::{self as ff, codec, format, frame, media};
use image::RgbImage;
use std::{fs::File, path::Path};
pub const MAX_FRAMES: usize = 1200;
pub struct Keyframe {
    pub start_s: f64,
    pub end_s: f64,
    pub rgb: RgbImage,
}
struct Scaler(*mut ff::ffi::SwsContext);
impl Scaler {
    fn new() -> Result<Self> {
        let p = unsafe { ff::ffi::sws_alloc_context() };
        if p.is_null() {
            Err(Error::Decode("video scaler unavailable"))
        } else {
            Ok(Self(p))
        }
    }
    fn rgb(&mut self, source: &frame::Video, w: u32, h: u32) -> Result<RgbImage> {
        let mut input = source.clone();
        let mut output = frame::Video::new(ff::format::Pixel::RGB24, w, h);
        // Match PyAV19's native sws_scale_frame route. Transfer/primaries conversion
        // is not requested by media extraction; neutralize those tags for scaling.
        unsafe {
            let src = input.as_mut_ptr();
            let dst = output.as_mut_ptr();
            ff::ffi::av_frame_copy_props(dst, src);
            (*src).color_trc = ff::ffi::AVColorTransferCharacteristic::AVCOL_TRC_UNSPECIFIED;
            (*src).color_primaries = ff::ffi::AVColorPrimaries::AVCOL_PRI_UNSPECIFIED;
            (*dst).color_trc = (*src).color_trc;
            (*dst).color_primaries = (*src).color_primaries;
            if (*src).colorspace as i32 == 5 {
                (*src).colorspace = ff::ffi::AVColorSpace::AVCOL_SPC_SMPTE170M;
                (*dst).colorspace = (*src).colorspace;
            }
            (*self.0).threads = 1;
            (*self.0).flags = ff::software::scaling::Flags::AREA.bits() as u32;
            let status = ff::ffi::sws_scale_frame(self.0, dst, src);
            if status < 0 {
                return Err(ff::Error::from(status).into());
            }
        }
        let mut pixels = Vec::with_capacity(w as usize * h as usize * 3);
        for y in 0..h as usize {
            let first = y * output.stride(0);
            pixels.extend_from_slice(&output.data(0)[first..first + w as usize * 3]);
        }
        RgbImage::from_raw(w, h, pixels).ok_or(Error::Decode("invalid decoded frame"))
    }
}
impl Drop for Scaler {
    fn drop(&mut self) {
        unsafe { ff::ffi::sws_free_context(&mut self.0) }
    }
}
fn time_base(input: &format::context::Input) -> f64 {
    let t = unsafe { (*input.as_ptr()).start_time };
    if t == ff::ffi::AV_NOPTS_VALUE {
        0.0
    } else {
        t as f64 / ff::ffi::AV_TIME_BASE as f64
    }
}
fn rotation(frame: &frame::Video) -> i32 {
    frame
        .side_data(ff::util::frame::side_data::Type::DisplayMatrix)
        .and_then(|d| {
            let bytes = d.data();
            if bytes.len() < 36 {
                return None;
            }
            let mut matrix = [0i32; 9];
            for (i, b) in bytes[..36].chunks_exact(4).enumerate() {
                matrix[i] = i32::from_ne_bytes(b.try_into().unwrap());
            }
            let angle = unsafe { ff::ffi::av_display_rotation_get(matrix.as_ptr()) };
            angle.is_finite().then_some(angle as i32)
        })
        .unwrap_or(0)
}
pub fn keyframes(
    path: &Path,
    stop: &dyn Fn() -> bool,
    emit: &mut dyn FnMut(Keyframe) -> Result<bool>,
) -> Result<()> {
    let mut input = av::open(File::open(path)?)?;
    let Some(duration) = av::duration(&input).filter(|d| d.is_finite() && *d > 0.0) else {
        return Ok(());
    };
    let Some(stream) = input
        .streams()
        .find(|s| s.parameters().medium() == media::Type::Video)
    else {
        return Ok(());
    };
    let index = stream.index();
    let tb = f64::from(stream.time_base());
    let mut context = codec::context::Context::from_parameters(stream.parameters())?;
    context.set_threading(codec::threading::Config::count(1));
    let mut decoder = context.decoder().video()?;
    let origin = time_base(&input);
    let step = 3.0_f64.max(duration / MAX_FRAMES as f64);
    let tolerance = step / 2.0;
    let mut at = 0.5_f64.min(duration / 2.0);
    let mut previous: Option<Keyframe> = None;
    let mut previous_hash = None;
    let mut scaler = Scaler::new()?;
    while at < duration {
        if stop() {
            return Ok(());
        }
        let target = origin + at;
        at += step;
        let status = unsafe {
            ff::ffi::av_seek_frame(
                input.as_mut_ptr(),
                index as i32,
                (target / tb) as i64,
                ff::ffi::AVSEEK_FLAG_BACKWARD,
            )
        };
        if status < 0 {
            continue;
        }
        decoder.flush();
        let mut selected = None;
        let mut examined = false;
        for (stream, packet) in input.packets() {
            if stream.index() != index {
                continue;
            }
            if decoder.send_packet(&packet).is_err() {
                break;
            }
            let mut frame = frame::Video::empty();
            while decoder.receive_frame(&mut frame).is_ok() {
                if let Some(pts) = frame.pts() {
                    let t = pts as f64 * tb;
                    if t >= target - tolerance {
                        if t <= target + tolerance {
                            selected = Some((t, frame));
                        }
                        examined = true;
                        break;
                    }
                }
            }
            if examined {
                break;
            }
        }
        let Some((timestamp, frame)) = selected else {
            continue;
        };
        let scale = (1024.0 / frame.width().max(frame.height()) as f64).min(1.0);
        let w = (frame.width() as f64 * scale).round_ties_even().max(1.0) as u32;
        let h = (frame.height() as f64 * scale).round_ties_even().max(1.0) as u32;
        let rgb = images::rotate_ccw(scaler.rgb(&frame, w, h)?, rotation(&frame));
        let hash = images::average_hash(&rgb);
        if previous_hash.is_some_and(|last: u64| (last ^ hash).count_ones() <= 4) {
            continue;
        }
        previous_hash = Some(hash);
        let start = (timestamp - origin).max(0.0);
        if let Some(mut old) = previous.take() {
            old.end_s = start;
            if !emit(old)? {
                return Ok(());
            }
        }
        previous = Some(Keyframe {
            start_s: start,
            end_s: duration,
            rgb: images::jpeg_backed(&rgb)?,
        });
    }
    if !stop() {
        if let Some(last) = previous {
            emit(last)?;
        }
    }
    Ok(())
}
