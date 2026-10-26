//! Native FFmpeg decode. Custom AVIO owns the sole readable artifact; nested
//! URL/file protocols are denied, including playlists referring to other files.
use crate::{Error, Result};
use av::{ChannelLayout, Dictionary, codec, format, frame, media, software};
use ffmpeg_next as av;
use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
    sync::OnceLock,
};
pub const SAMPLE_RATE: u32 = 16_000;
pub const MAX_AUDIO_WINDOWS: usize = 2160;
pub struct AudioWindow {
    pub start_s: f64,
    pub end_s: f64,
    pub samples: Vec<f32>,
}
fn init() -> Result<()> {
    static READY: OnceLock<std::result::Result<(), av::Error>> = OnceLock::new();
    READY
        .get_or_init(av::init)
        .as_ref()
        .map_err(|e| Error::Av(*e))?;
    Ok(())
}
pub fn open<R: Read + Seek + Send + 'static>(reader: R) -> Result<format::context::Input> {
    init()?;
    let io = format::context::StreamIo::from_read_seek(reader)?;
    let mut options = Dictionary::new();
    options.set("protocol_whitelist", "");
    options.set("enable_drefs", "0");
    options.set("use_absolute_path", "0");
    Ok(format::input_from_stream(io, None, Some(options))?)
}
fn base(input: &format::context::Input) -> f64 {
    let start = unsafe { (*input.as_ptr()).start_time };
    if start == av::ffi::AV_NOPTS_VALUE {
        0.0
    } else {
        start as f64 / av::ffi::AV_TIME_BASE as f64
    }
}
pub fn duration(input: &format::context::Input) -> Option<f64> {
    let value = input.duration();
    if value != av::ffi::AV_NOPTS_VALUE {
        return Some(value as f64 / av::ffi::AV_TIME_BASE as f64);
    }
    input.streams().find_map(|s| {
        let n = s.duration();
        (n != av::ffi::AV_NOPTS_VALUE).then(|| n as f64 * f64::from(s.time_base()))
    })
}

/// A bounded rolling buffer. Silent windows count toward the same15h cap.
struct Windows<'a> {
    buffer: Vec<f32>,
    offset: usize,
    decoded: usize,
    stopped: bool,
    stop: &'a dyn Fn() -> bool,
    emit: &'a mut dyn FnMut(AudioWindow) -> Result<bool>,
}
impl Windows<'_> {
    fn full(&mut self) -> Result<()> {
        const WIN: usize = 480000;
        const HOP: usize = 400000;
        while self.buffer.len() >= WIN && !self.stopped {
            if (self.stop)() {
                self.stopped = true;
                break;
            }
            let samples = &self.buffer[..WIN];
            let rms =
                (samples.iter().map(|x| (*x as f64).powi(2)).sum::<f64>() / WIN as f64).sqrt();
            if rms > 0.003
                && !(self.emit)(AudioWindow {
                    start_s: self.offset as f64 / SAMPLE_RATE as f64,
                    end_s: (self.offset + WIN) as f64 / SAMPLE_RATE as f64,
                    samples: samples.to_vec(),
                })?
            {
                self.stopped = true;
                break;
            }
            self.decoded += 1;
            if self.decoded >= MAX_AUDIO_WINDOWS {
                self.stopped = true;
                break;
            }
            self.buffer.copy_within(HOP.., 0);
            self.buffer.truncate(self.buffer.len() - HOP);
            self.offset += HOP;
        }
        Ok(())
    }
    fn feed(&mut self, samples: &[f32]) -> Result<()> {
        for piece in samples.chunks(SAMPLE_RATE as usize) {
            if self.stopped || (self.stop)() {
                self.stopped = true;
                break;
            }
            self.buffer.extend_from_slice(piece);
            self.full()?;
        }
        Ok(())
    }
    fn silence(&mut self, mut count: usize) -> Result<()> {
        let zero = [0f32; SAMPLE_RATE as usize];
        while count > 0 && !self.stopped {
            let n = count.min(zero.len());
            self.feed(&zero[..n])?;
            count -= n;
        }
        Ok(())
    }
    fn tail(&mut self) -> Result<()> {
        if self.stopped || (self.stop)() {
            return Ok(());
        }
        let covered = if self.offset == 0 { 0 } else { 80000 };
        if self.buffer.len().saturating_sub(covered) >= 32000 {
            let rms = (self.buffer.iter().map(|x| (*x as f64).powi(2)).sum::<f64>()
                / self.buffer.len() as f64)
                .sqrt();
            if rms > 0.003 {
                (self.emit)(AudioWindow {
                    start_s: self.offset as f64 / SAMPLE_RATE as f64,
                    end_s: (self.offset + self.buffer.len()) as f64 / SAMPLE_RATE as f64,
                    samples: std::mem::take(&mut self.buffer),
                })?;
            }
        }
        Ok(())
    }
}
struct Mono {
    resampler: Option<software::resampling::Context>,
    stereo: bool,
    trim: Option<usize>,
    out: usize,
    base: f64,
    time_base: f64,
}
impl Mono {
    fn push(&mut self, decoded: &mut frame::Audio, windows: &mut Windows<'_>) -> Result<()> {
        // FFmpeg may report only a channel count for WAV. The source filter graph
        // assigns its default order before Swr; direct Swr rejects an unspecified
        // frame against the layout normalized by swr_init. Normalize only that case.
        if decoded.channel_layout().is_empty() && decoded.channels() > 0 {
            decoded.set_channel_layout(ChannelLayout::default(decoded.channels() as i32));
        }
        let Some(pts) = decoded.pts() else {
            return Ok(());
        };
        let at = ((pts as f64 * self.time_base - self.base) * SAMPLE_RATE as f64).round_ties_even();
        if !at.is_finite() || at.abs() > usize::MAX as f64 {
            return Err(Error::Decode("invalid audio timestamp"));
        }
        if self.trim.is_none() {
            if at > 0.0 {
                windows.silence(at as usize)?;
                self.out = at as usize;
            }
            self.trim = Some((-at).max(0.0) as usize);
        } else if at - self.out as f64 > SAMPLE_RATE as f64 / 10.0 {
            windows.silence((at - self.out as f64) as usize)?;
            self.out = at as usize;
        }
        if windows.stopped {
            return Ok(());
        }
        if self.resampler.is_none() {
            let layout = if self.stereo {
                ChannelLayout::STEREO
            } else {
                ChannelLayout::MONO
            };
            self.resampler = Some(software::resampling::Context::get(
                decoded.format(),
                decoded.channel_layout(),
                decoded.rate(),
                av::format::Sample::F32(av::format::sample::Type::Planar),
                layout,
                SAMPLE_RATE,
            )?);
        }
        let resampler = self.resampler.as_mut().unwrap();
        let delayed = resampler
            .delay()
            .map(|d| d.output.max(0) as usize)
            .unwrap_or(0);
        let needed = (decoded.samples() * SAMPLE_RATE as usize).div_ceil(decoded.rate() as usize)
            + delayed
            + 64;
        let mut output = frame::Audio::new(
            resampler.output().format,
            needed,
            resampler.output().channel_layout,
        );
        output.set_rate(SAMPLE_RATE);
        resampler.run(decoded, &mut output)?;
        self.consume(&output, windows, true)
    }
    fn consume(
        &mut self,
        frame: &frame::Audio,
        windows: &mut Windows<'_>,
        count_output: bool,
    ) -> Result<()> {
        if frame.samples() == 0 {
            return Ok(());
        }
        let left = frame.plane::<f32>(0);
        let right = if self.stereo {
            Some(frame.plane::<f32>(1))
        } else {
            None
        };
        let trim = self.trim.as_mut().unwrap();
        let cut = (*trim).min(frame.samples());
        *trim -= cut;
        let samples: Vec<f32> = (cut..frame.samples())
            .map(|i| {
                let x = if let Some(right) = right {
                    0.5 * (left[i] + right[i])
                } else {
                    left[i]
                };
                x.clamp(-1.0, 1.0)
            })
            .collect();
        if samples.iter().any(|x| !x.is_finite()) {
            return Err(Error::Decode("nonfinite decoded audio"));
        }
        if count_output {
            self.out = self.out.saturating_add(samples.len());
        }
        windows.feed(&samples)
    }
    fn flush(&mut self, windows: &mut Windows<'_>) -> Result<()> {
        if self.resampler.is_none() {
            return Ok(());
        }
        loop {
            let resampler = self.resampler.as_mut().unwrap();
            let needed = resampler
                .delay()
                .map(|d| d.output.max(0) as usize)
                .unwrap_or(0)
                + 64;
            let mut output = frame::Audio::new(
                resampler.output().format,
                needed,
                resampler.output().channel_layout,
            );
            output.set_rate(SAMPLE_RATE);
            let more = resampler.flush(&mut output)?;
            let n = output.samples();
            self.consume(&output, windows, false)?;
            if windows.stopped || more.is_none() || n == 0 {
                break;
            }
        }
        Ok(())
    }
}
pub fn audio_windows(
    path: &Path,
    stop: &dyn Fn() -> bool,
    emit: &mut dyn FnMut(AudioWindow) -> Result<bool>,
) -> Result<()> {
    audio_from_reader(File::open(path)?, stop, emit)
}
pub fn audio_from_reader<R: Read + Seek + Send + 'static>(
    reader: R,
    stop: &dyn Fn() -> bool,
    emit: &mut dyn FnMut(AudioWindow) -> Result<bool>,
) -> Result<()> {
    let mut input = open(reader)?;
    let origin = base(&input);
    let Some(stream) = input
        .streams()
        .find(|s| s.parameters().medium() == media::Type::Audio)
    else {
        return Ok(());
    };
    let index = stream.index();
    let time_base = f64::from(stream.time_base());
    let mut context = codec::context::Context::from_parameters(stream.parameters())?;
    context.set_threading(codec::threading::Config::count(1));
    let mut decoder = context.decoder().audio()?;
    let mut mono = Mono {
        resampler: None,
        stereo: decoder.channels() > 1,
        trim: None,
        out: 0,
        base: origin,
        time_base,
    };
    let mut windows = Windows {
        buffer: Vec::new(),
        offset: 0,
        decoded: 0,
        stopped: false,
        stop,
        emit,
    };
    for (stream, packet) in input.packets() {
        if stop() {
            windows.stopped = true;
            break;
        }
        if stream.index() != index {
            continue;
        }
        if let Err(e) = decoder.send_packet(&packet) {
            if e == av::Error::InvalidData {
                continue;
            }
            return Err(e.into());
        }
        let mut frame = frame::Audio::empty();
        loop {
            match decoder.receive_frame(&mut frame) {
                Ok(()) => mono.push(&mut frame, &mut windows)?,
                Err(av::Error::Other { errno }) if errno == av::error::EAGAIN => break,
                Err(av::Error::InvalidData) => break,
                Err(av::Error::Eof) => break,
                Err(e) => return Err(e.into()),
            }
            if windows.stopped {
                break;
            }
        }
        if windows.stopped {
            break;
        }
    }
    if !windows.stopped {
        decoder.send_eof()?;
        let mut frame = frame::Audio::empty();
        while decoder.receive_frame(&mut frame).is_ok() {
            mono.push(&mut frame, &mut windows)?;
            if windows.stopped {
                break;
            }
        }
    }
    if !windows.stopped {
        mono.flush(&mut windows)?;
        windows.tail()?;
    }
    Ok(())
}
