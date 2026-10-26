use crate::{Error, Result};
use rustfft::{num_complex::Complex, Fft, FftPlanner};
use std::{f64::consts::PI, sync::Arc};

pub const AUDIO_SAMPLE_RATE: u32 = 16_000;
pub const AUDIO_MAX_SAMPLES: usize = 480_000;
const FRAME: usize = 320;
const HOP: usize = 160;
const FFT: usize = 512;
const BINS: usize = 257;
const MELS: usize = 128;

pub struct AudioInput {
    pub features: Vec<f32>,
    pub frame_mask: Vec<bool>,
    pub frames: usize,
    pub soft_tokens: usize,
}

impl AudioInput {
    pub fn text_input(&self) -> crate::TextInput {
        crate::text::media_ids(256000, 258881, 258883, self.soft_tokens)
    }
}

pub(crate) struct AudioProcessor {
    fft: Arc<dyn Fft<f64>>,
    window: [f32; FRAME],
    mel: Vec<f64>,
}

impl AudioProcessor {
    pub fn new() -> Self {
        let fft = FftPlanner::<f64>::new().plan_fft_forward(FFT);
        // Pinned processor casts the periodic Hann window to F32 before the
        // F32 frame multiply. NumPy then promotes its FFT to complex128.
        let window = std::array::from_fn(|i| {
            (0.5 - 0.5 * (2.0 * PI * i as f64 / FRAME as f64).cos()) as f32
        });
        let max_mel = 2595.0 * (1.0_f64 + 8000.0 / 700.0).log10();
        let points: Vec<f64> = (0..MELS + 2)
            .map(|i| {
                let mel = if i == MELS + 1 {
                    max_mel
                } else {
                    i as f64 * (max_mel / (MELS + 1) as f64)
                };
                700.0 * (10.0_f64.powf(mel / 2595.0) - 1.0)
            })
            .collect();
        let mut mel = vec![0.0; BINS * MELS];
        for frequency in 0..BINS {
            let hz = frequency as f64 * (8000.0 / (BINS - 1) as f64);
            for band in 0..MELS {
                let down = (hz - points[band]) / (points[band + 1] - points[band]);
                let up = (points[band + 2] - hz) / (points[band + 2] - points[band + 1]);
                mel[frequency * MELS + band] = down.min(up).max(0.0);
            }
        }
        Self { fft, window, mel }
    }

    pub fn preprocess(&self, samples: &[f32], sample_rate: u32) -> Result<AudioInput> {
        if sample_rate != AUDIO_SAMPLE_RATE {
            return Err(Error::InvalidInput(
                "audio must be mono16kHz float32; resample in the decoder",
            ));
        }
        if samples.is_empty() || samples.len() > AUDIO_MAX_SAMPLES {
            return Err(Error::InvalidInput("audio must contain1..480000samples"));
        }
        if samples.iter().any(|s| !s.is_finite()) {
            return Err(Error::InvalidInput("audio contains nonfinite samples"));
        }
        // unfold(size321,step160) after160 zeros; the321st sample is used for
        // mask validity, while only the first320 samples enter the FFT.
        if samples.len() + FRAME / 2 < FRAME + 1 {
            return Err(Error::InvalidInput(
                "audio is shorter than one analysis window",
            ));
        }
        let frames = (samples.len() + FRAME / 2 - (FRAME + 1)) / HOP + 1;
        let mut features = Vec::with_capacity(frames * MELS);
        let mut buffer = vec![Complex::new(0.0, 0.0); FFT];
        let mut scratch = vec![Complex::new(0.0, 0.0); self.fft.get_inplace_scratch_len()];
        for frame in 0..frames {
            buffer.fill(Complex::new(0.0, 0.0));
            for i in 0..FRAME {
                let padded_index = frame * HOP + i;
                let sample = if padded_index < FRAME / 2 {
                    0.0
                } else {
                    samples[padded_index - FRAME / 2]
                };
                buffer[i].re = (sample * self.window[i]) as f64;
            }
            self.fft.process_with_scratch(&mut buffer, &mut scratch);
            let mut spectrum = [0.0_f64; MELS];
            for (frequency, value) in buffer.iter().take(BINS).enumerate() {
                let magnitude = value.re.hypot(value.im);
                for (band, sum) in spectrum.iter_mut().enumerate() {
                    *sum += magnitude * self.mel[frequency * MELS + band];
                }
            }
            for value in spectrum {
                let value = (value + 0.001_f64).ln() as f32;
                if !value.is_finite() {
                    return Err(Error::InvalidInput("audio features became nonfinite"));
                }
                features.push(value);
            }
        }
        // Single-item batches have no right padding. The processor samples the
        // real last sample of every window; both stride2 SSCP masks use[::2].
        Ok(AudioInput {
            features,
            frame_mask: vec![true; frames],
            frames,
            soft_tokens: frames.div_ceil(4),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn input_and_window_boundaries() {
        let p = AudioProcessor::new();
        assert!(p.preprocess(&[], 16000).is_err());
        assert!(p.preprocess(&[0.0; 160], 16000).is_err());
        assert!(p.preprocess(&[0.0; 161], 8000).is_err());
        assert!(p.preprocess(&[f32::NAN; 161], 16000).is_err());
        let minimal = p.preprocess(&[0.0; 161], 16000).unwrap();
        assert_eq!((minimal.frames, minimal.soft_tokens), (1, 1));
        assert!(minimal.features.iter().all(|x| x.is_finite()));
        let second = p.preprocess(&vec![0.0; 16000], 16000).unwrap();
        assert_eq!((second.frames, second.soft_tokens), (99, 25));
    }
}
